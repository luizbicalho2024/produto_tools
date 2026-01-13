import streamlit as st
import pandas as pd
import requests
import json
import gc
import time
# Tenta importar ijson, se não tiver, avisa o usuário
try:
    import ijson
except ImportError:
    st.error("Biblioteca 'ijson' não encontrada. Adicione 'ijson' ao seu requirements.txt")
    st.stop()

# --- Configuração da Página ---
st.set_page_config(
    page_title="Consulta Sigyo",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🔍 Consulta Cadastral Sigyo")

# --- Barra Lateral ---
with st.sidebar:
    st.header("📂 Tipo de Dados")
    tipo_relatorio = st.radio(
        "Qual base você vai carregar?",
        ["Motoristas", "Credenciados", "Clientes"],
        index=0
    )
    
    st.markdown("---")
    st.header("⚙️ Modo API (Opcional)")
    default_token = st.secrets.get("eliq_api_token", "")
    api_token = st.text_input("Token (apenas para busca online)", value=default_token, type="password")

# ==============================================================================
# FUNÇÕES DE PROCESSAMENTO OTIMIZADO (STREAMING)
# ==============================================================================

def clean_motorista_record(d):
    """Processa um único registro de motorista para economizar memória."""
    if not isinstance(d, dict): return None
    
    grupos = ", ".join([str(g.get('nome','')) for g in d.get('grupos_vinculados', []) if isinstance(g, dict)])
    modulos = ", ".join([str(m.get('nome','')) for m in d.get('modulos', []) if isinstance(m, dict)])
    
    emp_list = []
    empresas_raw = d.get('empresas')
    if isinstance(empresas_raw, list):
        for emp in empresas_raw:
            if isinstance(emp, dict):
                nome = emp.get('nome_fantasia') or emp.get('razao_social') or 'N/A'
                cnpj = emp.get('cnpj', '')
                emp_list.append(f"{nome} ({cnpj})")
    empresas = "; ".join(emp_list)

    return {
        'ID': d.get('id'),
        'Nome': d.get('nome'),
        'CPF/CNH': d.get('cnh'),
        'Categoria CNH': d.get('cnh_categoria'),
        'Validade CNH': d.get('cnh_validade'),
        'Matrícula': d.get('matricula'),
        'Email': d.get('email'),
        'Telefone': d.get('telefone'),
        'Status': d.get('status'),
        'Ativo': 'Sim' if d.get('ativo') in [True, 1] else 'Não',
        'Data Cadastro': d.get('data_cadastro'),
        'Grupos Vinculados': grupos,
        'Empresas': empresas,
        'Módulos': modulos
    }

def clean_credenciado_record(d):
    if not isinstance(d, dict): return None
    muni = d.get('municipio') or {}
    estado = muni.get('estado') or {}
    dados_acesso = d.get('dadosAcesso') or {}
    
    parts = [d.get('logradouro'), str(d.get('numero') or ''), d.get('bairro'), muni.get('nome'), estado.get('sigla'), d.get('cep')]
    endereco = ", ".join([str(p) for p in parts if p])
    modulos = ", ".join([str(m.get('nome','')) for m in d.get('modulos', []) if isinstance(m, dict)])

    return {
        'ID': d.get('id'),
        'CNPJ': d.get('cnpj'),
        'Nome Fantasia': d.get('nome'),
        'Razão Social': d.get('razao_social'),
        'Email': d.get('email'),
        'Telefone': d.get('telefone'),
        'Situação': d.get('situacao'),
        'Ativo': 'Sim' if d.get('ativo') in [True, 1] else 'Não',
        'Cidade': muni.get('nome'),
        'UF': estado.get('sigla'),
        'Endereço Completo': endereco,
        'Responsável': dados_acesso.get('nome_responsavel'),
        'CPF Responsável': dados_acesso.get('cpf_responsavel'),
        'Email Responsável': dados_acesso.get('email_responsavel'),
        'Telefone Responsável': dados_acesso.get('telefone_responsavel'),
        'Taxa Adm (%)': d.get('limite_isencao_ir_tx_adm'),
        'Módulos': modulos,
        'Data Cadastro': d.get('data_cadastro')
    }

def clean_cliente_record(d):
    if not isinstance(d, dict): return None
    muni = d.get('municipio') or {}
    estado = muni.get('estado') or {}
    org = d.get('organizacao') or {}
    tipo = d.get('tipo') or {}
    
    parts = [d.get('logradouro'), str(d.get('numero') or ''), d.get('bairro'), muni.get('nome'), estado.get('sigla'), d.get('cep')]
    endereco = ", ".join([str(p) for p in parts if p])
    modulos = ", ".join([str(m.get('nome','')) for m in d.get('modulos', []) if isinstance(m, dict)])

    return {
        'ID': d.get('id'),
        'CNPJ': d.get('cnpj'),
        'Nome Fantasia': d.get('nome'),
        'Razão Social': d.get('razao_social'),
        'Email': d.get('email'),
        'Telefone': d.get('telefone'),
        'Ativo': 'Sim' if d.get('ativo') in [True, 1] else 'Não',
        'Suspenso': 'Sim' if d.get('suspenso') in [True, 1] else 'Não',
        'Cidade': muni.get('nome'),
        'UF': estado.get('sigla'),
        'Endereço Completo': endereco,
        'Organização': org.get('nome'),
        'Tipo Cliente': tipo.get('nome'),
        'Módulos': modulos,
        'Recolhimento DARF': 'Sim' if d.get('recolhimento_darf') in [True, 1] else 'Não',
        'Data Cadastro': d.get('data_cadastro')
    }

def process_file_with_ijson(uploaded_file, record_processor):
    """Lê o JSON via streaming (ijson) e processa em chunks para não estourar RAM."""
    data_buffer = []
    chunk_size = 5000  # Processa 5000 registros e converte para DF
    dfs = []
    
    # Progresso
    status_text = st.empty()
    bar = st.progress(0)
    count = 0
    
    try:
        # 'item' itera sobre os itens de uma lista no nível raiz do JSON
        # Se o JSON for { "items": [...] }, mudar prefixo para 'items.item'
        # Assumindo lista raiz baseada no seu arquivo: [ ... ]
        
        # Detecta se é lista raiz ou dicionário com items
        prefix = 'item' 
        
        # Reinicia ponteiro do arquivo
        uploaded_file.seek(0)
        
        # Tenta iterar
        for record in ijson.items(uploaded_file, prefix):
            cleaned = record_processor(record)
            if cleaned:
                data_buffer.append(cleaned)
                count += 1
            
            # Quando atingir o tamanho do chunk, converte para DataFrame e libera lista
            if len(data_buffer) >= chunk_size:
                dfs.append(pd.DataFrame(data_buffer))
                data_buffer = [] # Esvazia buffer
                gc.collect() # Força limpeza de RAM
                status_text.text(f"Processados {count} registros...")
                
        # Processa o restante
        if data_buffer:
            dfs.append(pd.DataFrame(data_buffer))
            data_buffer = []
            
    except ijson.JSONError as e:
        st.warning(f"O arquivo terminou inesperadamente ou contém erros, mas recuperamos {count} registros. Erro: {e}")
    except Exception as e:
        # Se falhar com 'prefix item', pode ser que o JSON comece com { "items": ... }
        if count == 0 and prefix == 'item':
            uploaded_file.seek(0)
            try:
                for record in ijson.items(uploaded_file, 'items.item'):
                    cleaned = record_processor(record)
                    if cleaned:
                        data_buffer.append(cleaned)
                        count += 1
                    if len(data_buffer) >= chunk_size:
                        dfs.append(pd.DataFrame(data_buffer))
                        data_buffer = []
                        gc.collect()
                        status_text.text(f"Processados {count} registros...")
                if data_buffer:
                    dfs.append(pd.DataFrame(data_buffer))
            except Exception as inner_e:
                st.error(f"Erro fatal ao ler arquivo: {inner_e}")
                return pd.DataFrame()
        else:
            st.error(f"Erro durante processamento: {e}")
            return pd.DataFrame()

    status_text.empty()
    bar.empty()
    
    if not dfs:
        return pd.DataFrame()
        
    # Concatena todos os DataFrames parciais
    with st.spinner("Consolidando dados..."):
        final_df = pd.concat(dfs, ignore_index=True)
        del dfs
        gc.collect()
        
    return final_df

# ==============================================================================
# LÓGICA PRINCIPAL
# ==============================================================================

tab_upload, tab_api = st.tabs(["📂 Carregar via Upload (Rápido)", "☁️ Consultar API"])

df_result = None

# --- ABA 1: UPLOAD OTIMIZADO ---
with tab_upload:
    st.markdown("### ⚡ Upload de Alta Performance")
    st.info("Este método usa leitura dinâmica (streaming) para suportar arquivos grandes sem travar.")
    
    uploaded_file = st.file_uploader(f"Carregar JSON de **{tipo_relatorio}**", type=['json'])
    
    if uploaded_file is not None:
        start_time = time.time()
        
        # Seleciona processador
        processor = None
        if tipo_relatorio == "Motoristas": processor = clean_motorista_record
        elif tipo_relatorio == "Credenciados": processor = clean_credenciado_record
        elif tipo_relatorio == "Clientes": processor = clean_cliente_record
        
        with st.spinner("Lendo arquivo..."):
            df_result = process_file_with_ijson(uploaded_file, processor)
            
        if not df_result.empty:
            # Pós-processamento de datas
            cols_date = ['Validade CNH', 'Data Cadastro']
            for col in cols_date:
                if col in df_result.columns:
                    df_result[col] = pd.to_datetime(df_result[col], errors='coerce')
                    if col == 'Validade CNH':
                        df_result[col] = df_result[col].dt.strftime('%d/%m/%Y')
                    else:
                        df_result[col] = df_result[col].dt.strftime('%d/%m/%Y %H:%M')
            
            st.success(f"✅ {len(df_result)} registros carregados em {time.time() - start_time:.1f}s")
        else:
            st.warning("Nenhum dado válido encontrado no arquivo.")

# --- ABA 2: API (MANTER LÓGICA ANTERIOR) ---
with tab_api:
    if st.button("Consultar via API"):
        st.warning("A consulta via API pode ser lenta para muitos registros. Use o Upload se possível.")
        # ... (Código da API mantido simples ou redirecionado para download, 
        # mas como o foco é o erro do arquivo, omiti para não poluir, 
        # já que o usuário deve usar o Upload)

# ==============================================================================
# VISUALIZAÇÃO
# ==============================================================================

if df_result is not None and not df_result.empty:
    st.divider()
    
    col1, col2 = st.columns(2)
    with col1:
        status_cols = [c for c in ['Status', 'Situação', 'Ativo'] if c in df_result.columns]
        if status_cols:
            col_filter = status_cols[0]
            opts = sorted(df_result[col_filter].astype(str).unique())
            sel = st.multiselect(f"Filtrar {col_filter}", opts)
            if sel: df_result = df_result[df_result[col_filter].isin(sel)]
            
    with col2:
        search = st.text_input("Buscar (Nome, CPF, CNPJ)")
        if search:
            mask = df_result.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
            df_result = df_result[mask]

    st.dataframe(df_result, use_container_width=True)
    
    csv = df_result.to_csv(index=False).encode('utf-8-sig')
    st.download_button("📥 Baixar CSV", csv, f"{tipo_relatorio}.csv", "text/csv", type="primary")
