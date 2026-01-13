import streamlit as st
import pandas as pd
import requests
import json
import gc
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
# PROCESSADORES DE DADOS (CACHEADOS E OTIMIZADOS)
# ==============================================================================

@st.cache_data(show_spinner=False, ttl=3600)
def process_motoristas(all_data):
    if not all_data: return pd.DataFrame()
    
    processed_rows = []
    for d in all_data:
        if not isinstance(d, dict): continue
        
        # Tratamento seguro de campos nulos e listas
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

        processed_rows.append({
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
        })
    
    df = pd.DataFrame(processed_rows)
    # Conversão de datas
    for col in ['Validade CNH', 'Data Cadastro']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            if col == 'Validade CNH':
                df[col] = df[col].dt.strftime('%d/%m/%Y')
            else:
                df[col] = df[col].dt.strftime('%d/%m/%Y %H:%M')
    return df

@st.cache_data(show_spinner=False, ttl=3600)
def process_credenciados(all_data):
    if not all_data: return pd.DataFrame()
    
    processed_rows = []
    for d in all_data:
        if not isinstance(d, dict): continue
        
        muni = d.get('municipio') or {}
        estado = muni.get('estado') or {}
        dados_acesso = d.get('dadosAcesso') or {}
        
        parts = [
            d.get('logradouro'),
            str(d.get('numero')) if d.get('numero') else '',
            d.get('bairro'),
            muni.get('nome'),
            estado.get('sigla'),
            d.get('cep')
        ]
        endereco = ", ".join([str(p) for p in parts if p])
        modulos = ", ".join([str(m.get('nome','')) for m in d.get('modulos', []) if isinstance(m, dict)])

        processed_rows.append({
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
        })

    df = pd.DataFrame(processed_rows)
    if 'Data Cadastro' in df.columns:
        df['Data Cadastro'] = pd.to_datetime(df['Data Cadastro'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
    return df

@st.cache_data(show_spinner=False, ttl=3600)
def process_clientes(all_data):
    if not all_data: return pd.DataFrame()
    
    processed_rows = []
    for d in all_data:
        if not isinstance(d, dict): continue
        
        muni = d.get('municipio') or {}
        estado = muni.get('estado') or {}
        org = d.get('organizacao') or {}
        tipo = d.get('tipo') or {}
        
        parts = [d.get('logradouro'), str(d.get('numero') or ''), d.get('bairro'), muni.get('nome'), estado.get('sigla'), d.get('cep')]
        endereco = ", ".join([str(p) for p in parts if p])
        modulos = ", ".join([str(m.get('nome','')) for m in d.get('modulos', []) if isinstance(m, dict)])

        processed_rows.append({
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
        })

    df = pd.DataFrame(processed_rows)
    if 'Data Cadastro' in df.columns:
        df['Data Cadastro'] = pd.to_datetime(df['Data Cadastro'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
    return df

# ==============================================================================
# FUNÇÕES DE API (BACKUP)
# ==============================================================================
def fetch_from_api(url, token, entity_name, params):
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=120)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Erro na API: {e}")
        return None

# ==============================================================================
# LÓGICA PRINCIPAL (ABAS)
# ==============================================================================

tab_upload, tab_api = st.tabs(["📂 Carregar via Upload (Rápido)", "☁️ Consultar API (Lento)"])

df_result = None

# --- ABA 1: UPLOAD ---
with tab_upload:
    st.markdown("### 1. Instruções")
    st.info("""
    1. Utilize o **Postman** ou navegador para baixar o JSON completo da API.
    2. Salve o arquivo no seu computador (ex: `motoristas.json`).
    3. Arraste o arquivo para a área abaixo.
    """)
    
    uploaded_file = st.file_uploader(f"Faça upload do JSON de **{tipo_relatorio}**", type=['json'])
    
    if uploaded_file is not None:
        try:
            with st.spinner("Lendo arquivo e processando dados..."):
                # Carrega o JSON
                raw_data = json.load(uploaded_file)
                
                # Normaliza (se vier dentro de 'items' ou direto em lista)
                data_to_process = []
                if isinstance(raw_data, list):
                    data_to_process = raw_data
                elif isinstance(raw_data, dict) and 'items' in raw_data:
                    data_to_process = raw_data['items']
                else:
                    data_to_process = raw_data if isinstance(raw_data, list) else []

                # Processa conforme a seleção
                if tipo_relatorio == "Motoristas":
                    df_result = process_motoristas(data_to_process)
                elif tipo_relatorio == "Credenciados":
                    df_result = process_credenciados(data_to_process)
                elif tipo_relatorio == "Clientes":
                    df_result = process_clientes(data_to_process)
                
                # Limpa memória bruta
                del raw_data
                del data_to_process
                gc.collect()

                if df_result.empty:
                    st.warning("O arquivo JSON foi lido, mas não continha dados reconhecíveis.")
                else:
                    st.success(f"Arquivo carregado com sucesso! {len(df_result)} registros encontrados.")

        except json.JSONDecodeError:
            st.error("Erro ao ler o arquivo: O JSON parece inválido ou corrompido.")
        except Exception as e:
            st.error(f"Erro inesperado ao processar arquivo: {e}")

# --- ABA 2: API ---
with tab_api:
    st.warning("⚠️ O modo API pode falhar se a conexão for instável ou a base for muito grande.")
    if st.button(f"Tentar buscar {tipo_relatorio} via API"):
        if not api_token:
            st.error("Insira o token na barra lateral.")
        else:
            with st.spinner("Conectando à API..."):
                url = ""
                params = {'inline': 'false'}
                
                if tipo_relatorio == "Motoristas":
                    url = "https://sigyo.uzzipay.com/api/motoristas"
                    params['expand'] = 'grupos_vinculados,modulos,empresas,empresas.municipio'
                elif tipo_relatorio == "Credenciados":
                    url = "https://sigyo.uzzipay.com/api/credenciados"
                    params['expand'] = 'dadosAcesso,municipio,municipio.estado,modulos'
                elif tipo_relatorio == "Clientes":
                    url = "https://sigyo.uzzipay.com/api/clientes"
                    params['expand'] = 'municipio,municipio.estado,modulos,organizacao,tipo'

                api_data_raw = fetch_from_api(url, api_token, tipo_relatorio, params)
                
                if api_data_raw:
                    items = api_data_raw if isinstance(api_data_raw, list) else api_data_raw.get('items', [])
                    if tipo_relatorio == "Motoristas":
                        df_result = process_motoristas(items)
                    elif tipo_relatorio == "Credenciados":
                        df_result = process_credenciados(items)
                    elif tipo_relatorio == "Clientes":
                        df_result = process_clientes(items)

# ==============================================================================
# EXIBIÇÃO FINAL
# ==============================================================================

if df_result is not None and not df_result.empty:
    st.divider()
    st.header(f"📊 Resultados: {tipo_relatorio}")
    
    col1, col2 = st.columns(2)
    with col1:
        status_cols = [c for c in ['Status', 'Situação', 'Ativo'] if c in df_result.columns]
        if status_cols:
            col_filter = status_cols[0]
            status_opts = sorted(df_result[col_filter].astype(str).unique())
            filtro_status = st.multiselect(f"Filtrar por {col_filter}:", options=status_opts, default=status_opts)
            if filtro_status:
                df_result = df_result[df_result[col_filter].isin(filtro_status)]
    
    with col2:
        search = st.text_input("Busca Rápida (Nome, CNPJ, Email):")
        if search:
            search_cols = [c for c in ['Nome', 'Nome Fantasia', 'Razão Social', 'CPF/CNH', 'CNPJ', 'Email'] if c in df_result.columns]
            mask = pd.Series([False] * len(df_result))
            for c in search_cols:
                mask |= df_result[c].astype(str).str.contains(search, case=False, na=False)
            df_result = df_result[mask]

    st.write(f"Mostrando {len(df_result)} registros.")
    
    # Seleção de colunas
    all_cols = df_result.columns.tolist()
    default_cols = all_cols[:6] # Padrão: 6 primeiras
    cols_to_show = st.multiselect("Colunas Visíveis", all_cols, default=default_cols)
    
    if cols_to_show:
        st.dataframe(df_result[cols_to_show], use_container_width=True)
        
        # Botão Download
        csv = df_result[cols_to_show].to_csv(index=False).encode('utf-8-sig')
        filename = f"{tipo_relatorio.lower()}_sigyo.csv"
        st.download_button("📥 Baixar CSV Filtrado", data=csv, file_name=filename, mime="text/csv", type="primary")
