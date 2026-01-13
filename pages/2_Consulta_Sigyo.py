import streamlit as st
import pandas as pd
import requests
import json
import time
import gc
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Configuração da Página ---
st.set_page_config(
    page_title="Consulta Sigyo",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🔍 Consulta Cadastral Sigyo (Paginada)")

# --- Barra Lateral ---
with st.sidebar:
    st.header("🔑 Credenciais")
    default_token = st.secrets.get("eliq_api_token", "")
    api_token = st.text_input("Token de Acesso (Bearer)", value=default_token, type="password")
    
    st.markdown("---")
    st.header("📂 Selecione a Base")
    tipo_relatorio = st.radio(
        "Qual cadastro deseja consultar?",
        ["Motoristas", "Credenciados", "Clientes"],
        index=0
    )
    
    st.markdown("---")
    st.header("⚙️ Configurações de Download")
    st.info("Ajuste estes valores se a consulta falhar ou demorar muito.")
    
    use_pagination = st.checkbox("Ativar Paginação (Recomendado)", value=True, help="Divide a consulta em várias partes pequenas para evitar erros de timeout.")
    batch_size = st.number_input("Tamanho do Lote (Registros por vez)", min_value=100, max_value=5000, value=1000, step=100)
    
    with st.expander("🔧 Avançado (Parâmetros da API)"):
        st.write("Ajuste apenas se souber que a API usa nomes diferentes.")
        param_limit = st.text_input("Nome do param. Limite", value="limit")
        param_offset = st.text_input("Nome do param. Pular/Offset", value="offset")

# ==============================================================================
# FUNÇÕES DE REDE (PAGINAÇÃO + RETRY)
# ==============================================================================

def get_retry_session():
    """Cria uma sessão HTTP com estratégia agressiva de reconexão."""
    session = requests.Session()
    retries = Retry(
        total=5, 
        backoff_factor=1, 
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def fetch_data_paginated(url, token, entity_name, base_params=None):
    """
    Busca dados da API dividindo em páginas para evitar timeout e estouro de memória.
    """
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    session = get_retry_session()
    all_items = []
    current_offset = 0
    page_num = 1
    has_more_data = True
    
    # Elementos de UI
    status_text = st.empty()
    progress_bar = st.progress(0)
    metric_col = st.empty()
    
    # Se paginação estiver desligada, tenta pegar tudo de uma vez (modo antigo)
    if not use_pagination:
        return fetch_single_shot(session, url, headers, base_params, entity_name, status_text)

    # Modo Paginado
    try:
        while has_more_data:
            # Atualiza parâmetros de paginação
            params = base_params.copy() if base_params else {}
            params[param_limit] = batch_size
            params[param_offset] = current_offset
            
            status_text.markdown(f"📥 **Página {page_num}:** Baixando registros {current_offset} a {current_offset + batch_size}...")
            
            try:
                # Timeout de 60s por página é suficiente se o lote for pequeno
                response = session.get(url, headers=headers, params=params, timeout=60)
                
                if response.status_code != 200:
                    st.error(f"Erro na Página {page_num} (Status {response.status_code}): {response.text[:200]}")
                    break
                
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    st.warning(f"Erro ao ler JSON na página {page_num}. Tentando novamente em 5s...")
                    time.sleep(5)
                    continue

                # Normalização da resposta (Lista ou Dicionário com 'items')
                items_in_page = []
                if isinstance(data, list):
                    items_in_page = data
                elif isinstance(data, dict) and 'items' in data:
                    items_in_page = data['items']
                else:
                    # Formato desconhecido, assume que é o dado ou lista vazia
                    items_in_page = data if isinstance(data, list) else []

                count = len(items_in_page)
                
                if count == 0:
                    status_text.text("✅ Download concluído: Nenhum registro restante.")
                    has_more_data = False
                    break
                
                # Detecção de API que ignora paginação (evita loop infinito)
                if page_num > 1 and count > 0 and items_in_page[0] == all_items[0]:
                    st.warning("⚠️ A API parece estar ignorando a paginação e retornando sempre o início. Interrompendo para evitar duplicidade.")
                    has_more_data = False
                    break

                all_items.extend(items_in_page)
                
                # Atualiza UI
                metric_col.metric("Registros Baixados", len(all_items))
                progress_bar.progress(min(page_num / 50.0, 1.0)) # Barra simbólica pois não sabemos o total exato
                
                # Prepara próxima página
                current_offset += count
                page_num += 1
                
                # Se vieram menos itens que o limite pedido, provavelmente acabou
                if count < batch_size:
                    has_more_data = False
                    status_text.text("✅ Fim da lista detectado.")
            
            except Exception as e:
                st.error(f"Falha de conexão na página {page_num}: {e}")
                time.sleep(2)
                # Tenta mais uma vez a mesma página ou aborta? Vamos abortar para não travar
                break
                
    finally:
        session.close()
        status_text.empty()
        progress_bar.empty()
        metric_col.empty()
        
    return all_items

def fetch_single_shot(session, url, headers, params, entity_name, status_text):
    """Fallback: Tenta baixar tudo de uma vez (código original robusto)."""
    status_text.text(f"Baixando {entity_name} (Modo Único - Pode demorar)...")
    try:
        response = session.get(url, headers=headers, params=params, stream=True, timeout=300)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Erro {response.status_code}")
            return None
    except Exception as e:
        st.error(f"Erro: {e}")
        return None

# ==============================================================================
# PROCESSADORES DE DADOS (OTIMIZADOS)
# ==============================================================================

@st.cache_data(show_spinner=False)
def process_motoristas(all_data):
    if not all_data: return pd.DataFrame()
    
    # Pré-aloca lista para performance
    processed_rows = []
    
    for d in all_data:
        if not isinstance(d, dict): continue
        
        # Extração otimizada
        grupos = ", ".join([str(g.get('nome','')) for g in d.get('grupos_vinculados', []) if isinstance(g, dict)])
        modulos = ", ".join([str(m.get('nome','')) for m in d.get('modulos', []) if isinstance(m, dict)])
        
        emp_list = []
        for emp in d.get('empresas', []):
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
    # Conversão de datas segura
    for col in ['Validade CNH', 'Data Cadastro']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            if col == 'Validade CNH':
                df[col] = df[col].dt.strftime('%d/%m/%Y')
            else:
                df[col] = df[col].dt.strftime('%d/%m/%Y %H:%M')
                
    return df

@st.cache_data(show_spinner=False)
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

@st.cache_data(show_spinner=False)
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
# LÓGICA DA INTERFACE
# ==============================================================================

if not api_token:
    st.warning("⚠️ Por favor, insira o Token da API na barra lateral para continuar.")
    st.stop()

# --- AÇÃO DE CONSULTA ---

if st.button(f"🔄 Iniciar Consulta de {tipo_relatorio}"):
    
    # Limpeza de memória forçada
    for k in ['df_motoristas', 'df_credenciados', 'df_clientes']:
        if k in st.session_state: del st.session_state[k]
    gc.collect()

    base_params = {}
    
    if tipo_relatorio == "Motoristas":
        url = "https://sigyo.uzzipay.com/api/motoristas"
        base_params = {
            'expand': 'grupos_vinculados,modulos,empresas,empresas.municipio,empresas.municipio.estado',
            'inline': 'false'
        }
        with st.spinner("Processando..."):
            raw_data = fetch_data_paginated(url, api_token, "Motoristas", base_params)
            if raw_data:
                st.session_state['df_motoristas'] = process_motoristas(raw_data)
                st.success(f"Sucesso! {len(raw_data)} motoristas carregados.")

    elif tipo_relatorio == "Credenciados":
        url = "https://sigyo.uzzipay.com/api/credenciados"
        base_params = {'expand': 'dadosAcesso,municipio,municipio.estado,modulos', 'inline': 'false'}
        with st.spinner("Processando..."):
            raw_data = fetch_data_paginated(url, api_token, "Credenciados", base_params)
            if raw_data:
                st.session_state['df_credenciados'] = process_credenciados(raw_data)
                st.success(f"Sucesso! {len(raw_data)} credenciados carregados.")

    elif tipo_relatorio == "Clientes":
        url = "https://sigyo.uzzipay.com/api/clientes"
        base_params = {'expand': 'municipio,municipio.estado,modulos,organizacao,tipo', 'inline': 'false'}
        with st.spinner("Processando..."):
            raw_data = fetch_data_paginated(url, api_token, "Clientes", base_params)
            if raw_data:
                st.session_state['df_clientes'] = process_clientes(raw_data)
                st.success(f"Sucesso! {len(raw_data)} clientes carregados.")

# --- EXIBIÇÃO DOS DADOS ---

current_key = ""
if tipo_relatorio == "Motoristas":
    current_key = 'df_motoristas'
    entity_title = "Motoristas"
    filename = "motoristas_sigyo.csv"
elif tipo_relatorio == "Credenciados":
    current_key = 'df_credenciados'
    entity_title = "Credenciados"
    filename = "credenciados_sigyo.csv"
elif tipo_relatorio == "Clientes":
    current_key = 'df_clientes'
    entity_title = "Clientes"
    filename = "clientes_sigyo.csv"

if current_key in st.session_state and isinstance(st.session_state[current_key], pd.DataFrame) and not st.session_state[current_key].empty:
    df = st.session_state[current_key]
    
    st.markdown(f"### 📋 Base de {entity_title}")
    
    col1, col2 = st.columns(2)
    with col1:
        status_cols = [c for c in ['Status', 'Situação', 'Ativo'] if c in df.columns]
        if status_cols:
            col_filter = status_cols[0]
            status_opts = sorted(df[col_filter].astype(str).unique())
            filtro_status = st.multiselect(f"Filtrar por {col_filter}:", options=status_opts, default=status_opts)
            if filtro_status:
                mask = df[col_filter].isin(filtro_status)
                df = df[mask]
    
    with col2:
        search = st.text_input("Busca Rápida (Nome, CNPJ/CPF, Email):")
        if search:
            search_cols = [c for c in ['Nome', 'Nome Fantasia', 'Razão Social', 'CPF/CNH', 'CNPJ', 'Email'] if c in df.columns]
            search_mask = pd.Series([False] * len(df))
            for c in search_cols:
                search_mask |= df[c].astype(str).str.contains(search, case=False, na=False)
            df = df[search_mask]

    st.markdown("#### Seleção de Colunas")
    all_cols = df.columns.tolist()
    
    # Configuração de visualização padrão
    if tipo_relatorio == "Motoristas": 
        default_view = ['ID', 'Nome', 'CPF/CNH', 'Status', 'Empresas']
    elif tipo_relatorio == "Credenciados": 
        default_view = ['ID', 'Nome Fantasia', 'CNPJ', 'Cidade', 'Responsável']
    else: 
        default_view = ['ID', 'Nome Fantasia', 'CNPJ', 'Cidade', 'Organização']
        
    default_view = [c for c in default_view if c in all_cols]
    if not default_view: default_view = all_cols[:6]
    
    selected_cols = st.multiselect("Colunas Visíveis:", all_cols, default=default_view)

    if selected_cols:
        df_display = df[selected_cols]
        st.info(f"Mostrando {len(df_display)} registros filtrados.")
        st.dataframe(df_display, use_container_width=True)
        
        csv = df_display.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label=f"📥 Baixar {entity_title} (CSV)",
            data=csv,
            file_name=filename,
            mime="text/csv",
            type="primary"
        )
    else:
        st.warning("Selecione ao menos uma coluna.")

elif current_key not in st.session_state:
    st.info(f"Clique no botão 'Iniciar Consulta' para carregar a base de {entity_title}.")
