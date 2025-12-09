import streamlit as st
import pandas as pd
import requests
import json
import io
import time
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
    st.header("Configurações")
    default_token = st.secrets.get("eliq_api_token", "")
    api_token = st.text_input("Token de Acesso (Bearer)", value=default_token, type="password")
    
    st.markdown("---")
    st.header("Selecione a Base")
    tipo_relatorio = st.radio(
        "Qual cadastro deseja consultar?",
        ["Motoristas", "Credenciados", "Clientes"],
        index=0
    )

# ==============================================================================
# FUNÇÕES DE REDE
# ==============================================================================

def get_retry_session():
    """Cria uma sessão HTTP que tenta reconectar automaticamente."""
    session = requests.Session()
    retries = Retry(
        total=5, # Aumentado para 5 tentativas
        backoff_factor=1, # Espera mais entre tentativas
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# --- FUNÇÃO PADRÃO (PARA CREDENCIADOS E CLIENTES) ---
def fetch_data_streaming(url, token, entity_name, params=None):
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
    }
    
    status_text = st.empty()
    progress_bar = st.progress(0)
    session = get_retry_session()

    try:
        status_text.text(f"Conectando à API de {entity_name}...")
        
        with session.get(url, headers=headers, params=params, stream=True, timeout=300) as response:
            if response.status_code != 200:
                st.error(f"Erro na API ({response.status_code}): {response.text[:500]}")
                return None
            
            total_size = int(response.headers.get('content-length', 0))
            data_buffer = io.BytesIO()
            downloaded_size = 0
            chunk_size = 100 * 1024 
            
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    data_buffer.write(chunk)
                    downloaded_size += len(chunk)
                    
                    if total_size > 0:
                        progress = min(downloaded_size / total_size, 1.0)
                        status_text.text(f"Baixando {entity_name}: {downloaded_size/(1024*1024):.2f} MB...")
                        progress_bar.progress(progress)

            progress_bar.progress(1.0)
            status_text.text("Processando JSON...")
            
            data_buffer.seek(0)
            json_content = json.load(data_buffer)
            
            if isinstance(json_content, list): return json_content
            elif isinstance(json_content, dict) and 'items' in json_content: return json_content['items']
            else: return []

    except Exception as e:
        st.error(f"Erro ao buscar {entity_name}: {e}")
        return None
    finally:
        status_text.empty()
        progress_bar.empty()

# --- NOVA FUNÇÃO ESPECÍFICA PARA MOTORISTAS (COM RETRY DE JSON) ---
def fetch_motoristas_heavy(token):
    url = "https://sigyo.uzzipay.com/api/motoristas"
    params = {
        'expand': 'grupos_vinculados,modulos,empresas,empresas.municipio,empresas.municipio.estado',
        'inline': 'false'
    }
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive' # Mantém conexão aberta
    }

    status_text = st.empty()
    progress_bar = st.progress(0)
    session = get_retry_session()
    
    # Tenta até 3 vezes se o JSON vier quebrado
    max_attempts = 3
    
    for attempt in range(1, max_attempts + 1):
        try:
            status_text.text(f"Tentativa {attempt}/{max_attempts}: Baixando base de Motoristas (Base Grande)...")
            
            # Timeout aumentado para 600s (10 minutos)
            with session.get(url, headers=headers, params=params, stream=True, timeout=600) as response:
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                data_buffer = io.BytesIO()
                downloaded_size = 0
                # Chunk de 1MB para baixar mais rápido
                chunk_size = 1024 * 1024 
                
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        data_buffer.write(chunk)
                        downloaded_size += len(chunk)
                        
                        mb = downloaded_size / (1024 * 1024)
                        if total_size > 0:
                            prog = min(downloaded_size / total_size, 1.0)
                            status_text.text(f"Tentativa {attempt}: Baixando Motoristas - {mb:.2f} MB de {total_size/(1024*1024):.2f} MB...")
                            progress_bar.progress(prog)
                        else:
                            status_text.text(f"Tentativa {attempt}: Baixando Motoristas - {mb:.2f} MB recebidos...")

                progress_bar.progress(1.0)
                status_text.text("Download finalizado. Validando integridade do JSON...")
                
                data_buffer.seek(0)
                json_content = json.load(data_buffer) # Aqui que costuma dar erro
                
                # Se chegou aqui, funcionou!
                if isinstance(json_content, list): return json_content
                elif isinstance(json_content, dict) and 'items' in json_content: return json_content['items']
                else: return []

        except json.JSONDecodeError:
            st.warning(f"Tentativa {attempt} falhou: O arquivo foi cortado pelo servidor. Tentando novamente em 5 segundos...")
            time.sleep(5) # Espera um pouco antes de tentar de novo
            progress_bar.progress(0)
            continue # Tenta próxima iteração
            
        except Exception as e:
            st.error(f"Erro fatal na tentativa {attempt}: {e}")
            return None
            
    st.error("Falha após 3 tentativas. A base de motoristas é muito grande e o servidor está cortando a conexão. Tente novamente mais tarde.")
    return None
    finally:
        status_text.empty()
        progress_bar.empty()

# ==============================================================================
# PROCESSADORES DE DADOS
# ==============================================================================

def process_motoristas(all_data):
    if not all_data: return pd.DataFrame()

    def extract_names(item_list):
        if not isinstance(item_list, list): return ""
        return ", ".join([str(i.get('nome', '')) for i in item_list if isinstance(i, dict) and i.get('nome')])

    def extract_empresas(empresa_list):
        if not isinstance(empresa_list, list): return ""
        items = []
        for emp in empresa_list:
            if isinstance(emp, dict):
                nome = emp.get('nome_fantasia') or emp.get('razao_social') or 'N/A'
                cnpj = emp.get('cnpj', '')
                items.append(f"{nome} ({cnpj})")
        return "; ".join(items)

    processed_rows = []
    for d in all_data:
        if not isinstance(d, dict): continue
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
            'Grupos Vinculados': extract_names(d.get('grupos_vinculados')),
            'Empresas': extract_empresas(d.get('empresas')),
            'Módulos': extract_names(d.get('modulos'))
        })

    df = pd.DataFrame(processed_rows)
    if 'Validade CNH' in df.columns:
        df['Validade CNH'] = pd.to_datetime(df['Validade CNH'], errors='coerce').dt.strftime('%d/%m/%Y')
    if 'Data Cadastro' in df.columns:
        df['Data Cadastro'] = pd.to_datetime(df['Data Cadastro'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
    return df

def process_credenciados(all_data):
    if not all_data: return pd.DataFrame()

    def get_address(d):
        muni = d.get('municipio') or {}
        estado = muni.get('estado') or {}
        parts = [d.get('logradouro'), str(d.get('numero') or ''), d.get('bairro'), muni.get('nome'), estado.get('sigla')]
        return ", ".join([str(p) for p in parts if p])

    def extract_modulos(item_list):
        if not isinstance(item_list, list): return ""
        return ", ".join([str(i.get('nome', '')) for i in item_list if isinstance(i, dict)])

    processed_rows = []
    for d in all_data:
        if not isinstance(d, dict): continue
        dados_acesso = d.get('dadosAcesso') or {}
        municipio = d.get('municipio') or {}
        estado = municipio.get('estado') or {}

        processed_rows.append({
            'ID': d.get('id'),
            'CNPJ': d.get('cnpj'),
            'Nome Fantasia': d.get('nome'),
            'Razão Social': d.get('razao_social'),
            'Situação': d.get('situacao'),
            'Ativo': 'Sim' if d.get('ativo') in [True, 1] else 'Não',
            'Telefone': d.get('telefone'),
            'Email': d.get('email'),
            'Cidade': municipio.get('nome'),
            'UF': estado.get('sigla'),
            'Endereço Completo': get_address(d),
            'Responsável': dados_acesso.get('nome_responsavel'),
            'CPF Responsável': dados_acesso.get('cpf_responsavel'),
            'Módulos': extract_modulos(d.get('modulos')),
            'Data Cadastro': d.get('data_cadastro')
        })

    df = pd.DataFrame(processed_rows)
    if 'Data Cadastro' in df.columns:
        df['Data Cadastro'] = pd.to_datetime(df['Data Cadastro'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
    return df

def process_clientes(all_data):
    if not all_data: return pd.DataFrame()

    def get_address(d):
        muni = d.get('municipio') or {}
        estado = muni.get('estado') or {}
        parts = [d.get('logradouro'), str(d.get('numero') or ''), d.get('bairro'), muni.get('nome'), estado.get('sigla')]
        return ", ".join([str(p) for p in parts if p])

    def extract_modulos(item_list):
        if not isinstance(item_list, list): return ""
        return ", ".join([str(i.get('nome', '')) for i in item_list if isinstance(i, dict)])

    processed_rows = []
    for d in all_data:
        if not isinstance(d, dict): continue
        municipio = d.get('municipio') or {}
        estado = municipio.get('estado') or {}
        org = d.get('organizacao') or {}
        tipo = d.get('tipo') or {}

        processed_rows.append({
            'ID': d.get('id'),
            'CNPJ': d.get('cnpj'),
            'Nome Fantasia': d.get('nome'),
            'Razão Social': d.get('razao_social'),
            'Email': d.get('email'),
            'Telefone': d.get('telefone'),
            'Ativo': 'Sim' if d.get('ativo') in [True, 1] else 'Não',
            'Suspenso': 'Sim' if d.get('suspenso') in [True, 1] else 'Não',
            'Cidade': municipio.get('nome'),
            'UF': estado.get('sigla'),
            'Endereço Completo': get_address(d),
            'Organização': org.get('nome'),
            'Tipo': tipo.get('nome'),
            'Módulos': extract_modulos(d.get('modulos')),
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

if st.button(f"🔄 Consultar {tipo_relatorio}"):
    
    if tipo_relatorio == "Motoristas":
        # Usa a função "heavy" específica
        raw_data = fetch_motoristas_heavy(api_token)
        if raw_data:
            st.session_state['df_motoristas'] = process_motoristas(raw_data)
            st.success("Dados de Motoristas atualizados!")

    elif tipo_relatorio == "Credenciados":
        url = "https://sigyo.uzzipay.com/api/credenciados"
        params = {'expand': 'dadosAcesso,municipio,municipio.estado,modulos', 'inline': 'false'}
        raw_data = fetch_data_streaming(url, api_token, "Credenciados", params)
        if raw_data:
            st.session_state['df_credenciados'] = process_credenciados(raw_data)
            st.success("Dados de Credenciados atualizados!")

    elif tipo_relatorio == "Clientes":
        url = "https://sigyo.uzzipay.com/api/clientes"
        params = {'expand': 'municipio,municipio.estado,modulos,organizacao,tipo', 'inline': 'false'}
        raw_data = fetch_data_streaming(url, api_token, "Clientes", params)
        if raw_data:
            st.session_state['df_clientes'] = process_clientes(raw_data)
            st.success("Dados de Clientes atualizados!")

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

if current_key in st.session_state and not st.session_state[current_key].empty:
    df = st.session_state[current_key]
    
    st.markdown(f"### 📋 Base de {entity_title}")
    
    col1, col2 = st.columns(2)
    with col1:
        # Tenta encontrar status
        for col in ['Status', 'Situação', 'Ativo']:
            if col in df.columns:
                status_opts = sorted(df[col].astype(str).unique())
                filtro_status = st.multiselect(f"Filtrar por {col}:", options=status_opts, default=status_opts)
                mask = df[col].isin(filtro_status)
                break
        else:
            mask = pd.Series([True] * len(df))
    
    with col2:
        search = st.text_input("Busca Rápida (Nome, CNPJ/CPF, Email):")

    if search:
        search_cols = [c for c in ['Nome', 'Nome Fantasia', 'Razão Social', 'CPF/CNH', 'CNPJ', 'Email'] if c in df.columns]
        search_mask = pd.Series([False] * len(df))
        for c in search_cols:
            search_mask |= df[c].astype(str).str.contains(search, case=False, na=False)
        mask &= search_mask

    df_filtered = df[mask]

    st.markdown("#### Seleção de Colunas")
    all_cols = df_filtered.columns.tolist()
    
    # Defaults
    if tipo_relatorio == "Motoristas": default_view = ['ID', 'Nome', 'CPF/CNH', 'Status', 'Empresas']
    elif tipo_relatorio == "Credenciados": default_view = ['ID', 'Nome Fantasia', 'CNPJ', 'Cidade', 'Responsável']
    else: default_view = ['ID', 'Nome Fantasia', 'CNPJ', 'Cidade', 'Organização']
        
    default_view = [c for c in default_view if c in all_cols]
    if not default_view: default_view = all_cols[:6]
    
    selected_cols = st.multiselect("Colunas Visíveis:", all_cols, default=default_view)

    if selected_cols:
        df_display = df_filtered[selected_cols]
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
    st.info(f"Clique no botão acima para carregar a base de {entity_title}.")
