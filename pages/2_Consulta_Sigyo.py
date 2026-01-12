import streamlit as st
import pandas as pd
import requests
import json
import time
import gc
import tempfile
import os
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
# FUNÇÕES DE REDE (BLINDADAS - VERSÃO DISK CACHE)
# ==============================================================================

def get_retry_session():
    """Cria uma sessão HTTP com estratégia agressiva de reconexão."""
    session = requests.Session()
    retries = Retry(
        total=5, 
        backoff_factor=2, # Espera 2s, 4s, 8s...
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def fetch_data_robust(url, token, entity_name, params=None):
    """
    Realiza o download salvando em arquivo temporário (DISCO) para economizar RAM.
    """
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    status_text = st.empty()
    progress_bar = st.progress(0)
    
    session = get_retry_session()
    max_attempts = 3
    result_data = None
    temp_file_path = None
    
    try:
        for attempt in range(1, max_attempts + 1):
            try:
                status_text.markdown(f"⏳ **Tentativa {attempt}/{max_attempts}:** Baixando {entity_name} para disco temporário...")
                
                # Timeout alto para conexões lentas
                with session.get(url, headers=headers, params=params, stream=True, timeout=900) as response:
                    if response.status_code != 200:
                        st.error(f"Erro na API ({response.status_code}): {response.text[:500]}")
                        time.sleep(5)
                        continue
                    
                    total_size = int(response.headers.get('content-length', 0))
                    downloaded_size = 0
                    chunk_size = 1024 * 1024 # 1MB chunks
                    
                    # Cria arquivo temporário no disco (não ocupa RAM)
                    with tempfile.NamedTemporaryFile(delete=False, mode='wb') as tmp_file:
                        temp_file_path = tmp_file.name
                        
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            if chunk:
                                tmp_file.write(chunk)
                                downloaded_size += len(chunk)
                                
                                # Atualiza progresso visual
                                if total_size > 0:
                                    prog = min(downloaded_size / total_size, 1.0)
                                    progress_bar.progress(prog)
                                    mb_down = downloaded_size / (1024 * 1024)
                                    mb_total = total_size / (1024 * 1024)
                                    status_text.text(f"Baixando: {mb_down:.2f} MB / {mb_total:.2f} MB")
                                else:
                                    mb_down = downloaded_size / (1024 * 1024)
                                    status_text.text(f"Baixando: {mb_down:.2f} MB recebidos...")

                    # Validação básica de tamanho (se content-length existir)
                    if total_size > 0 and downloaded_size < total_size:
                        raise Exception(f"Download incompleto. Esperado: {total_size}, Recebido: {downloaded_size}")

                    progress_bar.progress(1.0)
                    status_text.text("Lendo arquivo do disco e processando JSON...")
                    
                    # Carrega do disco para evitar pico de memória
                    with open(temp_file_path, 'r', encoding='utf-8') as f:
                        json_content = json.load(f)
                    
                    # Validação e Extração
                    if isinstance(json_content, list): 
                        result_data = json_content
                        break
                    elif isinstance(json_content, dict) and 'items' in json_content: 
                        result_data = json_content['items']
                        break
                    else:
                        st.warning(f"Formato inesperado: {type(json_content)}")
                        result_data = []
                        break

            except json.JSONDecodeError as e:
                st.warning(f"Tentativa {attempt}: Arquivo corrompido (JSON inválido). Retentando... Erro: {str(e)[:100]}")
                time.sleep(3)
                continue
            except Exception as e:
                st.warning(f"Tentativa {attempt} falhou: {str(e)}. Retentando em 5s...")
                time.sleep(5)
                continue
            finally:
                # Limpa arquivo temporário se existir
                if temp_file_path and os.path.exists(temp_file_path):
                    try:
                        os.remove(temp_file_path)
                    except:
                        pass
        
        if result_data is None:
            st.error(f"❌ Não foi possível obter os dados de {entity_name} após {max_attempts} tentativas.")
            return None
            
        return result_data

    finally:
        status_text.empty()
        progress_bar.empty()
        session.close()
        gc.collect()

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
    # Otimização: List Comprehension para loop mais rápido
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
    
    # Limpeza agressiva
    del all_data
    gc.collect()

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
        parts = [
            d.get('logradouro'),
            str(d.get('numero')) if d.get('numero') else '',
            d.get('bairro'),
            muni.get('nome'),
            estado.get('sigla'),
            d.get('cep')
        ]
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
            'Email': d.get('email'),
            'Telefone': d.get('telefone'),
            'Situação': d.get('situacao'),
            'Ativo': 'Sim' if d.get('ativo') in [True, 1] else 'Não',
            'Cidade': municipio.get('nome'),
            'UF': estado.get('sigla'),
            'Endereço Completo': get_address(d),
            'Responsável': dados_acesso.get('nome_responsavel'),
            'CPF Responsável': dados_acesso.get('cpf_responsavel'),
            'Email Responsável': dados_acesso.get('email_responsavel'),
            'Telefone Responsável': dados_acesso.get('telefone_responsavel'),
            'Taxa Adm (%)': d.get('limite_isencao_ir_tx_adm'),
            'Módulos': extract_modulos(d.get('modulos')),
            'Data Cadastro': d.get('data_cadastro')
        })
    
    del all_data
    gc.collect()

    df = pd.DataFrame(processed_rows)
    if 'Data Cadastro' in df.columns:
        df['Data Cadastro'] = pd.to_datetime(df['Data Cadastro'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
    return df

def process_clientes(all_data):
    if not all_data: return pd.DataFrame()

    def get_address(d):
        muni = d.get('municipio') or {}
        estado = muni.get('estado') or {}
        parts = [d.get('logradouro'), str(d.get('numero') or ''), d.get('bairro'), muni.get('nome'), estado.get('sigla'), d.get('cep')]
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
            'Tipo Cliente': tipo.get('nome'),
            'Módulos': extract_modulos(d.get('modulos')),
            'Recolhimento DARF': 'Sim' if d.get('recolhimento_darf') in [True, 1] else 'Não',
            'Data Cadastro': d.get('data_cadastro')
        })
    
    del all_data
    gc.collect()

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
    
    # Limpa dados anteriores da memória para evitar OOM
    keys_to_clear = ['df_motoristas', 'df_credenciados', 'df_clientes']
    for k in keys_to_clear:
        if k in st.session_state:
            del st.session_state[k]
    gc.collect()

    if tipo_relatorio == "Motoristas":
        url = "https://sigyo.uzzipay.com/api/motoristas"
        params = {
            'expand': 'grupos_vinculados,modulos,empresas,empresas.municipio,empresas.municipio.estado',
            'inline': 'false'
        }
        with st.spinner("Baixando base de Motoristas..."):
            raw_data = fetch_data_robust(url, api_token, "Motoristas", params)
            if raw_data:
                st.session_state['df_motoristas'] = process_motoristas(raw_data)
                st.success("Dados de Motoristas atualizados!")

    elif tipo_relatorio == "Credenciados":
        url = "https://sigyo.uzzipay.com/api/credenciados"
        params = {'expand': 'dadosAcesso,municipio,municipio.estado,modulos', 'inline': 'false'}
        with st.spinner("Baixando base de Credenciados..."):
            raw_data = fetch_data_robust(url, api_token, "Credenciados", params)
            if raw_data:
                st.session_state['df_credenciados'] = process_credenciados(raw_data)
                st.success("Dados de Credenciados atualizados!")

    elif tipo_relatorio == "Clientes":
        url = "https://sigyo.uzzipay.com/api/clientes"
        params = {'expand': 'municipio,municipio.estado,modulos,organizacao,tipo', 'inline': 'false'}
        with st.spinner("Baixando base de Clientes..."):
            raw_data = fetch_data_robust(url, api_token, "Clientes", params)
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

if current_key in st.session_state and isinstance(st.session_state[current_key], pd.DataFrame) and not st.session_state[current_key].empty:
    df = st.session_state[current_key]
    
    st.markdown(f"### 📋 Base de {entity_title}")
    
    col1, col2 = st.columns(2)
    with col1:
        # Filtros automáticos de Status/Ativo
        status_cols = [c for c in ['Status', 'Situação', 'Ativo'] if c in df.columns]
        if status_cols:
            col_filter = status_cols[0]
            status_opts = sorted(df[col_filter].astype(str).unique())
            filtro_status = st.multiselect(f"Filtrar por {col_filter}:", options=status_opts, default=status_opts)
            mask = df[col_filter].isin(filtro_status)
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
    
    # Colunas padrão por tipo
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
