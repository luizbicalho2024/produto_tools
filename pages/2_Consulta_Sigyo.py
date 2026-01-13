import streamlit as st
import pandas as pd
import requests
import json
import tempfile
import os
import gc
import shutil
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
# FUNÇÕES DE REDE (BLINDADAS CONTRA CORRUPÇÃO DE ARQUIVO)
# ==============================================================================

def get_session():
    """Cria uma sessão HTTP robusta com retry automático."""
    session = requests.Session()
    adapter = HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=2,  # Espera 2s, 4s, 8s entre tentativas
            status_forcelist=[500, 502, 503, 504, 104],
            allowed_methods=["GET"]
        )
    )
    session.mount("https://", adapter)
    return session

@st.cache_data(ttl=900, show_spinner=False)
def fetch_data_safe(url, token, params=None):
    """
    Baixa dados com verificação estrita de integridade.
    Se o arquivo baixado estiver incompleto (bytes faltando), ele tenta novamente.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate"
    }

    # Loop manual de tentativas para garantir integridade do download
    max_attempts = 3
    
    for attempt in range(1, max_attempts + 1):
        session = get_session()
        fd, tmp_path = tempfile.mkstemp(suffix=".json")
        os.close(fd)

        try:
            # Timeout: 20s para conectar, 300s (5 min) para baixar o conteúdo
            with session.get(url, headers=headers, params=params, stream=True, timeout=(20, 300)) as response:
                response.raise_for_status()
                
                # Tenta obter o tamanho total esperado
                total_size_expected = int(response.headers.get('content-length', 0))
                
                with open(tmp_path, 'wb') as f:
                    shutil.copyfileobj(response.raw, f)
            
            # --- VERIFICAÇÃO DE INTEGRIDADE ---
            file_size = os.path.getsize(tmp_path)
            
            # Se o servidor informou o tamanho e o arquivo baixado for menor, falhou.
            if total_size_expected > 0 and file_size < total_size_expected:
                raise Exception(f"Download incompleto. Esperado: {total_size_expected} bytes, Baixado: {file_size} bytes.")
            
            # Se o arquivo for muito pequeno (menos de 100 bytes), provavelmente é erro
            if file_size < 100:
                # Tenta ler pra ver se é um JSON de erro
                with open(tmp_path, 'r') as f:
                    content = f.read()
                    raise Exception(f"Arquivo muito pequeno, provável erro: {content}")

            # Tenta carregar o JSON (Teste final de integridade)
            with open(tmp_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Se chegou aqui, sucesso!
            gc.collect() # Limpeza de memória
            
            # Normalização do retorno
            if isinstance(data, dict) and "items" in data:
                return data["items"]
            if isinstance(data, list):
                return data
            return []

        except (json.JSONDecodeError, requests.exceptions.ChunkedEncodingError, Exception) as e:
            st.warning(f"⚠️ Tentativa {attempt}/{max_attempts} falhou: {str(e)}. Tentando novamente...")
            # Remove arquivo corrompido
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            
            # Se for a última tentativa, erro fatal
            if attempt == max_attempts:
                st.error(f"❌ Falha crítica após {max_attempts} tentativas. O servidor está interrompendo a conexão.")
                return None
        
        finally:
            session.close()
            # Limpeza final do arquivo se ainda existir
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except:
                    pass

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
    gc.collect()
    
    if tipo_relatorio == "Motoristas":
        url = "https://sigyo.uzzipay.com/api/motoristas"
        params = {
            'expand': 'grupos_vinculados,modulos,empresas,empresas.municipio,empresas.municipio.estado',
            'inline': 'false'
        }
        with st.spinner("Baixando base de Motoristas... (Isso pode demorar alguns minutos)"):
            raw_data = fetch_data_safe(url, api_token, params)
        
        if raw_data is not None:
            with st.spinner("Processando dados..."):
                st.session_state['df_motoristas'] = process_motoristas(raw_data)
            st.success("Dados de Motoristas atualizados!")
            del raw_data
            gc.collect()

    elif tipo_relatorio == "Credenciados":
        url = "https://sigyo.uzzipay.com/api/credenciados"
        params = {'expand': 'dadosAcesso,municipio,municipio.estado,modulos', 'inline': 'false'}
        with st.spinner("Baixando base de Credenciados..."):
            raw_data = fetch_data_safe(url, api_token, params)
        
        if raw_data is not None:
            with st.spinner("Processando dados..."):
                st.session_state['df_credenciados'] = process_credenciados(raw_data)
            st.success("Dados de Credenciados atualizados!")
            del raw_data
            gc.collect()

    elif tipo_relatorio == "Clientes":
        url = "https://sigyo.uzzipay.com/api/clientes"
        params = {'expand': 'municipio,municipio.estado,modulos,organizacao,tipo', 'inline': 'false'}
        with st.spinner("Baixando base de Clientes..."):
            raw_data = fetch_data_safe(url, api_token, params)
        
        if raw_data is not None:
            with st.spinner("Processando dados..."):
                st.session_state['df_clientes'] = process_clientes(raw_data)
            st.success("Dados de Clientes atualizados!")
            del raw_data
            gc.collect()

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
        # Tenta encontrar status para filtro
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
