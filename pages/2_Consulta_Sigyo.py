import streamlit as st
import pandas as pd
import requests
import json
import tempfile
import os
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
# FUNÇÕES DE REDE (RETROCOMPATÍVEL E SEGURA PARA MEMÓRIA)
# ==============================================================================

@st.cache_data(ttl=900, show_spinner=False)
def fetch_data_safe(url, token, params=None):
    """
    Baixa os dados salvando em disco primeiro para economizar memória RAM.
    Evita o erro de 'Unterminated string' e timeouts do Streamlit Cloud.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    session = requests.Session()
    adapter = HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"]
        )
    )
    session.mount("https://", adapter)

    # Cria um arquivo temporário para armazenar o download
    fd, tmp_path = tempfile.mkstemp(suffix=".json")
    os.close(fd)  # Fecha o descritor de arquivo de baixo nível, vamos usar open() depois

    try:
        # Timeout aumentado: (10s conexão, 120s leitura) para suportar arquivos de 40MB+
        with session.get(url, headers=headers, params=params, stream=True, timeout=(10, 120)) as response:
            response.raise_for_status()
            
            # Baixa em pedaços (chunks) para o disco
            with open(tmp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        
        # Agora carregamos do disco para o JSON (isso é mais estável que carregar da rede direto)
        with open(tmp_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Limpa memória explicitamente
        gc.collect()

        # Normalização do retorno
        if isinstance(data, dict) and "items" in data:
            return data["items"]
        if isinstance(data, list):
            return data
            
        return []

    except requests.exceptions.Timeout:
        st.error("⏱️ A API demorou demais para responder (Timeout > 120s).")
        return None
    except json.JSONDecodeError as e:
        st.error(f"❌ Erro ao ler o JSON (Arquivo incompleto ou corrompido): {e}")
        return None
    except Exception as e:
        st.error(f"❌ Erro inesperado: {e}")
        return None
    finally:
        # Remove o arquivo temporário
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        session.close()

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
    # Otimização: List Comprehension é mais rápida que loop for append
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
    # Conversões otimizadas
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
    # Força a limpeza de memória antes de iniciar
    gc.collect()
    
    if tipo_relatorio == "Motoristas":
        url = "https://sigyo.uzzipay.com/api/motoristas"
        params = {
            'expand': 'grupos_vinculados,modulos,empresas,empresas.municipio,empresas.municipio.estado',
            'inline': 'false'
        }
        with st.spinner("Baixando base de Motoristas... (Isso pode levar de 1 a 2 minutos)"):
            raw_data = fetch_data_safe(url, api_token, params)
        
        if raw_data is not None:
            with st.spinner("Processando dados..."):
                st.session_state['df_motoristas'] = process_motoristas(raw_data)
            st.success("Dados de Motoristas atualizados!")
            # Limpa a variável raw_data da memória
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
