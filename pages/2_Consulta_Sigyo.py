import streamlit as st
import pandas as pd
import requests
import json
import io
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Configuração da Página ---
st.set_page_config(
    page_title="Consulta Sigyo",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🔍 Consulta Cadastral Sigyo")

# --- Barra Lateral (Configurações Globais) ---
with st.sidebar:
    st.header("Configurações")
    default_token = st.secrets.get("eliq_api_token", "")
    api_token = st.text_input("Token de Acesso (Bearer)", value=default_token, type="password")
    
    st.markdown("---")
    st.header("Selecione a Base")
    tipo_relatorio = st.radio(
        "Qual cadastro deseja consultar?",
        ["Motoristas", "Credenciados"],
        index=0
    )

# ==============================================================================
# FUNÇÕES DE REDE ROBUSTA (SESSION COM RETRY)
# ==============================================================================

def get_retry_session():
    """Cria uma sessão HTTP blindada que tenta reconectar automaticamente."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# ==============================================================================
# FUNÇÃO GENÉRICA DE DOWNLOAD (STREAMING)
# ==============================================================================

def fetch_data_streaming(url, token, entity_name, params=None):
    """
    Função genérica para baixar grandes volumes de dados via JSON Streaming.
    """
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
    }
    
    if params is None:
        params = {'inline': 'false'} # Padrão para não vir meta-dados envelopando demais

    status_text = st.empty()
    progress_bar = st.progress(0)
    session = get_retry_session()

    try:
        status_text.text(f"Conectando à API de {entity_name}...")
        
        with session.get(url, headers=headers, params=params, stream=True, timeout=300) as response:
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            data_buffer = io.BytesIO()
            downloaded_size = 0
            chunk_size = 100 * 1024 # 100KB chunks
            
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    data_buffer.write(chunk)
                    downloaded_size += len(chunk)
                    
                    mb_downloaded = downloaded_size / (1024 * 1024)
                    if total_size > 0:
                        progress = min(downloaded_size / total_size, 1.0)
                        status_text.text(f"Baixando {entity_name}: {mb_downloaded:.2f} MB...")
                        progress_bar.progress(progress)
                    else:
                        status_text.text(f"Baixando {entity_name}: {mb_downloaded:.2f} MB recebidos...")

            progress_bar.progress(1.0)
            status_text.text("Download concluído! Processando dados...")
            
            data_buffer.seek(0)
            try:
                json_content = json.load(data_buffer)
            except json.JSONDecodeError:
                st.error("Erro ao decodificar o arquivo baixado. O download pode ter sido corrompido.")
                return None
            
            # Normaliza resposta (algumas vêm como lista, outras como dict com chave 'items')
            if isinstance(json_content, dict) and 'items' in json_content:
                return json_content['items']
            elif isinstance(json_content, list):
                return json_content
            else:
                st.warning(f"Formato de resposta inesperado para {entity_name}.")
                return []

    except Exception as e:
        st.error(f"Erro ao buscar {entity_name}: {e}")
        return None
    finally:
        status_text.empty()
        progress_bar.empty()

# ==============================================================================
# 1. PROCESSAMENTO DE MOTORISTAS
# ==============================================================================

def process_motoristas(all_data):
    if not all_data: return pd.DataFrame()

    def extract_names(item_list):
        if not isinstance(item_list, list): return ""
        return ", ".join([str(i.get('nome', '')) for i in item_list if isinstance(i, dict) and i.get('nome')])

    def extract_empresas(empresa_list):
        if not isinstance(empresa_list, list): return ""
        nomes = []
        for emp in empresa_list:
            if isinstance(emp, dict):
                nome = emp.get('nome_fantasia') or emp.get('razao_social') or 'N/A'
                cnpj = emp.get('cnpj', '')
                nomes.append(f"{nome} ({cnpj})")
        return "; ".join(nomes)

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

    output = pd.DataFrame(processed_rows)
    # Formatação Datas
    for col in ['Validade CNH', 'Data Cadastro']:
        if col in output.columns:
            output[col] = pd.to_datetime(output[col], errors='coerce')
            if col == 'Validade CNH': output[col] = output[col].dt.strftime('%d/%m/%Y')
            else: output[col] = output[col].dt.strftime('%d/%m/%Y %H:%M')
            
    return output

# ==============================================================================
# 2. PROCESSAMENTO DE CREDENCIADOS (NOVO)
# ==============================================================================

def process_credenciados(all_data):
    if not all_data: return pd.DataFrame()

    # Função auxiliar para extrair endereço completo se vier aninhado ou separado
    def format_address(d):
        parts = [
            d.get('logradouro') or d.get('endereco'),
            d.get('numero'),
            d.get('bairro'),
            d.get('cidade') or (d.get('municipio', {}).get('nome') if isinstance(d.get('municipio'), dict) else ''),
            d.get('uf') or (d.get('estado', {}).get('sigla') if isinstance(d.get('estado'), dict) else '')
        ]
        return ", ".join([str(p) for p in parts if p])

    processed_rows = []
    for d in all_data:
        if not isinstance(d, dict): continue
        
        # Mapeamento genérico de campos comuns de credenciados
        row = {
            'ID': d.get('id'),
            'Razão Social': d.get('razao_social'),
            'Nome Fantasia': d.get('nome_fantasia'),
            'CNPJ': d.get('cnpj'),
            'Email': d.get('email') or d.get('email_contato'),
            'Telefone': d.get('telefone') or d.get('celular'),
            'Contato': d.get('nome_contato') or d.get('responsavel'),
            'Status': d.get('status'),
            'Ativo': 'Sim' if d.get('ativo') in [True, 1] else 'Não',
            'Data Cadastro': d.get('data_cadastro'),
            'Endereço Completo': format_address(d),
            'Latitude': d.get('latitude'),
            'Longitude': d.get('longitude'),
            'Taxa Adm (%)': d.get('taxa_administracao') or d.get('taxa_adm')
        }
        processed_rows.append(row)

    output = pd.DataFrame(processed_rows)
    
    if 'Data Cadastro' in output.columns:
        output['Data Cadastro'] = pd.to_datetime(output['Data Cadastro'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
        
    return output

# ==============================================================================
# LÓGICA PRINCIPAL (INTERFACE)
# ==============================================================================

if not api_token:
    st.warning("⚠️ Por favor, insira o Token da API na barra lateral para continuar.")
    st.stop()

# ------------------------------------------------------------------------------
# OPÇÃO 1: MOTORISTAS
# ------------------------------------------------------------------------------
if tipo_relatorio == "Motoristas":
    st.subheader("📋 Base de Motoristas")
    st.markdown("Consulta cadastro completo de motoristas, CNH, grupos e empresas vinculadas.")

    if st.button("🔄 Baixar Base de Motoristas"):
        # URL da API de Motoristas
        url = "https://sigyo.uzzipay.com/api/motoristas"
        params = {
            'expand': 'grupos_vinculados,modulos,empresas,empresas.municipio,empresas.municipio.estado',
            'inline': 'false'
        }
        
        raw_data = fetch_data_streaming(url, api_token, "Motoristas", params)
        
        if raw_data:
            df_motoristas = process_motoristas(raw_data)
            st.session_state['df_motoristas'] = df_motoristas
            st.success(f"Sucesso! {len(df_motoristas)} motoristas carregados.")
        else:
            st.warning("Não foi possível carregar os dados.")

    if 'df_motoristas' in st.session_state and not st.session_state['df_motoristas'].empty:
        df = st.session_state['df_motoristas']
        
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            status_opts = sorted(df['Status'].astype(str).unique())
            filtro_status = st.multiselect("Filtrar por Status:", options=status_opts, default=status_opts)
        with col_f2:
            search_term = st.text_input("Buscar por Nome ou CNH:", "")

        mask = df['Status'].isin(filtro_status)
        if search_term:
            mask &= (
                df['Nome'].str.contains(search_term, case=False, na=False) | 
                df['CPF/CNH'].str.contains(search_term, na=False)
            )
        
        df_filtered = df[mask]

        st.markdown("### Selecionar Colunas para Exportação")
        all_cols = df_filtered.columns.tolist()
        cols_default = ['ID', 'Nome', 'CPF/CNH', 'Status', 'Empresas', 'Grupos Vinculados', 'Módulos']
        cols_default = [c for c in cols_default if c in all_cols]
        
        selected_cols = st.multiselect("Colunas:", all_cols, default=cols_default)

        if not selected_cols:
            st.error("Selecione pelo menos uma coluna.")
        else:
            df_display = df_filtered[selected_cols]
            st.markdown(f"**Registros exibidos:** {len(df_display)}")
            st.dataframe(df_display, use_container_width=True)
            
            csv = df_display.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📥 Baixar Planilha (CSV)",
                data=csv,
                file_name="base_motoristas_sigyo.csv",
                mime="text/csv",
                type="primary"
            )

# ------------------------------------------------------------------------------
# OPÇÃO 2: CREDENCIADOS (NOVO)
# ------------------------------------------------------------------------------
elif tipo_relatorio == "Credenciados":
    st.subheader("🏢 Base de Credenciados")
    st.markdown("Consulta dados cadastrais de estabelecimentos e credenciados (Razão Social, CNPJ, Email, Endereço).")

    if st.button("🔄 Baixar Base de Credenciados"):
        # URL da API de Credenciados
        # NOTA: Se o endpoint for 'clientes' ou 'empresas', ajuste a URL abaixo.
        # Padrão Sigyo costuma ser 'credenciados' para rede externa ou 'clientes' para B2B.
        url = "https://sigyo.uzzipay.com/api/credenciados" 
        
        # Parâmetros genéricos para trazer o máximo de info cadastral
        params = {
            'expand': 'municipio,municipio.estado', # Expansão comum de endereço
            'inline': 'false'
        }
        
        raw_data = fetch_data_streaming(url, api_token, "Credenciados", params)
        
        if raw_data:
            df_credenciados = process_credenciados(raw_data)
            st.session_state['df_credenciados'] = df_credenciados
            st.success(f"Sucesso! {len(df_credenciados)} credenciados carregados.")
        else:
            st.error("Falha ao buscar dados. Verifique se o endpoint da API é 'credenciados', 'clientes' ou 'estabelecimentos'.")

    if 'df_credenciados' in st.session_state and not st.session_state['df_credenciados'].empty:
        df = st.session_state['df_credenciados']
        
        # Filtros
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            if 'Status' in df.columns:
                status_opts = sorted(df['Status'].astype(str).unique())
                filtro_status = st.multiselect("Filtrar por Status:", options=status_opts, default=status_opts)
            else:
                filtro_status = []
        with col_f2:
            search_term = st.text_input("Buscar por Nome, CNPJ ou Email:", "")

        mask = pd.Series([True] * len(df))
        if filtro_status and 'Status' in df.columns:
            mask &= df['Status'].isin(filtro_status)
        
        if search_term:
            # Busca em várias colunas de texto
            text_cols = ['Razão Social', 'Nome Fantasia', 'CNPJ', 'Email']
            # Filtra colunas que realmente existem no DF
            text_cols = [c for c in text_cols if c in df.columns]
            
            mask_search = pd.Series([False] * len(df))
            for col in text_cols:
                mask_search |= df[col].astype(str).str.contains(search_term, case=False, na=False)
            mask &= mask_search
        
        df_filtered = df[mask]

        st.markdown("### Selecionar Colunas para Exportação")
        all_cols = df_filtered.columns.tolist()
        cols_default = ['ID', 'Nome Fantasia', 'CNPJ', 'Email', 'Telefone', 'Status', 'Endereço Completo']
        cols_default = [c for c in cols_default if c in all_cols]
        if not cols_default: cols_default = all_cols[:5]
        
        selected_cols = st.multiselect("Colunas:", all_cols, default=cols_default)

        if not selected_cols:
            st.error("Selecione pelo menos uma coluna.")
        else:
            df_display = df_filtered[selected_cols]
            st.markdown(f"**Registros exibidos:** {len(df_display)}")
            
            st.dataframe(df_display, use_container_width=True)
            
            csv = df_display.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📥 Baixar Planilha (CSV)",
                data=csv,
                file_name="base_credenciados_sigyo.csv",
                mime="text/csv",
                type="primary"
            )
