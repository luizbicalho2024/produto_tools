# /pages/ConsultaGeral.py

import streamlit as st
import pandas as pd
import requests
import io

# --- URL Base da API (Fixa) ---
# A URL foi movida para uma constante, tirando-a da interface do usuário.
API_BASE_URL = "https://services.host.logpay.com.br"

# --- VERIFICAÇÃO DE LOGIN ---
if not st.session_state.get('logged_in'):
    st.error("🔒 Você precisa estar logado para acessar esta página.")
    st.info("Por favor, retorne à página de Login e insira suas credenciais.")
    st.stop()

# --- FUNÇÕES AUXILIARES ---

@st.cache_data(ttl=600)
def fetch_api_data(endpoint, username, password):
    """
    Função genérica para buscar dados da API Logpay usando autenticação Basic.
    A URL base agora é uma constante.
    """
    full_url = f"{API_BASE_URL}{endpoint}"
    try:
        response = requests.get(full_url, auth=(username, password), timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
        if err.response.status_code == 401:
            st.error(f"Erro de autenticação [401] ao acessar {endpoint}. Verifique o usuário e a senha da API.")
        else:
            st.error(f"Erro HTTP ao acessar {endpoint}: {err.response.status_code} - {err.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Erro de conexão com a API: {e}")
        return None
    except ValueError:
        st.error(f"A resposta do endpoint {endpoint} não é um JSON válido.")
        st.code(response.text)
        return None

def to_excel(df, sheet_name='Dados'):
    """Converte um DataFrame para um arquivo Excel em memória."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()

def display_data_section(title, data, file_name):
    """
    Função aprimorada para exibir um DataFrame com seletor de colunas e botão de download.
    """
    st.subheader(title)
    if data is None:
        return
    if not data:
        st.info("Nenhum dado encontrado para esta consulta.")
        return

    try:
        df = pd.DataFrame(data)
        
        all_columns = sorted(df.columns.tolist())
        selected_columns = st.multiselect(
            "Selecione as colunas para visualizar e exportar:",
            options=all_columns,
            default=all_columns,
            key=f"multiselect_{file_name}"
        )

        if not selected_columns:
            st.warning("Por favor, selecione ao menos uma coluna para exibir os dados.")
            return
            
        df_selected = df[selected_columns]
        st.dataframe(df_selected, use_container_width=True, hide_index=True)
        
        excel_data = to_excel(df_selected, sheet_name=title)
        st.download_button(
            label=f"📥 Baixar dados selecionados como XLSX",
            data=excel_data,
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"download_{file_name}"
        )
    except Exception as e:
        st.error(f"Ocorreu um erro ao processar os dados de '{title}': {e}")
        st.json(data)

# --- INTERFACE DA APLICAÇÃO ---

st.set_page_config(layout="wide", page_title="Consulta API")

st.title("🔎 Painel de Consulta API Logpay")
st.markdown("Visualize e exporte dados de clientes e credenciados do sistema.")

# --- INPUT DAS CREDENCIAIS DA API ---
with st.container(border=True):
    st.subheader("Autenticação da API")
    col1, col2 = st.columns(2) # Layout ajustado para 2 colunas
    with col1:
        api_username = st.text_input(
            "API Username",
            help="Usuário para autenticação Basic na API."
        )
    with col2:
        api_password = st.text_input(
            "API Password",
            type="password",
            help="Senha para autenticação Basic na API."
        )

# --- ABAS PARA CADA TIPO DE CONSULTA ---
tab_clients, tab_establishments = st.tabs([
    "🏢 Clientes",
    "🏪 Credenciados (Estabelecimentos)"
])

def can_fetch_data():
    if not api_username or not api_password:
        st.warning("Por favor, preencha o Username e o Password da API para continuar.")
        return False
    return True

# --- ABA 1: CLIENTES (API LOGPAY) ---
with tab_clients:
    st.header("Consulta de Clientes via API")
    if st.button("Buscar Todos os Clientes", key="fetch_clients"):
        if can_fetch_data():
            with st.spinner("Consultando API de Clientes..."):
                client_data = fetch_api_data("/api/Cliente", api_username, api_password)
                st.session_state['client_data'] = client_data

    if 'client_data' in st.session_state:
        display_data_section("Clientes", st.session_state['client_data'], "clientes_logpay.xlsx")

# --- ABA 2: CREDENCIADOS (API LOGPAY) ---
with tab_establishments:
    st.header("Consulta de Credenciados (Estabelecimentos) via API")
    st.write("Consulte os estabelecimentos e seus dados relacionados, como taxas.")

    if st.button("Buscar Todos os Credenciados", key="fetch_establishments"):
        if can_fetch_data():
            with st.spinner("Consultando API de Credenciados..."):
                establishment_data = fetch_api_data("/api/Estabelecimento/AdditionalInformation", api_username, api_password)
                st.session_state['establishment_data'] = establishment_data

    if 'establishment_data' in st.session_state:
        display_data_section("Credenciados", st.session_state['establishment_data'], "credenciados_logpay.xlsx")

    st.divider()
    
    st.subheader("Dados Relacionados a Credenciados")
    if st.button("Buscar Credenciados com Taxas", key="fetch_establishments_taxas"):
        if can_fetch_data():
            with st.spinner("Buscando credenciados e suas taxas..."):
                taxas_data = fetch_api_data("/api/Estabelecimento/Taxas", api_username, api_password)
                st.session_state['taxas_data'] = taxas_data

    if 'taxas_data' in st.session_state:
        display_data_section("Credenciados com Taxas", st.session_state['taxas_data'], "credenciados_com_taxas.xlsx")
