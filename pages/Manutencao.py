# /pages/ConsultaGeral.py

import streamlit as st
import pandas as pd
import requests
import io
import database as db  # Importa o módulo do banco de dados local

# --- VERIFICAÇÃO DE LOGIN ---
if not st.session_state.get('logged_in'):
    st.error("🔒 Você precisa estar logado para acessar esta página.")
    st.info("Por favor, retorne à página de Login e insira suas credenciais.")
    st.stop()

# --- FUNÇÕES AUXILIARES ---

@st.cache_data(ttl=600) # Cache para otimizar chamadas repetidas
def fetch_api_data(base_url, endpoint):
    """Função genérica para buscar dados de um endpoint da API Logpay."""
    full_url = f"{base_url}{endpoint}"
    try:
        response = requests.get(full_url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
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
    """Função para exibir um DataFrame e o botão de download."""
    st.subheader(title)
    if not data:
        st.info("Nenhum dado encontrado para esta consulta.")
        return

    try:
        df = pd.DataFrame(data)
        st.dataframe(df, use_container_width=True, hide_index=True)
        
        excel_data = to_excel(df, sheet_name=title)
        st.download_button(
            label=f"📥 Baixar {title} como XLSX",
            data=excel_data,
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        st.error(f"Ocorreu um erro ao processar os dados de '{title}': {e}")
        st.json(data)

# --- INTERFACE DA APLICAÇÃO ---

st.set_page_config(layout="wide", page_title="Consulta Geral")

st.title("🔎 Painel de Consulta Geral")
st.markdown("Visualize e exporte dados centralizados de usuários, clientes e credenciados do sistema.")

# --- INPUT DA URL DA API ---
with st.container(border=True):
    st.subheader("Configuração da API")
    api_base_url = st.text_input(
        "URL Base da API Logpay",
        value="https://services.host.logpay.com.br",
        help="Insira o endereço principal da API que será consultada."
    )

# --- ABAS PARA CADA TIPO DE CONSULTA ---
tab_users, tab_clients, tab_establishments = st.tabs([
    "👤 Usuários do Sistema",
    "🏢 Clientes",
    "🏪 Credenciados (Estabelecimentos)"
])

# --- ABA 1: USUÁRIOS DO SISTEMA (BANCO DE DADOS LOCAL) ---
with tab_users:
    st.header("Gerenciamento de Usuários do Sistema")
    st.info("Estes são os usuários cadastrados no banco de dados local desta aplicação.")
    
    with st.spinner("Carregando usuários..."):
        all_users = db.get_all_users()
        display_data_section("Usuários Cadastrados", all_users, "usuarios_sistema.xlsx")

# --- ABA 2: CLIENTES (API LOGPAY) ---
with tab_clients:
    st.header("Consulta de Clientes via API")
    if st.button("Buscar Todos os Clientes", key="fetch_clients"):
        with st.spinner("Consultando API de Clientes..."):
            # Usamos o endpoint que retorna informações adicionais para mais detalhes
            client_data = fetch_api_data(api_base_url, "/api/Cliente/AdditionalInformation")
            # Armazena em session_state para evitar nova busca ao interagir com a UI
            st.session_state['client_data'] = client_data

    if 'client_data' in st.session_state:
        display_data_section("Clientes", st.session_state['client_data'], "clientes_logpay.xlsx")

# --- ABA 3: CREDENCIADOS (API LOGPAY) ---
with tab_establishments:
    st.header("Consulta de Credenciados (Estabelecimentos) via API")
    st.write("Consulte os estabelecimentos e seus dados relacionados, como taxas.")

    if st.button("Buscar Todos os Credenciados", key="fetch_establishments"):
        with st.spinner("Consultando API de Credenciados..."):
            # Usamos o endpoint com informações adicionais
            establishment_data = fetch_api_data(api_base_url, "/api/Estabelecimento/AdditionalInformation")
            st.session_state['establishment_data'] = establishment_data

    if 'establishment_data' in st.session_state:
        display_data_section("Credenciados", st.session_state['establishment_data'], "credenciados_logpay.xlsx")

    st.divider()
    
    # Seção para dados relacionados (ex: Taxas)
    st.subheader("Dados Relacionados a Credenciados")
    if st.button("Buscar Credenciados com Taxas", key="fetch_establishments_taxas"):
        with st.spinner("Buscando credenciados e suas taxas..."):
            taxas_data = fetch_api_data(api_base_url, "/api/Estabelecimento/Taxas")
            st.session_state['taxas_data'] = taxas_data

    if 'taxas_data' in st.session_state:
        display_data_section("Credenciados com Taxas", st.session_state['taxas_data'], "credenciados_com_taxas.xlsx")
