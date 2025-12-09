import streamlit as st
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Configuração da Página ---
st.set_page_config(
    page_title="Diagnóstico Sigyo",
    layout="wide"
)

st.title("🛠️ Diagnóstico de Estrutura API Sigyo")
st.markdown("Use esta ferramenta para ver exatamente quais colunas a API está retornando.")

# --- Barra Lateral ---
with st.sidebar:
    st.header("Configurações")
    default_token = st.secrets.get("eliq_api_token", "")
    api_token = st.text_input("Token de Acesso (Bearer)", value=default_token, type="password")
    
    st.markdown("---")
    endpoint_option = st.radio(
        "Selecione a API para testar:",
        ["Motoristas", "Credenciados", "Clientes"]
    )

# --- Função de Rede Robusta ---
def get_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

# --- Lógica Principal ---
if st.button("🕵️ Investigar Estrutura da API"):
    if not api_token:
        st.error("Insira o Token na barra lateral.")
        st.stop()

    # 1. Definição das URLs e Parâmetros (baseado no que você enviou)
    if endpoint_option == "Motoristas":
        url = "https://sigyo.uzzipay.com/api/motoristas"
        # expand complexo
        expand = "grupos_vinculados,modulos,empresas,empresas.municipio,empresas.municipio.estado"
    
    elif endpoint_option == "Credenciados":
        url = "https://sigyo.uzzipay.com/api/credenciados"
        expand = "dadosAcesso,municipio,municipio.estado,modulos"
    
    elif endpoint_option == "Clientes":
        url = "https://sigyo.uzzipay.com/api/clientes"
        expand = "municipio,municipio.estado,modulos,organizacao,tipo"

    # Parâmetros para buscar POUCOS dados (apenas para ver colunas)
    params = {
        'expand': expand,
        'per-page': 5,  # Pede apenas 5 itens
        'limit': 5,
        'page': 1
    }
    
    headers = {'Authorization': f'Bearer {api_token}'}
    session = get_session()

    try:
        with st.spinner(f"Consultando estrutura de {endpoint_option}..."):
            response = session.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code != 200:
                st.error(f"Erro na API: {response.status_code}")
                st.text(response.text)
                st.stop()

            data = response.json()

            # Normalização para encontrar a lista de dados
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict) and 'items' in data:
                items = data['items']
            else:
                st.warning("Formato de resposta desconhecido (não é lista nem dict com 'items').")
                st.write(data)
                st.stop()

            if not items:
                st.warning("A API retornou uma lista vazia. Não há dados para mapear.")
                st.stop()

            # --- EXIBIÇÃO DOS RESULTADOS ---
            
            # 1. JSON Puro (Primeiro item)
            st.subheader("1. Amostra do JSON Bruto (1º registro)")
            st.json(items[0])

            # 2. Flattening (Achatamento) para ver colunas
            # max_level=3 garante que objetos muito aninhados virem colunas tipo 'empresa.municipio.nome'
            df = pd.json_normalize(items, sep='_') 
            
            st.subheader("2. Colunas Identificadas")
            col_list = df.columns.tolist()
            
            # Exibe lista para copiar fácil
            st.text_area("Copie estas colunas e me envie:", value=", ".join(col_list), height=150)
            
            st.subheader("3. Visualização em Tabela (Amostra)")
            st.dataframe(df)

            # Botão para baixar essa amostra
            csv = df.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                "📥 Baixar CSV de Amostra (Estrutura)",
                csv,
                "estrutura_api.csv",
                "text/csv"
            )

    except Exception as e:
        st.error(f"Erro ao executar diagnóstico: {e}")
