import streamlit as st
import requests
import pandas as pd
import io

# Função para buscar os dados da API
# Utiliza o cache do Streamlit para evitar novas chamadas à API a cada interação do usuário
@st.cache_data
def fetch_data(api_token):
    """
    Busca os dados da API de credenciados usando um token de autorização.

    Args:
        api_token (str): O token de API para autenticação.

    Returns:
        list: Uma lista de dicionários com os dados dos credenciados ou None em caso de erro.
    """
    url = "https://sigyo.uzzipay.com/api/credenciados?expand=dadosAcesso,municipio,municipio.estado,modulos"
    headers = {
        "Authorization": f"Bearer {api_token}"
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()  # Lança um erro para respostas com status 4xx/5xx
        return response.json()
    except requests.exceptions.HTTPError as err:
        if response.status_code == 401:
            st.error("Erro de autenticação: Token de API inválido ou expirado.")
        else:
            st.error(f"Erro HTTP ao buscar dados: {err}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Erro de conexão ao buscar dados: {e}")
        return None

# Função para achatar a estrutura JSON e prepará-la para o DataFrame
def flatten_data(data):
    """
    Transforma a lista de dicionários aninhados da API em uma lista "achatada".

    Args:
        data (list): A lista de dados vinda da API.

    Returns:
        list: Uma lista de dicionários com a estrutura de dados simplificada.
    """
    processed_data = []
    for entry in data:
        # Extrai os módulos e os concatena em uma string
        modulos = ", ".join([modulo['nome'] for modulo in entry.get('modulos', [])])

        flat_entry = {
            'ID': entry.get('id'),
            'Nome Fantasia': entry.get('nome'),
            'Razão Social': entry.get('razao_social'),
            'CNPJ': entry.get('cnpj'),
            'Situação': entry.get('situacao'),
            'Ativo': entry.get('ativo'),
            'Data de Cadastro': entry.get('data_cadastro'),
            'Telefone': entry.get('telefone'),
            'Email Responsável': entry.get('dadosAcesso', {}).get('email_responsavel'),
            'Nome Responsável': entry.get('dadosAcesso', {}).get('nome_responsavel'),
            'Município': entry.get('municipio', {}).get('nome'),
            'Estado': entry.get('municipio', {}).get('estado', {}).get('nome'),
            'UF': entry.get('municipio', {}).get('estado', {}).get('sigla'),
            'Logradouro': entry.get('logradouro'),
            'Número': entry.get('numero'),
            'Bairro': entry.get('bairro'),
            'CEP': entry.get('cep'),
            'Módulos': modulos
        }
        processed_data.append(flat_entry)
    return processed_data

# Função para converter o DataFrame para um arquivo Excel em memória
def to_excel(df):
    """
    Converte um DataFrame do Pandas para um arquivo Excel (formato XLSX) em memória.

    Args:
        df (pd.DataFrame): O DataFrame a ser convertido.

    Returns:
        bytes: O conteúdo do arquivo Excel em bytes.
    """
    output = io.BytesIO()
    # 'engine="openpyxl"' é necessário para o formato .xlsx
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Credenciados')
    processed_data = output.getvalue()
    return processed_data

# --- Interface da Aplicação Streamlit ---

st.set_page_config(layout="wide", page_title="Visualizador de Credenciados")

st.title("📄 Visualizador e Exportador de Credenciados")
st.markdown("Insira seu Token de API para buscar os dados, selecionar as colunas desejadas e exportar para Excel.")

# Campo para inserção do Token
api_token = st.text_input("Insira seu Token de API", type="password")

if api_token:
    # Busca os dados da API
    json_data = fetch_data(api_token)

    if json_data:
        # Processa e "achata" o JSON para um formato tabular
        processed_data = flatten_data(json_data)
        
        # Cria o DataFrame com os dados processados
        df = pd.DataFrame(processed_data)

        st.header("Visualização dos Dados")
        st.info(f"Total de {len(df)} registros encontrados.")

        # Seleção de colunas
        all_columns = df.columns.tolist()
        default_columns = ['Nome Fantasia', 'CNPJ', 'Situação', 'Município', 'UF', 'Nome Responsável', 'Email Responsável']
        
        # Garante que as colunas padrão existam no dataframe antes de usá-las
        valid_default_columns = [col for col in default_columns if col in all_columns]

        selected_columns = st.multiselect(
            "Selecione as colunas que deseja visualizar e exportar:",
            options=all_columns,
            default=valid_default_columns
        )

        if selected_columns:
            # Filtra o DataFrame com base nas colunas selecionadas
            df_selected = df[selected_columns]

            # Mostra os dados na tela
            st.dataframe(df_selected, use_container_width=True)

            st.header("Exportar Dados")

            # Botão de download para o arquivo XLSX
            excel_data = to_excel(df_selected)
            st.download_button(
                label="📥 Baixar dados selecionados como XLSX",
                data=excel_data,
                file_name="credenciados_selecionados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("Por favor, selecione ao menos uma coluna para visualizar os dados.")
else:
    st.info("Aguardando a inserção do Token de API para iniciar a consulta.")
