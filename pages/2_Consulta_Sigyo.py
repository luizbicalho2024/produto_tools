import streamlit as st
import requests
import pandas as pd
import io

# --- VERIFICAÇÃO DE LOGIN (GUARDA DE PÁGINA) ---
# Esta é a parte mais importante para a segurança.
# Se o usuário não estiver logado, a execução da página para aqui.
if not st.session_state.get('logged_in'):
    st.error("🔒 Você precisa estar logado para acessar esta página.")
    st.info("Por favor, retorne à página de Login e insira suas credenciais.")
    st.stop()  # Interrompe a execução do script

# --- FUNÇÕES DE DADOS (APRIMORADAS) ---

@st.cache_data
def fetch_data(api_token):
    """Busca os dados da API de credenciados."""
    url = "https://sigyo.uzzipay.com/api/credenciados?expand=dadosAcesso,municipio,municipio.estado,modulos"
    headers = {"Authorization": f"Bearer {api_token}"}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
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

def flatten_data_fully(data):
    """
    Função aprimorada para achatar o JSON, capturando TODOS os campos,
    incluindo os de 'dadosAcesso', 'municipio' e 'estado'.
    """
    processed_data = []
    for entry in data:
        flat_entry = {}
        
        # Adiciona chaves do nível principal
        for key, value in entry.items():
            if not isinstance(value, (dict, list)):
                flat_entry[key] = value

        # Adiciona chaves do 'dadosAcesso'
        if 'dadosAcesso' in entry and entry['dadosAcesso']:
            for key, value in entry['dadosAcesso'].items():
                flat_entry[f"acesso_{key}"] = value

        # Adiciona chaves do 'municipio'
        if 'municipio' in entry and entry['municipio']:
            for key, value in entry['municipio'].items():
                if not isinstance(value, dict):
                    flat_entry[f"municipio_{key}"] = value
            
            # Adiciona chaves do 'estado' dentro de 'municipio'
            if 'estado' in entry['municipio'] and entry['municipio']['estado']:
                for key, value in entry['municipio']['estado'].items():
                    flat_entry[f"estado_{key}"] = value
        
        # Concatena os módulos
        if 'modulos' in entry and entry['modulos']:
            flat_entry["modulos"] = ", ".join([mod['nome'] for mod in entry['modulos']])
            
        processed_data.append(flat_entry)
    return processed_data

def to_excel(df):
    """Converte um DataFrame para um arquivo Excel em memória."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Credenciados')
    return output.getvalue()

# --- INTERFACE DA APLICAÇÃO ---

st.set_page_config(layout="wide", page_title="Dados dos Credenciados")

st.title("📄 Visualizador e Exportador de Credenciados")
st.markdown("Insira seu Token de API para buscar os dados, selecionar as colunas desejadas e exportar para Excel.")

api_token = st.text_input("Insira seu Token de API", type="password")

if api_token:
    json_data = fetch_data(api_token)

    if json_data:
        processed_data = flatten_data_fully(json_data)
        df = pd.DataFrame(processed_data)

        st.header("Visualização dos Dados")
        st.info(f"Total de {len(df)} registros encontrados.")

        all_columns = sorted(df.columns.tolist())
        
        # Sugestão de colunas padrão (pode personalizar)
        default_columns = [col for col in ['nome', 'cnpj', 'situacao', 'municipio_nome', 'estado_sigla', 'acesso_nome_responsavel', 'acesso_email_responsavel'] if col in all_columns]

        selected_columns = st.multiselect(
            "Selecione as colunas que deseja visualizar e exportar:",
            options=all_columns,
            default=default_columns
        )

        if selected_columns:
            df_selected = df[selected_columns]
            st.dataframe(df_selected, use_container_width=True)

            st.header("Exportar Dados")
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
