# /pages/Manutencao.py

import streamlit as st
import requests
import pandas as pd
import io

# --- VERIFICAÇÃO DE LOGIN (GUARDA DE PÁGINA) ---
if not st.session_state.get('logged_in'):
    st.error("🔒 Você precisa estar logado para acessar esta página.")
    st.info("Por favor, retorne à página de Login e insira suas credenciais.")
    st.stop()  # Interrompe a execução do script

# --- FUNÇÕES DE DADOS ---

@st.cache_data(ttl=300) # Adicionado cache para evitar múltiplas chamadas repetidas
def fetch_maintenance_data(base_url, endpoint, establishment_id, user_id):
    """
    Busca os dados da API de Manutenção da Logpay.
    Nota: Esta função assume autenticação via Header, similar a outras APIs.
    Se a autenticação for diferente, o header pode precisar de ajuste.
    """
    # Monta a URL completa
    url = f"{base_url}/api/appmanutencao/{endpoint}/{establishment_id}/{user_id}"
    
    # Exemplo de Headers. Adapte se a autenticação for diferente (ex: Bearer Token)
    headers = {
        "Content-Type": "application/json",
        # "Authorization": f"Bearer {api_token}" # Descomente e ajuste se usar token
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status() # Lança um erro para códigos HTTP 4xx/5xx
        return response.json()
    except requests.exceptions.HTTPError as err:
        st.error(f"Erro HTTP ao buscar dados: {err.response.status_code} - {err.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Erro de conexão ao buscar dados: {e}")
        return None
    except ValueError: # Captura erros de JSON decoding
        st.error("A resposta da API não é um JSON válido. Resposta recebida:")
        st.code(response.text)
        return None


def to_excel(df):
    """Converte um DataFrame para um arquivo Excel em memória."""
    output = io.BytesIO()
    # Usa openpyxl como engine para compatibilidade com .xlsx
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados')
    return output.getvalue()

# --- MAPEAMENTO DE ENDPOINTS ---
# Dicionário para mapear nomes amigáveis para os endpoints da API
ENDPOINT_OPTIONS = {
    "OS Pendentes": "ospending",
    "OS Aguardando Aprovação": "oswaitingapproval",
    "OS Veículo Entregue": "osvehicledelivered",
    "OS Aprovadas": "osapproved",
    "OS Finalizadas": "osfinished",
    "Faturas": "invoice"
}

# --- INTERFACE DA APLICAÇÃO ---

st.set_page_config(layout="wide", page_title="Consultas de Manutenção")

st.title("📊 Consulta e Exportação - Módulo Manutenção")
st.markdown("Selecione o tipo de consulta, preencha os parâmetros e visualize ou exporte os dados.")

# --- FORMULÁRIO DE ENTRADA ---
with st.container(border=True):
    st.subheader("Parâmetros da Consulta")
    
    # A base da URL da API
    base_url = st.text_input("URL Base da API", value="https://services.host.logpay.com.br")
    
    # Seleção do tipo de consulta
    selected_option = st.selectbox(
        "Selecione o tipo de dado que deseja consultar:",
        options=list(ENDPOINT_OPTIONS.keys())
    )
    
    # Inputs para os IDs necessários
    col1, col2 = st.columns(2)
    with col1:
        establishment_id = st.text_input("ID do Estabelecimento (EstablishmentID)")
    with col2:
        user_id = st.text_input("ID do Usuário (UserID)")
    
    # Botão para iniciar a busca
    if st.button("Buscar Dados", type="primary"):
        endpoint = ENDPOINT_OPTIONS[selected_option]
        
        # Validação simples dos inputs
        if base_url and endpoint and establishment_id and user_id:
            with st.spinner(f"Buscando {selected_option}..."):
                json_data = fetch_maintenance_data(base_url, endpoint, establishment_id, user_id)
                
                # Armazena os dados no session_state para persistir
                st.session_state['maintenance_data'] = json_data 
                st.session_state['last_query'] = selected_option # Guarda o nome da última consulta
        else:
            st.warning("Por favor, preencha todos os campos para realizar a busca.")

# --- EXIBIÇÃO DOS DADOS E EXPORTAÇÃO ---
if 'maintenance_data' in st.session_state and st.session_state['maintenance_data'] is not None:
    
    data = st.session_state['maintenance_data']
    
    if not data:
        st.info("A consulta não retornou resultados.")
    else:
        try:
            df = pd.DataFrame(data)

            st.header(f"Resultados para: {st.session_state.get('last_query', 'Consulta')}")
            st.info(f"Total de {len(df)} registros encontrados.")

            all_columns = sorted(df.columns.tolist())
            
            # Deixa todas as colunas selecionadas por padrão
            selected_columns = st.multiselect(
                "Selecione as colunas que deseja visualizar e exportar:",
                options=all_columns,
                default=all_columns
            )

            if selected_columns:
                df_selected = df[selected_columns]
                st.dataframe(df_selected, use_container_width=True)

                st.header("Exportar Dados")
                excel_data = to_excel(df_selected)
                
                # Gera um nome de arquivo dinâmico
                file_name = f"export_{ENDPOINT_OPTIONS[st.session_state['last_query']]}_{establishment_id}.xlsx"
                
                st.download_button(
                    label="📥 Baixar dados selecionados como XLSX",
                    data=excel_data,
                    file_name=file_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.warning("Por favor, selecione ao menos uma coluna para visualizar os dados.")
        
        except Exception as e:
            st.error(f"Ocorreu um erro ao processar os dados recebidos: {e}")
            st.json(data) # Mostra o JSON bruto se não for possível converter para DataFrame
