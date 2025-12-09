import streamlit as st
import pandas as pd
import requests
import io
from datetime import date, timedelta

# --- Configuração da Página ---
st.set_page_config(
    page_title="Consulta Sigyo",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🔍 Consulta API Sigyo")

# --- Barra Lateral (Configurações Globais) ---
with st.sidebar:
    st.header("Configurações")
    # Tenta pegar do secrets, se não, pede input
    default_token = st.secrets.get("eliq_api_token", "")
    api_token = st.text_input("Token de Acesso (Bearer)", value=default_token, type="password")
    
    st.markdown("---")
    tipo_relatorio = st.radio(
        "Tipo de Relatório",
        ["Transações", "Motoristas"],
        index=0
    )

# ==============================================================================
# FUNÇÕES DE BUSCA E PROCESSAMENTO (MOTORISTAS)
# ==============================================================================

@st.cache_data(show_spinner="Buscando motoristas...", ttl=300)
def fetch_motoristas_sigyo(token):
    """Busca todos os motoristas paginados e processa os dados aninhados."""
    base_url = "https://sigyo.uzzipay.com/api/motoristas"
    headers = {'Authorization': f'Bearer {token}'}
    
    # Parâmetros fixos conforme solicitado
    params = {
        'expand': 'grupos_vinculados,modulos,empresas,empresas.municipio,empresas.municipio.estado',
        'inline': 'false',
        'page': 1
    }
    
    all_data = []
    page = 1
    
    # Container para barra de progresso
    progress_text = "Iniciando busca de motoristas..."
    my_bar = st.progress(0, text=progress_text)
    
    try:
        while True:
            params['page'] = page
            response = requests.get(base_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Verifica se a resposta é uma lista (padrão Yii2 sem envelope ou com envelope dependendo da config)
            # Assumindo que a API retorna lista direta ou dicionário com chave de dados
            if isinstance(data, dict) and 'items' in data:
                items = data['items']
                meta = data.get('_meta', {})
                total_pages = meta.get('pageCount', 1)
            elif isinstance(data, list):
                items = data
                # Se retornar lista direta, difícil saber total de páginas sem headers, 
                # assumiremos que se a lista vier vazia ou menor que o esperado, acabou.
                # Para segurança, vamos limitar ou verificar headers se disponível.
                total_pages = int(response.headers.get('X-Pagination-Page-Count', 100)) # Fallback
            else:
                items = []
                total_pages = 1

            if not items:
                break
                
            all_data.extend(items)
            
            # Atualiza barra de progresso (estimativa)
            if total_pages > 0:
                percent = min(page / total_pages, 1.0)
                my_bar.progress(percent, text=f"Buscando página {page}...")
            
            # Critério de parada (se items vazio ou atingiu ultima pagina conhecida)
            if isinstance(data, dict) and page >= total_pages:
                break
            if isinstance(data, list) and not items:
                break
                
            page += 1
            
    except Exception as e:
        st.error(f"Erro na comunicação com a API: {e}")
        return pd.DataFrame()
    finally:
        my_bar.empty()

    if not all_data:
        return pd.DataFrame()

    # --- Normalização e Processamento ---
    df = pd.json_normalize(all_data)
    
    # Função auxiliar para extrair listas aninhadas em strings
    def extract_names(item_list, key='nome'):
        if not isinstance(item_list, list):
            return ""
        return ", ".join([str(i.get(key, '')) for i in item_list if i.get(key)])

    def extract_empresas(empresa_list):
        if not isinstance(empresa_list, list):
            return ""
        nomes = []
        for emp in empresa_list:
            # Tenta pegar nome fantasia, se não, razão social
            nome = emp.get('nome_fantasia') or emp.get('razao_social') or 'N/A'
            cnpj = emp.get('cnpj', '')
            nomes.append(f"{nome} ({cnpj})")
        return "; ".join(nomes)

    # Processamento das colunas complexas (usando os dados originais 'all_data' para garantir acesso às listas)
    # Recriamos um DF base para facilitar
    df_processed = pd.DataFrame(all_data)
    
    # Mapeamento e Tratamento
    output = pd.DataFrame()
    output['ID'] = df_processed.get('id')
    output['Nome'] = df_processed.get('nome')
    output['CPF/CNH'] = df_processed.get('cnh')
    output['Categoria CNH'] = df_processed.get('cnh_categoria')
    output['Validade CNH'] = pd.to_datetime(df_processed.get('cnh_validade'), errors='coerce').dt.strftime('%d/%m/%Y')
    output['Matrícula'] = df_processed.get('matricula')
    output['Email'] = df_processed.get('email')
    output['Telefone'] = df_processed.get('telefone')
    output['Status'] = df_processed.get('status')
    output['Ativo'] = df_processed.get('ativo').map({True: 'Sim', False: 'Não', 1: 'Sim', 0: 'Não'})
    
    # Listas Aninhadas
    output['Grupos Vinculados'] = df_processed.get('grupos_vinculados').apply(lambda x: extract_names(x, 'nome'))
    output['Empresas'] = df_processed.get('empresas').apply(extract_empresas)
    output['Módulos'] = df_processed.get('modulos').apply(lambda x: extract_names(x, 'nome'))
    
    output['Data Cadastro'] = pd.to_datetime(df_processed.get('data_cadastro'), errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
    
    return output

# ==============================================================================
# FUNÇÕES DE BUSCA E PROCESSAMENTO (TRANSAÇÕES - CÓDIGO EXISTENTE)
# ==============================================================================

@st.cache_data(show_spinner="Buscando transações...", ttl=300)
def fetch_transacoes_sigyo(token, start_date, end_date):
    """ Busca dados da API de Transações. """
    base_url = "https://sigyo.uzzipay.com/api/transacoes"
    headers = {'Authorization': f'Bearer {token}'}
    
    start_str = start_date.strftime('%d/%m/%Y')
    end_str = end_date.strftime('%d/%m/%Y')
    params = {'TransacaoSearch[data_cadastro]': f'{start_str} - {end_str}'}

    all_data = []
    page = 1
    
    try:
        while True:
            params['page'] = page
            response = requests.get(base_url, headers=headers, params=params, timeout=30)
            
            if response.status_code != 200:
                st.error(f"Erro na API: {response.status_code} - {response.text}")
                break
                
            data = response.json()
            if isinstance(data, list) and data:
                all_data.extend(data)
                # Lógica simples de parada: se vier menos de 50 registros (tamanho padrão pag), deve ser a última
                if len(data) < 20: 
                    break
                page += 1
            else:
                break
                
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return pd.DataFrame()

    if not all_data:
        return pd.DataFrame()

    # Normalização
    df = pd.json_normalize(all_data)
    
    # Seleção e Renomeação de Colunas
    cols_map = {
        'id': 'ID Transação',
        'data_cadastro': 'Data',
        'valor_total': 'Valor Total',
        'cliente_nome': 'Cliente',
        'cliente_cnpj': 'CNPJ Cliente',
        'nome_fantasia': 'Estabelecimento',
        'tipo_transacao_nome': 'Tipo',
        'status_transacao_nome': 'Status',
        'usuario_nome': 'Usuário/Motorista',
        'placa': 'Placa'
    }
    
    # Garante que as colunas existem antes de renomear
    available_cols = [c for c in cols_map.keys() if c in df.columns]
    df_final = df[available_cols].rename(columns=cols_map)
    
    # Tratamento de dados
    if 'Data' in df_final.columns:
        df_final['Data'] = pd.to_datetime(df_final['Data']).dt.strftime('%d/%m/%Y %H:%M')
    
    if 'Valor Total' in df_final.columns:
        df_final['Valor Total'] = pd.to_numeric(df_final['Valor Total'], errors='coerce')

    return df_final

# ==============================================================================
# LÓGICA DA INTERFACE (UI)
# ==============================================================================

if not api_token:
    st.warning("⚠️ Por favor, insira o Token da API na barra lateral para continuar.")
    st.stop()

# --- BLOCO DE RELATÓRIO: MOTORISTAS ---
if tipo_relatorio == "Motoristas":
    st.subheader("📋 Base de Motoristas")
    st.info("Este relatório lista todos os motoristas cadastrados, com seus grupos, empresas e status.")

    if st.button("🔄 Buscar Dados de Motoristas"):
        df_motoristas = fetch_motoristas_sigyo(api_token)
        
        if not df_motoristas.empty:
            st.session_state['df_motoristas'] = df_motoristas
            st.success(f"{len(df_motoristas)} motoristas encontrados!")
        else:
            st.warning("Nenhum motorista encontrado ou erro na busca.")

    # Exibição e Exportação (se houver dados na sessão)
    if 'df_motoristas' in st.session_state and not st.session_state['df_motoristas'].empty:
        df = st.session_state['df_motoristas']
        
        # Filtros Rápidos
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            filtro_status = st.multiselect("Filtrar por Status:", options=df['Status'].unique(), default=df['Status'].unique())
        with col_f2:
            search_term = st.text_input("Buscar por Nome ou CNH:", "")

        # Aplica Filtros
        df_filtered = df[df['Status'].isin(filtro_status)]
        if search_term:
            df_filtered = df_filtered[
                df_filtered['Nome'].str.contains(search_term, case=False, na=False) | 
                df_filtered['CPF/CNH'].str.contains(search_term, na=False)
            ]

        # Seleção de Colunas
        st.markdown("### Selecionar Colunas para Exportação")
        all_cols = df_filtered.columns.tolist()
        cols_default = ['ID', 'Nome', 'CPF/CNH', 'Status', 'Empresas', 'Grupos Vinculados']
        # Garante que os defaults existem
        cols_default = [c for c in cols_default if c in all_cols]
        
        selected_cols = st.multiselect(
            "Colunas:",
            all_cols,
            default=cols_default
        )

        if not selected_cols:
            st.error("Selecione pelo menos uma coluna.")
        else:
            df_display = df_filtered[selected_cols]
            
            # Mostra Tabela
            st.dataframe(df_display, use_container_width=True)
            
            # Botão de Exportação
            csv = df_display.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📥 Baixar Planilha (CSV)",
                data=csv,
                file_name="relatorio_motoristas_sigyo.csv",
                mime="text/csv",
                type="primary"
            )

# --- BLOCO DE RELATÓRIO: TRANSAÇÕES ---
elif tipo_relatorio == "Transações":
    st.subheader("💲 Relatório de Transações")
    
    # Filtros de Data
    col1, col2 = st.columns(2)
    today = date.today()
    default_start = today - timedelta(days=7)
    
    with col1:
        data_inicial = st.date_input("Data Inicial", default_start, format="DD/MM/YYYY")
    with col2:
        data_final = st.date_input("Data Final", today, format="DD/MM/YYYY")

    if st.button("🔄 Buscar Transações"):
        if data_inicial > data_final:
            st.error("Data inicial não pode ser maior que a final.")
        else:
            df_transacoes = fetch_transacoes_sigyo(api_token, data_inicial, data_final)
            
            if not df_transacoes.empty:
                st.session_state['df_transacoes'] = df_transacoes
                st.success(f"{len(df_transacoes)} transações encontradas!")
            else:
                st.warning("Nenhuma transação encontrada para o período.")

    # Exibição e Exportação
    if 'df_transacoes' in st.session_state and not st.session_state['df_transacoes'].empty:
        df = st.session_state['df_transacoes']
        
        # Seleção de Colunas
        st.markdown("### Selecionar Colunas para Exportação")
        all_cols = df.columns.tolist()
        selected_cols = st.multiselect(
            "Colunas:",
            all_cols,
            default=all_cols
        )
        
        if not selected_cols:
            st.error("Selecione pelo menos uma coluna.")
        else:
            df_display = df[selected_cols]
            
            # KPIs Rápidos
            if 'Valor Total' in df_display.columns:
                total_val = df_display['Valor Total'].sum()
                st.metric("Volume Total no Período", f"R$ {total_val:,.2f}")

            st.dataframe(df_display, use_container_width=True)
            
            csv = df_display.to_csv(index=False, sep=';', decimal=',').encode('utf-8-sig')
            st.download_button(
                label="📥 Baixar Planilha (CSV)",
                data=csv,
                file_name="relatorio_transacoes_sigyo.csv",
                mime="text/csv",
                type="primary"
            )
