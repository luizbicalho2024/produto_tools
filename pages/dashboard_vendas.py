import streamlit as st
import pandas as pd
import plotly.express as px
import io

# --- Configurações e Nomes das Abas ---
st.set_page_config(
    page_title="Dashboard de Vendas Rovema Pay",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ATENÇÃO: Se os nomes das suas abas (sheets) forem diferentes, ajuste aqui!
TRANSACOES_SHEET_NAME = 'transacoes_rovemapay'
CLIENTES_SHEET_NAME = 'carteira_clientes'

# --- Funções de Carregamento e Pré-processamento ---

@st.cache_data
def load_and_preprocess_data(uploaded_file):
    """
    Carrega os dados do arquivo XLSX, lê as abas e realiza o pré-processamento.
    """
    st.info(f"Carregando e processando dados das abas: {TRANSACOES_SHEET_NAME} e {CLIENTES_SHEET_NAME}...", icon="⏳")

    # 1. Carregar o arquivo XLSX
    try:
        # Lê o arquivo Excel, lendo todas as abas de uma vez
        xls = pd.ExcelFile(uploaded_file)
        
        # Tenta ler as abas específicas
        df_transacoes = pd.read_excel(xls, TRANSACOES_SHEET_NAME)
        df_clientes = pd.read_excel(xls, CLIENTES_SHEET_NAME)
        
    except ValueError as e:
        # Captura erro se a aba não for encontrada
        st.error(f"Erro: Uma das abas (sheets) não foi encontrada no arquivo Excel. Verifique se os nomes estão corretos: '{TRANSACOES_SHEET_NAME}' e '{CLIENTES_SHEET_NAME}'.")
        st.error(f"Detalhes do erro: {e}")
        return pd.DataFrame(), pd.DataFrame()
    except Exception as e:
        st.error(f"Erro ao carregar o arquivo Excel: {e}")
        return pd.DataFrame(), pd.DataFrame()


    # 2. Pré-processamento e Limpeza
    # Limpa espaços em branco nos nomes das colunas de ambos os DFs
    df_transacoes.columns = [c.strip() for c in df_transacoes.columns]
    df_clientes.columns = [c.strip() for c in df_clientes.columns]

    # Tratar colunas de data (assumindo formato YYYY-MM-DD)
    if 'venda' in df_transacoes.columns:
        # Converte para datetime
        df_transacoes['venda'] = pd.to_datetime(df_transacoes['venda'], errors='coerce')
        df_transacoes.dropna(subset=['venda'], inplace=True) # Remove linhas com data inválida

    # 3. Junção dos DataFrames
    # Usa 'cnpj' para a junção
    if 'cnpj' in df_transacoes.columns and 'responsavel_comercial' in df_clientes.columns:
        df_merged = pd.merge(
            df_transacoes,
            df_clientes[['cnpj', 'responsavel_comercial']],
            on='cnpj',
            how='left'
        )
        df_merged['responsavel_comercial'] = df_merged['responsavel_comercial'].fillna('Não Atribuído')
    else:
        st.error("Colunas essenciais ('cnpj' ou 'responsavel_comercial') não encontradas após o carregamento. Verifique a formatação das abas.")
        return pd.DataFrame(), pd.DataFrame()

    st.success("Dados carregados e processados com sucesso!", icon="✅")
    return df_merged, df_clientes

# --- Interface Streamlit ---

st.title("💰 Dashboard de Análise de Vendas e Performance Comercial")
st.markdown("Métricas estratégicas para acompanhamento de vendas e gestão do time comercial.")

# --- Área de Upload na Barra Lateral ---
with st.sidebar:
    st.header("Upload do Arquivo de Dados")
    st.markdown("Faça o upload do arquivo Excel (.xlsx) contendo as abas de Transações e Clientes.")

    uploaded_file = st.file_uploader(
        "1. Upload do Arquivo Único (Excel)",
        type=['xlsx'],
        key="excel_upload"
    )
    
    # Exibe os nomes das abas esperadas para referência
    st.markdown(f"**Abas Esperadas:**")
    st.markdown(f"- Transações: `{TRANSACOES_SHEET_NAME}`")
    st.markdown(f"- Clientes: `{CLIENTES_SHEET_NAME}`")


df_merged = pd.DataFrame()
df_clientes_original = pd.DataFrame()

if uploaded_file:
    # Chama a função de processamento com o arquivo carregado
    df_merged, df_clientes_original = load_and_preprocess_data(uploaded_file)

# Verifica se os dados foram carregados corretamente
if df_merged.empty or 'bruto' not in df_merged.columns:
    st.warning("Aguardando upload do arquivo Excel e processamento dos dados.", icon="⚠️")
else:
    # --- Filtros de Data (Sidebar) ---
    with st.sidebar:
        st.subheader("Filtros de Período")
        data_min = df_merged['venda'].min().date()
        data_max = df_merged['venda'].max().date()

        data_inicial, data_final = st.date_input(
            "Selecione o Período de Análise",
            value=(data_min, data_max),
            min_value=data_min,
            max_value=data_max
        )

        # Filtra o DataFrame
        df_filtered = df_merged[
            (df_merged['venda'].dt.date >= data_inicial) &
            (df_merged['venda'].dt.date <= data_final)
        ]

    # --- 1. Métricas Chave (KPIs) ---
    st.header("1. Performance Geral de Vendas")

    # Recálculo dos KPIs com dados filtrados
    total_gmv = df_filtered['bruto'].sum()
    total_liquido = df_filtered['liquido'].sum()
    ticket_medio_bruto = df_filtered['bruto'].mean()
    volume_transacoes = df_filtered['id_venda'].nunique()

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("GMV Total Bruto", f"R$ {total_gmv:,.2f}")
    col2.metric("Líquido Total Recebido", f"R$ {total_liquido:,.2f}")
    col3.metric("Ticket Médio Bruto", f"R$ {ticket_medio_bruto:,.2f}")
    col4.metric("Volume de Transações", f"{volume_transacoes:,}")

    st.markdown("---")

    # --- 2. Acompanhamento de Vendas (Gráficos) ---
    st.header("2. Acompanhamento de Vendas (Gráficos Analíticos)")
    col5, col6 = st.columns(2)

    with col5:
        st.subheader("Evolução Diária do GMV (Valor Bruto)")
        df_evolucao = df_filtered.groupby(df_filtered['venda'].dt.date)['bruto'].sum().reset_index()
        df_evolucao.columns = ['Data da Venda', 'GMV']

        fig_evolucao = px.line(
            df_evolucao, x='Data da Venda', y='GMV',
            title='GMV Diário ao Longo do Tempo',
            labels={'GMV': 'GMV (R$)', 'Data da Venda': 'Data'}
        )
        fig_evolucao.update_layout(hovermode="x unified")
        st.plotly_chart(fig_evolucao, use_container_width=True)

    with col6:
        st.subheader("Distribuição de Vendas por Bandeira")
        df_bandeira = df_filtered.groupby('bandeira')['bruto'].sum().reset_index()
        df_bandeira = df_bandeira.sort_values(by='bruto', ascending=False)
        fig_bandeira = px.bar(
            df_bandeira, x='bandeira', y='bruto',
            title='GMV por Bandeira',
            labels={'bandeira': 'Bandeira', 'bruto': 'GMV (R$)'},
            color='bandeira'
        )
        st.plotly_chart(fig_bandeira, use_container_width=True)

    col9, col10 = st.columns(2)

    with col9:
        st.subheader("Distribuição de Vendas por Tipo")
        df_tipo = df_filtered.groupby('tipo')['bruto'].sum().reset_index()
        fig_tipo = px.pie(
            df_tipo, values='bruto', names='tipo',
            title='GMV por Tipo (Crédito vs. Débito)',
        )
        fig_tipo.update_traces(textinfo='percent+label', pull=[0.05, 0])
        st.plotly_chart(fig_tipo, use_container_width=True)


    # --- 3. Acompanhamento da Atividade do Time Comercial ---
    st.header("3. Performance e Atividade do Time Comercial")

    col7, col8 = st.columns(2)

    with col7:
        st.subheader("Ranking de Vendedores por GMV")
        df_ranking_gmv = df_filtered.groupby('responsavel_comercial')['bruto'].sum().reset_index()
        df_ranking_gmv = df_ranking_gmv.sort_values(by='bruto', ascending=False)

        fig_ranking_gmv = px.bar(
            df_ranking_gmv, x='responsavel_comercial', y='bruto',
            title='GMV por Responsável Comercial',
            labels={'responsavel_comercial': 'Vendedor', 'bruto': 'GMV (R$)'},
            color='responsavel_comercial'
        )
        st.plotly_chart(fig_ranking_gmv, use_container_width=True)


    with col8:
        st.subheader("Total de Clientes por Vendedor")
        # Contagem de clientes únicos a partir do DataFrame original
        df_clientes_vendedor = df_clientes_original.groupby('responsavel_comercial')['cnpj'].nunique().reset_index()
        df_clientes_vendedor.columns = ['Vendedor', 'Total de Clientes']
        df_clientes_vendedor = df_clientes_vendedor.sort_values(by='Total de Clientes', ascending=False)

        fig_clientes_vendedor = px.bar(
            df_clientes_vendedor, x='Vendedor', y='Total de Clientes',
            title='Contagem de Clientes Únicos por Vendedor',
            labels={'Vendedor': 'Vendedor', 'Total de Clientes': 'Qtd. Clientes'},
            color='Vendedor'
        )
        st.plotly_chart(fig_clientes_vendedor, use_container_width=True)

    st.markdown("---")

    # --- Tabela de Dados (Expansível) ---
    with st.expander("Visualizar Dados Detalhados (Filtrados)"):
        st.dataframe(df_filtered)
