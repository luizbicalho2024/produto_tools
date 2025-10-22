import streamlit as st
import pandas as pd
import plotly.express as px
import io

# Configuração da página
st.set_page_config(
    page_title="Dashboard de Vendas Rovema Pay",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Funções de Carregamento e Pré-processamento ---

@st.cache_data
def load_and_preprocess_data(uploaded_transacoes, uploaded_clientes):
    """
    Carrega os dados dos arquivos de upload, realiza o pré-processamento
    e a junção dos DataFrames.
    """
    st.info("Carregando e processando dados...", icon="⏳")

    # 1. Carregar o arquivo de Transações
    # Usamos io.BytesIO para ler o objeto UploadedFile
    try:
        if uploaded_transacoes.name.endswith('.csv'):
            df_transacoes = pd.read_csv(io.StringIO(uploaded_transacoes.getvalue().decode('utf-8')))
        # Adicionar suporte para Excel caso o usuário upe um XLSX (se o nome do arquivo vier como .csv mas for XLSX)
        # O Streamlit trata arquivos XLSX automaticamente se for lido com pd.read_excel, mas aqui estamos com o objeto em memória
        # Como o problema inicial era com CSVs, focamos em CSV.
        else:
             df_transacoes = pd.read_csv(io.StringIO(uploaded_transacoes.getvalue().decode('utf-8')))
    except Exception as e:
        st.error(f"Erro ao carregar o arquivo de Transações: {e}")
        return pd.DataFrame(), pd.DataFrame()

    # 2. Carregar o arquivo de Clientes
    try:
        if uploaded_clientes.name.endswith('.csv'):
            df_clientes = pd.read_csv(io.StringIO(uploaded_clientes.getvalue().decode('utf-8')))
        else:
            df_clientes = pd.read_csv(io.StringIO(uploaded_clientes.getvalue().decode('utf-8')))
    except Exception as e:
        st.error(f"Erro ao carregar o arquivo de Clientes: {e}")
        return pd.DataFrame(), pd.DataFrame()


    # 3. Pré-processamento e Limpeza

    # Tratar colunas de data (assumindo formato YYYY-MM-DD como visto na análise anterior)
    if 'venda' in df_transacoes.columns:
        # Tenta converter para datetime, forçando erros para NaT
        df_transacoes['venda'] = pd.to_datetime(df_transacoes['venda'], errors='coerce')
        df_transacoes.dropna(subset=['venda'], inplace=True) # Remove linhas com data inválida

    # 4. Junção dos DataFrames
    # Renomear colunas para garantir a junção
    df_clientes.columns = [c.strip() for c in df_clientes.columns]
    df_transacoes.columns = [c.strip() for c in df_transacoes.columns]

    # Usar 'cnpj' para a junção
    if 'cnpj' in df_transacoes.columns and 'cnpj' in df_clientes.columns:
        df_merged = pd.merge(
            df_transacoes,
            df_clientes[['cnpj', 'responsavel_comercial']],
            on='cnpj',
            how='left'
        )
        df_merged['responsavel_comercial'] = df_merged['responsavel_comercial'].fillna('Não Atribuído')
    else:
        st.error("Colunas 'cnpj' não encontradas em ambos os arquivos. Verifique se os nomes das colunas estão corretos.")
        return pd.DataFrame(), pd.DataFrame()

    st.success("Dados carregados e processados com sucesso!", icon="✅")
    return df_merged, df_clientes

# --- Interface Streamlit ---

st.title("💰 Dashboard de Análise de Vendas e Performance Comercial")
st.markdown("Métricas estratégicas para acompanhamento de vendas e gestão do time comercial.")

# --- Área de Upload na Barra Lateral ---
with st.sidebar:
    st.header("Upload dos Arquivos")
    st.markdown("Faça o upload dos dois arquivos originais (.csv)")

    uploaded_transacoes = st.file_uploader(
        "1. Upload do Arquivo de Transações",
        type=['csv'],
        key="transacoes"
    )

    uploaded_clientes = st.file_uploader(
        "2. Upload do Arquivo de Clientes",
        type=['csv'],
        key="clientes"
    )

df_merged = pd.DataFrame()
df_clientes = pd.DataFrame()

if uploaded_transacoes and uploaded_clientes:
    # Chama a função de processamento com os arquivos carregados
    df_merged, df_clientes_original = load_and_preprocess_data(uploaded_transacoes, uploaded_clientes)

# Verifica se os dados foram carregados corretamente
if df_merged.empty or 'bruto' not in df_merged.columns:
    st.warning("Aguardando upload dos arquivos e processamento dos dados.", icon="⚠️")
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
        # Contagem de clientes únicos a partir do DataFrame original (df_clientes_original)
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
