import streamlit as st
import pandas as pd
import plotly.express as px

# Configuração da página
st.set_page_config(
    page_title="Dashboard de Vendas Rovema Pay",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data
def load_data(file_path):
    """Carrega e pré-processa os dados."""
    try:
        df = pd.read_csv(file_path)
        # Conversão de data para garantir o tipo correto para gráficos de séries temporais
        if 'venda' in df.columns:
            df['venda'] = pd.to_datetime(df['venda'])
        return df
    except FileNotFoundError:
        st.error(f"Arquivo não encontrado: {file_path}. Certifique-se de que o arquivo está na mesma pasta.")
        return pd.DataFrame()

# --- Carregar Dados ---
DATA_FILE = "df_transacoes_com_vendedor.csv"
df = load_data(DATA_FILE)

if not df.empty:
    st.title("💰 Dashboard de Análise de Vendas e Performance Comercial")
    st.markdown("Métricas estratégicas para acompanhamento de vendas e gestão do time comercial.")

    # --- 1. Métricas Chave (KPIs) ---
    st.header("1. Performance Geral de Vendas")

    # Filtro de Data (Sidebar)
    with st.sidebar:
        st.header("Filtros")
        if 'venda' in df.columns:
            data_min = df['venda'].min().date()
            data_max = df['venda'].max().date()
            data_inicial, data_final = st.date_input(
                "Período de Análise",
                value=(data_min, data_max),
                min_value=data_min,
                max_value=data_max
            )
            # Converte as datas de volta para datetime para o filtro
            df_filtered = df[
                (df['venda'].dt.date >= data_inicial) &
                (df['venda'].dt.date <= data_final)
            ]
        else:
            st.warning("Coluna 'venda' não encontrada para filtro de data.")
            df_filtered = df.copy()

    # Cálculo dos KPIs
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
    col5, col6 = st.columns(2)

    with col5:
        st.subheader("Evolução Diária do GMV (Valor Bruto)")
        # Agrupamento para o gráfico de linha
        df_evolucao = df_filtered.groupby(df_filtered['venda'].dt.date)['bruto'].sum().reset_index()
        df_evolucao.columns = ['Data da Venda', 'GMV']

        fig_evolucao = px.line(
            df_evolucao,
            x='Data da Venda',
            y='GMV',
            title='GMV Diário ao Longo do Tempo',
            labels={'GMV': 'GMV (R$)', 'Data da Venda': 'Data'}
        )
        fig_evolucao.update_traces(mode='lines+markers')
        fig_evolucao.update_layout(xaxis_title='Data', yaxis_title='GMV (R$)', hovermode="x unified")
        st.plotly_chart(fig_evolucao, use_container_width=True)

    with col6:
        st.subheader("Distribuição de Vendas por Bandeira")
        # Agrupamento para o gráfico de barras por bandeira
        df_bandeira = df_filtered.groupby('bandeira')['bruto'].sum().reset_index()
        df_bandeira = df_bandeira.sort_values(by='bruto', ascending=False)
        fig_bandeira = px.bar(
            df_bandeira,
            x='bandeira',
            y='bruto',
            title='GMV por Bandeira',
            labels={'bandeira': 'Bandeira', 'bruto': 'GMV (R$)'}
        )
        fig_bandeira.update_layout(xaxis_title='Bandeira', yaxis_title='GMV (R$)')
        st.plotly_chart(fig_bandeira, use_container_width=True)

    st.markdown("---")

    # --- 3. Acompanhamento da Atividade do Time Comercial ---
    st.header("3. Performance e Atividade do Time Comercial")

    col7, col8 = st.columns(2)

    with col7:
        st.subheader("Ranking de Vendedores por GMV")
        # Agrupamento para ranking de vendedores por GMV
        df_ranking_gmv = df_filtered.groupby('responsavel_comercial')['bruto'].sum().reset_index()
        df_ranking_gmv = df_ranking_gmv.sort_values(by='bruto', ascending=False)

        fig_ranking_gmv = px.bar(
            df_ranking_gmv,
            x='responsavel_comercial',
            y='bruto',
            title='GMV por Responsável Comercial',
            labels={'responsavel_comercial': 'Vendedor', 'bruto': 'GMV (R$)'},
            color='responsavel_comercial'
        )
        fig_ranking_gmv.update_layout(xaxis_title='Vendedor', yaxis_title='GMV (R$)')
        st.plotly_chart(fig_ranking_gmv, use_container_width=True)


    with col8:
        st.subheader("Total de Clientes por Vendedor")
        # Requer o df original de clientes para contagem única.
        # (Idealmente, isso viria de outro arquivo/base, mas usaremos a agregação de CNPJs únicos do df_merged para simplificar o script.)
        df_clientes_vendedor = df.groupby('responsavel_comercial')['cnpj'].nunique().reset_index()
        df_clientes_vendedor.columns = ['Vendedor', 'Total de Clientes']
        df_clientes_vendedor = df_clientes_vendedor.sort_values(by='Total de Clientes', ascending=False)

        fig_clientes_vendedor = px.bar(
            df_clientes_vendedor,
            x='Vendedor',
            y='Total de Clientes',
            title='Contagem de Clientes Únicos por Vendedor',
            labels={'Vendedor': 'Vendedor', 'Total de Clientes': 'Qtd. Clientes'},
            color='Vendedor'
        )
        fig_clientes_vendedor.update_layout(xaxis_title='Vendedor', yaxis_title='Qtd. Clientes')
        st.plotly_chart(fig_clientes_vendedor, use_container_width=True)

    # --- Tabela de Dados (Expansível) ---
    with st.expander("Visualizar Dados Detalhados"):
        st.dataframe(df_filtered)
