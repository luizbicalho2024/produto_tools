import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import io

# --- Configurações e Nomes das Abas ---

st.set_page_config(
    page_title="Dashboard de Transações - Rovema Pay",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ATENÇÃO: Ajuste os nomes das abas se forem diferentes no seu arquivo XLSX!
TRANSACOES_SHEET_NAME = 'transacoes_rovemapay'
CLIENTES_SHEET_NAME = 'carteira_clientes'

# --- Funções de Carregamento e Pré-processamento ---

@st.cache_data(show_spinner="Carregando e processando dados...")
def load_and_preprocess_data(uploaded_file):
    """
    Carrega o arquivo XLSX, lê as abas, processa e une os DataFrames.
    """
    try:
        xls = pd.ExcelFile(uploaded_file)
        
        # Leitura das abas
        df_transacoes = pd.read_excel(xls, TRANSACOES_SHEET_NAME)
        df_clientes = pd.read_excel(xls, CLIENTES_SHEET_NAME)
        
    except ValueError as e:
        # Erro se a aba não for encontrada
        st.error(f"Erro: Uma das abas (sheets) não foi encontrada no arquivo Excel. Esperado: '{TRANSACOES_SHEET_NAME}' e '{CLIENTES_SHEET_NAME}'.")
        return pd.DataFrame(), pd.DataFrame()
    except Exception as e:
        # Outros erros de carregamento
        st.error(f"Erro ao carregar o arquivo Excel: {e}")
        return pd.DataFrame(), pd.DataFrame()

    # Pré-processamento e Limpeza
    # Limpa espaços em branco nos nomes das colunas de ambos os DFs
    df_transacoes.columns = [c.strip() for c in df_transacoes.columns]
    df_clientes.columns = [c.strip() for c in df_clientes.columns]

    # Tratar colunas de data 
    if 'venda' in df_transacoes.columns:
        df_transacoes['venda'] = pd.to_datetime(df_transacoes['venda'], errors='coerce')
        df_transacoes.dropna(subset=['venda'], inplace=True) 
    else:
        st.error("Coluna 'venda' não encontrada na aba de transações.")
        return pd.DataFrame(), pd.DataFrame()

    # Junção dos DataFrames
    if 'cnpj' in df_transacoes.columns and 'responsavel_comercial' in df_clientes.columns:
        df_merged = pd.merge(
            df_transacoes,
            df_clientes[['cnpj', 'responsavel_comercial']],
            on='cnpj',
            how='left'
        )
        df_merged['responsavel_comercial'] = df_merged['responsavel_comercial'].fillna('Não Atribuído')
    else:
        st.error("Colunas essenciais ('cnpj' ou 'responsavel_comercial') não encontradas. Verifique a formatação.")
        return pd.DataFrame(), pd.DataFrame()

    # Cálculos essenciais para KPIs e Receita
    if 'bruto' in df_merged.columns and 'liquido' in df_merged.columns:
        df_merged['receita'] = df_merged['bruto'] - df_merged['liquido']
    else:
        st.error("Colunas 'bruto' e 'liquido' essenciais não encontradas para cálculos de Receita.")
        return pd.DataFrame(), pd.DataFrame()

    st.success("Dados carregados e processados com sucesso!", icon="✅")
    return df_merged, df_clientes

# --- Interface Streamlit ---

st.title("💰 Dashboard de Transações")

# --- Área de Upload na Barra Lateral ---
with st.sidebar:
    st.header("Filtros")
    
    uploaded_file = st.file_uploader(
        "Upload do Arquivo Único (Excel)",
        type=['xlsx'],
        key="excel_upload"
    )
    
    st.markdown(f"**Abas Esperadas:** ` {TRANSACOES_SHEET_NAME} ` e ` {CLIENTES_SHEET_NAME} `")


df_merged = pd.DataFrame()
df_clientes_original = pd.DataFrame()

if uploaded_file:
    df_merged, df_clientes_original = load_and_preprocess_data(uploaded_file)


# --- Seção Principal do Dashboard ---

if df_merged.empty or 'bruto' not in df_merged.columns:
    st.warning("Por favor, faça o upload do arquivo Excel para começar a análise.", icon="⚠️")
else:
    # --- Filtros de Data (Barra Lateral) ---
    with st.sidebar:
        st.markdown("---")
        data_min = df_merged['venda'].min().date()
        data_max = df_merged['venda'].max().date()

        data_inicial, data_final = st.date_input(
            "Selecione o Período de Análise",
            value=(data_min, data_max),
            min_value=data_min,
            max_value=data_max
        )

        df_filtered = df_merged[
            (df_merged['venda'].dt.date >= data_inicial) &
            (df_merged['venda'].dt.date <= data_final)
        ]
        
        # Placeholder para o filtro de Carteira, como no anexo
        st.selectbox("Filtrar por Vendedor (Carteira)", 
                     options=['Todos'] + list(df_clientes_original['responsavel_comercial'].unique()),
                     key="filtro_vendedor")
        st.button("Atualizar") # Botão no anexo é só visual, pois o Streamlit atualiza automaticamente

    # --- 1. Cálculos de KPIs ---
    
    total_gmv = df_filtered['bruto'].sum()
    total_liquido = df_filtered['liquido'].sum()
    receita_total = df_filtered['receita'].sum()
    clientes_ativos = df_filtered['cnpj'].nunique()
    
    margem_media = (receita_total / total_gmv) * 100 if total_gmv > 0 else 0
    
    # Clientes em Queda (Placeholder: Usamos clientes com 1 transação como proxy para "baixo engajamento")
    transacoes_por_cliente = df_filtered.groupby('cnpj')['id_venda'].nunique()
    clientes_em_queda = transacoes_por_cliente[transacoes_por_cliente == 1].count()


    # --- 2. Linha de KPIs ---
    
    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric("Transacionado (Bruto)", f"R$ {total_gmv:,.2f}")
    col2.metric("Nossa Receita", f"R$ {receita_total:,.2f}")
    col3.metric("Margem Média", f"{margem_media:,.2f}%")
    col4.metric("Clientes Ativos", f"{clientes_ativos:,}")
    col5.metric("Clientes em Queda (Proxy)", f"{clientes_em_queda:,}")
    
    st.markdown("---")

    # --- 3. Gráfico de Evolução (GMV vs Receita) ---
    st.subheader("Evolução do Valor Transacionado vs Receita")

    # Agrupamento para o gráfico de linha
    df_evolucao = df_filtered.groupby(df_filtered['venda'].dt.date).agg(
        GMV=('bruto', 'sum'),
        Receita=('receita', 'sum')
    ).reset_index()
    df_evolucao.columns = ['Data da Venda', 'GMV', 'Receita']
    
    fig_evolucao = go.Figure()
    
    # Linha do GMV (Valor Transacionado)
    fig_evolucao.add_trace(go.Scatter(
        x=df_evolucao['Data da Venda'], y=df_evolucao['GMV'],
        mode='lines+markers', name='Valor Transacionado (Bruto)',
        line=dict(color='blue')
    ))
    
    # Linha da Receita
    fig_evolucao.add_trace(go.Scatter(
        x=df_evolucao['Data da Venda'], y=df_evolucao['Receita'],
        mode='lines+markers', name='Receita',
        line=dict(color='red')
    ))
    
    fig_evolucao.update_layout(
        xaxis_title='Data',
        yaxis_title='Valor (R$)',
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig_evolucao, use_container_width=True)

    st.markdown("---")

    # --- 4. Linha de Gráficos (Receita por Carteira e Participação por Bandeira) ---
    col6, col7 = st.columns([1.5, 1])

    with col6:
        st.subheader("Receita por Carteira")
        
        # Agrupamento para Receita por Vendedor
        df_receita_vendedor = df_filtered.groupby('responsavel_comercial')['receita'].sum().reset_index()
        df_receita_vendedor = df_receita_vendedor.sort_values(by='receita', ascending=False)

        fig_receita_vendedor = px.bar(
            df_receita_vendedor, 
            x='responsavel_comercial', y='receita',
            labels={'responsavel_comercial': 'Vendedor', 'receita': 'Receita (R$)'},
            color='responsavel_comercial',
            title='Receita Gerada por Responsável Comercial'
        )
        st.plotly_chart(fig_receita_vendedor, use_container_width=True)

    with col7:
        st.subheader("Participação por Bandeira")
        
        # Agrupamento para Participação por Bandeira
        df_bandeira = df_filtered.groupby('bandeira')['bruto'].sum().reset_index()
        
        fig_bandeira = px.pie(
            df_bandeira, values='bruto', names='bandeira',
            title='Participação do GMV por Bandeira',
        )
        fig_bandeira.update_traces(textinfo='percent+label', pull=[0.05, 0.0, 0.0])
        st.plotly_chart(fig_bandeira, use_container_width=True)
        
    st.markdown("---")

    # --- 5. Top 10 Clientes (Substituindo Crescimento/Queda) ---
    st.subheader("Detalhamento por Cliente (GMV)")
    
    col8, col9 = st.columns(2)
    
    # Ranking de Clientes (GMV)
    df_ranking_clientes = df_filtered.groupby(['cnpj', 'ec']).agg(
        Receita=('receita', 'sum'),
        GMV=('bruto', 'sum'),
        Vendas=('id_venda', 'nunique')
    ).reset_index().sort_values(by='GMV', ascending=False)
    
    # TOP 10 (Proxy para Top 10 Crescimento)
    with col8:
        st.markdown("##### Top 10 Clientes por GMV")
        df_top10 = df_ranking_clientes.head(10).copy()
        df_top10.rename(columns={'ec': 'Cliente', 'GMV': 'Valor Transacionado (R$)', 'Receita': 'Receita (R$)'}, inplace=True)
        df_top10['Valor Transacionado (R$)'] = df_top10['Valor Transacionado (R$)'].apply(lambda x: f"R$ {x:,.2f}")
        df_top10['Receita (R$)'] = df_top10['Receita (R$)'].apply(lambda x: f"R$ {x:,.2f}")
        
        st.dataframe(df_top10[['Cliente', 'Valor Transacionado (R$)', 'Vendas', 'Receita (R$)']], hide_index=True, use_container_width=True)

    # BOTTOM 10 (Proxy para Top 10 Queda)
    with col9:
        st.markdown("##### Top 10 Clientes com Menor GMV")
        df_bottom10 = df_ranking_clientes.sort_values(by='GMV', ascending=True).head(10).copy()
        df_bottom10.rename(columns={'ec': 'Cliente', 'GMV': 'Valor Transacionado (R$)', 'Receita': 'Receita (R$)'}, inplace=True)
        df_bottom10['Valor Transacionado (R$)'] = df_bottom10['Valor Transacionado (R$)'].apply(lambda x: f"R$ {x:,.2f}")
        df_bottom10['Receita (R$)'] = df_bottom10['Receita (R$)'].apply(lambda x: f"R$ {x:,.2f}")
        
        st.dataframe(df_bottom10[['Cliente', 'Valor Transacionado (R$)', 'Vendas', 'Receita (R$)']], hide_index=True, use_container_width=True)

    # --- Tabela Detalhada (Rodapé) ---
    with st.expander("Visualizar Todos os Dados Detalhados"):
        st.dataframe(df_filtered)
