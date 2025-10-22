import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import io

# --- Configurações de Aparência ---

st.set_page_config(
    page_title="Dashboard de Transações - Rovema Pay",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constantes para Nomes das Abas (Sheets)
# ATENÇÃO: Verifique e ajuste se os nomes das abas em seu XLSX são diferentes!
TRANSACOES_SHEET_NAME = 'transacoes_rovemapay'
CLIENTES_SHEET_NAME = 'carteira_clientes'


# --- Funções de Carregamento e Pré-processamento ---

@st.cache_data(show_spinner="Carregando e processando dados...")
def load_and_preprocess_data(uploaded_file):
    """
    Carrega o arquivo XLSX, lê as abas, processa, une e calcula a Receita.
    Inclui a coluna 'produto' para o filtro.
    """
    try:
        xls = pd.ExcelFile(uploaded_file)
        
        df_transacoes = pd.read_excel(xls, TRANSACOES_SHEET_NAME)
        df_clientes = pd.read_excel(xls, CLIENTES_SHEET_NAME)
        
    except ValueError as e:
        st.error(f"Erro: Uma das abas (sheets) não foi encontrada no arquivo Excel. Esperado: '{TRANSACOES_SHEET_NAME}' e '{CLIENTES_SHEET_NAME}'.")
        return pd.DataFrame(), pd.DataFrame()
    except Exception as e:
        st.error(f"Erro ao carregar o arquivo Excel: {e}")
        return pd.DataFrame(), pd.DataFrame()

    # Limpeza de colunas e datas
    df_transacoes.columns = [c.strip() for c in df_transacoes.columns]
    df_clientes.columns = [c.strip() for c in df_clientes.columns]

    if 'venda' in df_transacoes.columns:
        df_transacoes['venda'] = pd.to_datetime(df_transacoes['venda'], errors='coerce')
        df_transacoes.dropna(subset=['venda'], inplace=True) 
    else:
        st.error("Coluna 'venda' não encontrada na aba de transações.")
        return pd.DataFrame(), pd.DataFrame()

    # Junção (Merge): AGORA INCLUINDO 'produto' para o filtro
    cols_to_merge = ['cnpj', 'responsavel_comercial', 'produto']
    if all(col in df_transacoes.columns for col in ['cnpj', 'bruto', 'liquido']) and all(col in df_clientes.columns for col in cols_to_merge):
        df_merged = pd.merge(
            df_transacoes,
            df_clientes[cols_to_merge],
            on='cnpj',
            how='left'
        )
        df_merged['responsavel_comercial'] = df_merged['responsavel_comercial'].fillna('Não Atribuído')
        df_merged['produto'] = df_merged['produto'].fillna('Não Especificado')
        df_merged['receita'] = df_merged['bruto'] - df_merged['liquido']
    else:
        st.error("Colunas essenciais (cnpj, responsavel_comercial, produto, bruto, ou liquido) não encontradas.")
        return pd.DataFrame(), pd.DataFrame()

    return df_merged, df_clientes


# --- Interface Streamlit ---

st.title("💰 Rovema Bank - Dashboard de Transações")

# --- Área de Upload e Filtros (Barra Lateral) ---
with st.sidebar:
    st.header("Filtros")
    
    # Upload
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


# --- Dashboard Principal ---

if df_merged.empty or 'bruto' not in df_merged.columns:
    st.warning("Por favor, faça o upload do arquivo Excel para iniciar a análise.", icon="⚠️")
else:
    # --- FILTROS COMPLETOS (Barra Lateral) ---
    with st.sidebar:
        st.markdown("---")
        
        # 1. Filtro de Data
        data_min = df_merged['venda'].min().date()
        data_max = df_merged['venda'].max().date()
        data_inicial, data_final = st.date_input(
            "Período de Análise",
            value=(data_min, data_max),
            min_value=data_min,
            max_value=data_max
        )

        # 2. Filtro de Vendedor/Carteira
        vendedores = ['Todas'] + sorted(df_clientes_original['responsavel_comercial'].unique().tolist())
        filtro_vendedor = st.selectbox("Filtrar por Vendedor (Carteira)", options=vendedores)

        # 3. Filtro de Produto
        produtos = ['Todos'] + sorted(df_merged['produto'].unique().tolist())
        filtro_produto = st.selectbox("Filtrar por Produto", options=produtos)
        
        # 4. Filtro de Bandeira
        bandeiras = ['Todas'] + sorted(df_merged['bandeira'].unique().tolist())
        filtro_bandeira = st.selectbox("Filtrar por Bandeira", options=bandeiras)
        
        # 5. Filtro de Tipo (Crédito/Débito)
        tipos = ['Todos'] + sorted(df_merged['tipo'].unique().tolist())
        filtro_tipo = st.selectbox("Filtrar por Tipo", options=tipos)

        # --- Aplicação dos Filtros ---
        df_filtered = df_merged[
            (df_merged['venda'].dt.date >= data_inicial) &
            (df_merged['venda'].dt.date <= data_final)
        ].copy()
        
        if filtro_vendedor != 'Todas':
             df_filtered = df_filtered[df_filtered['responsavel_comercial'] == filtro_vendedor]
        
        if filtro_produto != 'Todos':
             df_filtered = df_filtered[df_filtered['produto'] == filtro_produto]
             
        if filtro_bandeira != 'Todas':
             df_filtered = df_filtered[df_filtered['bandeira'] == filtro_bandeira]

        if filtro_tipo != 'Todos':
             df_filtered = df_filtered[df_filtered['tipo'] == filtro_tipo]
             
        st.button("Atualizar") # Botão no anexo é só visual, pois o Streamlit atualiza automaticamente


    # --- 1. Cálculos de KPIs ---
    
    total_gmv = df_filtered['bruto'].sum()
    receita_total = df_filtered['receita'].sum()
    clientes_ativos = df_filtered['cnpj'].nunique()
    
    margem_media = (receita_total / total_gmv) * 100 if total_gmv > 0 else 0
    
    # Proxy para "Clientes em Queda" (Clientes com GMV baixo, e.g., < R$1000)
    gmv_por_cliente = df_filtered.groupby('cnpj')['bruto'].sum()
    clientes_em_queda_proxy = gmv_por_cliente[gmv_por_cliente < 1000].count()
    
    # --- 2. Linha de KPIs (Cards) ---
    st.markdown("""
        <style>
            .st-emotion-cache-1r6r0z6 {
                color: #262730; /* Cor do rótulo da métrica */
            }
            .st-emotion-cache-1s0k10z {
                background-color: #f0f2f6; /* Fundo dos cards (simulando cinza claro) */
                border-radius: 8px;
                padding: 10px;
                box-shadow: 1px 1px 5px rgba(0,0,0,0.05);
            }
        </style>
    """, unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric("Transacionado (Bruto)", f"R$ {total_gmv:,.2f}")
    col2.metric("Nossa Receita", f"R$ {receita_total:,.2f}")
    col3.metric("Margem Média", f"{margem_media:,.2f}%")
    col4.metric("Clientes Ativos", f"{clientes_ativos:,}")
    col5.metric("Clientes em Queda (Proxy)", f"{clientes_em_queda_proxy:,}")
    
    st.markdown("---")

    # --- 3. Gráfico de Evolução (GMV vs Receita) ---
    st.subheader("Evolução do Valor Transacionado vs Receita")

    # Agrupamento por dia
    df_evolucao = df_filtered.groupby(df_filtered['venda'].dt.date).agg(
        GMV=('bruto', 'sum'),
        Receita=('receita', 'sum')
    ).reset_index()
    df_evolucao.columns = ['Data da Venda', 'GMV', 'Receita']
    
    fig_evolucao = go.Figure()
    
    fig_evolucao.add_trace(go.Scatter(
        x=df_evolucao['Data da Venda'], y=df_evolucao['GMV'],
        mode='lines', name='Valor Transacionado (Bruto)',
        line=dict(color='blue', width=2)
    ))
    
    fig_evolucao.add_trace(go.Scatter(
        x=df_evolucao['Data da Venda'], y=df_evolucao['Receita'],
        mode='lines', name='Nossa Receita',
        line=dict(color='red', width=2)
    ))
    
    fig_evolucao.update_layout(
        xaxis_title='Data',
        yaxis_title='Valor (R$)',
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5)
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
            color_discrete_sequence=px.colors.qualitative.Plotly,
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
            color_discrete_sequence=px.colors.qualitative.Safe
        )
        fig_bandeira.update_traces(textinfo='percent+label', pull=[0.05, 0.0, 0.0])
        st.plotly_chart(fig_bandeira, use_container_width=True)
        
    st.markdown("---")

    # --- 5. Tabelas de Detalhamento por Cliente (Top 10 Crescimento/Queda - Proxy) ---
    st.subheader("Detalhamento por Cliente")
    
    col8, col9 = st.columns(2)
    
    # DataFrame de Ranqueamento de Clientes
    df_ranking_clientes = df_filtered.groupby(['cnpj', 'ec']).agg(
        GMV=('bruto', 'sum'),
        Receita=('receita', 'sum'),
        Vendas=('id_venda', 'nunique')
    ).reset_index().sort_values(by='GMV', ascending=False)
    
    # TOP 10 (Proxy para Top 10 Crescimento)
    with col8:
        st.markdown("##### Top 10 Crescimento (Proxy: Maior GMV)")
        df_top10 = df_ranking_clientes.head(10).copy()
        df_top10['GMV'] = df_top10['GMV'].apply(lambda x: f"R$ {x:,.2f}")
        df_top10['Receita'] = df_top10['Receita'].apply(lambda x: f"R$ {x:,.2f}")
        df_top10.rename(columns={'ec': 'Cliente', 'GMV': 'Transacionado', 'Vendas': 'Qtd. Vendas'}, inplace=True)
        
        st.dataframe(df_top10[['Cliente', 'Transacionado', 'Qtd. Vendas', 'Receita']], hide_index=True, use_container_width=True)

    # BOTTOM 10 (Proxy para Top 10 Queda)
    with col9:
        st.markdown("##### Top 10 Queda (Proxy: Menor GMV)")
        df_bottom10 = df_ranking_clientes.sort_values(by='GMV', ascending=True).head(10).copy()
        df_bottom10['GMV'] = df_bottom10['GMV'].apply(lambda x: f"R$ {x:,.2f}")
        df_bottom10['Receita'] = df_bottom10['Receita'].apply(lambda x: f"R$ {x:,.2f}")
        df_bottom10.rename(columns={'ec': 'Cliente', 'GMV': 'Transacionado', 'Vendas': 'Qtd. Vendas'}, inplace=True)

        st.dataframe(df_bottom10[['Cliente', 'Transacionado', 'Qtd. Vendas', 'Receita']], hide_index=True, use_container_width=True)

    # --- Tabela Detalhada (Rodapé) ---
    with st.expander("Visualizar Todos os Dados (Detalhados)"):
        st.dataframe(df_filtered)
