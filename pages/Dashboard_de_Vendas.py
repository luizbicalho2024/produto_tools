import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import io

# --- Configurações de Aparência ---

st.set_page_config(
    page_title="Dashboard de Transações Consolidadas - Rovema Bank",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constantes para Nomes das Abas (Sheets)
CLIENTES_SHEET_NAME = 'carteira_clientes'
TRANSACOES_SHEET_NAMES = [
    'transacoes_rovemapay', 
    'transacoes_eliq', 
    'transacoes_asto', 
    'transacoes_bionio'
]


# --- Funções de Processamento e Insights ---

@st.cache_data(show_spinner="Carregando e processando DADOS CONSOLIDADOS de todas as abas...")
def load_and_preprocess_data(uploaded_file):
    """
    Carrega o arquivo XLSX, lê TODAS as abas, normaliza e une os dados.
    """
    try:
        xls = pd.ExcelFile(uploaded_file)
        
        # 1. Carregar Carteira de Clientes (Base para Responsável e Produto)
        df_clientes = pd.read_excel(xls, CLIENTES_SHEET_NAME)
        df_clientes.columns = [c.strip() for c in df_clientes.columns]
        df_clientes = df_clientes[['cnpj', 'responsavel_comercial', 'produto', 'cliente_ec']].copy()

        all_transactions = []
        
        # 2. Iterar e Normalizar Transações
        for sheet_name in TRANSACOES_SHEET_NAMES:
            try:
                df = pd.read_excel(xls, sheet_name)
                df.columns = [c.strip() for c in df.columns]
                
                # DataFrame de destino normalizado
                df_norm = pd.DataFrame()
                
                if sheet_name == 'transacoes_rovemapay':
                    df_norm['cnpj'] = df['cnpj']
                    df_norm['bruto'] = df['bruto']
                    df_norm['receita'] = df['bruto'] - df['liquido']
                    df_norm['venda'] = pd.to_datetime(df['venda'], errors='coerce')
                    df_norm['ec'] = df['ec']
                    df_norm['plataforma'] = 'Rovema Pay'
                    df_norm['tipo'] = df['tipo'].astype(str)
                    df_norm['bandeira'] = df['bandeira'].astype(str)
                
                elif sheet_name == 'transacoes_eliq':
                    df_norm['cnpj'] = df['cliente_cnpj']
                    df_norm['bruto'] = df['valor_bruto']
                    # Receita Proxy: valor a receber (margem Eliq)
                    df_norm['receita'] = df['valor_receber']
                    df_norm['venda'] = pd.to_datetime(df['data'], errors='coerce')
                    df_norm['ec'] = df['cliente_nome']
                    df_norm['plataforma'] = 'Eliq'
                    df_norm['tipo'] = df['grupo'].astype(str) # Usando grupo como tipo para Eliq
                    df_norm['bandeira'] = df['subgrupo'].astype(str) # Usando subgrupo como bandeira
                    
                elif sheet_name == 'transacoes_asto':
                    df_norm['cnpj'] = df['cliente_cnpj']
                    df_norm['bruto'] = df['valor']
                    # Receita Proxy: margem baseada na taxa do cliente (5% sobre o valor)
                    df_norm['receita'] = df['valor'] * (df['taxa_cliente'] / 100)
                    df_norm['venda'] = pd.to_datetime(df['data'], errors='coerce')
                    df_norm['ec'] = df['cliente']
                    df_norm['plataforma'] = 'Asto'
                    df_norm['tipo'] = df['unidade'].astype(str) # Usando unidade como tipo para Asto
                    df_norm['bandeira'] = df['estabelecimento'].astype(str) # Usando estabelecimento como bandeira
                    
                elif sheet_name == 'transacoes_bionio':
                    df_norm['cnpj'] = df['cnpj_da_organizacao']
                    df_norm['bruto'] = df['valor_total_do_pedido']
                    # Receita Proxy: 5% fixo sobre o valor total (Apenas para exemplificar o cálculo de métricas)
                    df_norm['receita'] = df['valor_total_do_pedido'] * 0.05
                    df_norm['venda'] = pd.to_datetime(df['data_da_criacao_do_pedido'], errors='coerce')
                    df_norm['ec'] = df['razao_social']
                    df_norm['plataforma'] = 'Bionio'
                    df_norm['tipo'] = df['nome_do_beneficio'].astype(str) # Usando nome_do_beneficio como tipo
                    df_norm['bandeira'] = df['tipo_de_pagamento'].astype(str) # Usando tipo_de_pagamento como bandeira

                df_norm.dropna(subset=['venda', 'bruto', 'cnpj'], inplace=True)
                all_transactions.append(df_norm)
                
            except Exception as e:
                st.warning(f"Aba '{sheet_name}' não processada: {e}", icon="⚠️")
        
        if not all_transactions:
            st.error("Nenhuma aba de transações válida foi carregada.")
            return pd.DataFrame(), pd.DataFrame()

        # 3. Concatenar todas as transações
        df_merged_transacoes = pd.concat(all_transactions, ignore_index=True)

        # 4. Unir com a Carteira de Clientes (Responsável Comercial/Produto)
        df_merged = pd.merge(
            df_merged_transacoes,
            df_clientes[['cnpj', 'responsavel_comercial', 'produto']],
            on='cnpj',
            how='left'
        )
        
        df_merged['responsavel_comercial'] = df_merged['responsavel_comercial'].fillna('Não Atribuído')
        # Usa o nome da Plataforma como produto caso não encontre na carteira
        df_merged['produto'] = df_merged['produto'].fillna(df_merged['plataforma'])

    except Exception as e:
        st.error(f"Erro fatal durante o processamento de dados: {e}")
        return pd.DataFrame(), pd.DataFrame()

    st.success(f"Dados consolidados de {len(all_transactions)} plataformas processados com sucesso!", icon="✅")
    return df_merged, df_clientes

def generate_insights(df_filtered, total_gmv, receita_total):
    """Gera insights automáticos baseados nos dados filtrados."""
    insights = []
    
    if df_filtered.empty:
        return ["Nenhum dado encontrado para o período/filtros selecionados."]
        
    # Insight 1: Vendedor com maior Receita
    if 'responsavel_comercial' in df_filtered.columns:
        df_receita_vendedor = df_filtered.groupby('responsavel_comercial')['receita'].sum().reset_index()
        top_vendedor = df_receita_vendedor.loc[df_receita_vendedor['receita'].idxmax()]
        insights.append(f"O **{top_vendedor['responsavel_comercial']}** é o vendedor com a maior Receita no período, gerando **R$ {top_vendedor['receita']:,.2f}**.")

    # Insight 2: Cliente com maior GMV
    if 'ec' in df_filtered.columns:
        df_gmv_cliente = df_filtered.groupby('ec')['bruto'].sum().reset_index()
        top_cliente = df_gmv_cliente.loc[df_gmv_cliente['bruto'].idxmax()]
        insights.append(f"O cliente **{top_cliente['ec']}** é o principal motor de vendas, com um GMV de **R$ {top_cliente['bruto']:,.2f}**.")
    
    # Insight 3: Plataforma/Produto com maior GMV
    if 'plataforma' in df_filtered.columns:
        df_plataforma = df_filtered.groupby('plataforma')['bruto'].sum().reset_index()
        top_plataforma = df_plataforma.loc[df_plataforma['bruto'].idxmax()]
        insights.append(f"A plataforma **{top_plataforma['plataforma']}** é a mais rentável por GMV, com **R$ {top_plataforma['bruto']:,.2f}**.")
    
    # Insight 4: Margem média
    margem_media = (receita_total / total_gmv) * 100 if total_gmv > 0 else 0
    insights.append(f"A margem média de Receita sobre o GMV no período é de **{margem_media:,.2f}%**.")

    return insights


# --- Interface Streamlit ---

st.title("💰 Rovema Bank - Dashboard de Transações Consolidadas")

# --- Área de Upload (Barra Lateral) ---
with st.sidebar:
    st.header("Upload do Arquivo")
    uploaded_file = st.file_uploader(
        "Arquivo Único (Excel .xlsx)",
        type=['xlsx'],
        key="excel_upload"
    )
    st.markdown("---")
    st.markdown("**Abas de Transação Usadas:**")
    for sheet in TRANSACOES_SHEET_NAMES:
        st.markdown(f"- `{sheet}`")
    st.markdown(f"- **Aba de Clientes:** `{CLIENTES_SHEET_NAME}`")


df_merged = pd.DataFrame()
df_clientes_original = pd.DataFrame()

if uploaded_file:
    df_merged, df_clientes_original = load_and_preprocess_data(uploaded_file)


# --- Dashboard Principal ---

if df_merged.empty or 'bruto' not in df_merged.columns:
    st.warning("Por favor, faça o upload do arquivo Excel na barra lateral para iniciar a análise.", icon="⚠️")
else:
    # --- FILTROS (Após o Título) ---
    st.subheader("Filtros de Análise")
    
    # Organiza os filtros em colunas
    col_date, col_plataforma, col_vendedor, col_bandeira, col_tipo, col_atualizar = st.columns([1.5, 1.5, 1.5, 1, 1, 0.5])
    
    with col_date:
        data_min = df_merged['venda'].min().date()
        data_max = df_merged['venda'].max().date()
        data_inicial, data_final = st.date_input(
            "Período",
            value=(data_min, data_max),
            min_value=data_min,
            max_value=data_max
        )

    with col_plataforma:
        # Plataforma/Produto agora é o principal filtro de produto
        plataformas = ['Todos'] + sorted(df_merged['plataforma'].unique().tolist())
        filtro_plataforma = st.selectbox("Plataforma/Produto", options=plataformas)

    with col_vendedor:
        vendedores = ['Todas'] + sorted(df_clientes_original['responsavel_comercial'].unique().tolist())
        filtro_vendedor = st.selectbox("Carteira", options=vendedores)
    
    with col_bandeira:
        # Filtro de Bandeira (usando 'bandeira' normalizada)
        bandeiras = ['Todas'] + sorted(df_merged['bandeira'].unique().tolist())
        filtro_bandeira = st.selectbox("Bandeira", options=bandeiras)
    
    with col_tipo:
        # Filtro de Tipo (usando 'tipo' normalizado)
        tipos = ['Todos'] + sorted(df_merged['tipo'].unique().tolist())
        filtro_tipo = st.selectbox("Tipo", options=tipos)
        
    with col_atualizar:
        st.markdown("<div style='height: 25px;'></div>", unsafe_allow_html=True) 
        st.button("Atualizar") 

    # --- Aplicação dos Filtros ---
    df_filtered = df_merged[
        (df_merged['venda'].dt.date >= data_inicial) &
        (df_merged['venda'].dt.date <= data_final)
    ].copy()
    
    if filtro_plataforma != 'Todos':
         df_filtered = df_filtered[df_filtered['plataforma'] == filtro_plataforma]
    
    if filtro_vendedor != 'Todas':
         df_filtered = df_filtered[df_filtered['responsavel_comercial'] == filtro_vendedor]
         
    if filtro_bandeira != 'Todas':
         df_filtered = df_filtered[df_filtered['bandeira'] == filtro_bandeira]

    if filtro_tipo != 'Todos':
         df_filtered = df_filtered[df_filtered['tipo'] == filtro_tipo]

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
            .st-emotion-cache-1r6r0z6 {color: #262730;} 
            .st-emotion-cache-1s0k10z {
                background-color: #f0f2f6; 
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

    # --- 4. Linha de Gráficos (Receita por Carteira e Participação por Plataforma/Produto) ---
    col6, col7 = st.columns([1.5, 1])

    with col6:
        st.subheader("Receita por Carteira")
        
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
        st.subheader("Participação por Plataforma/Produto")
        
        df_plataforma = df_filtered.groupby('plataforma')['bruto'].sum().reset_index()
        
        fig_plataforma = px.pie(
            df_plataforma, values='bruto', names='plataforma',
            title='Participação do GMV por Plataforma',
            color_discrete_sequence=px.colors.qualitative.Safe
        )
        fig_plataforma.update_traces(textinfo='percent+label', pull=[0.05, 0.0, 0.0])
        st.plotly_chart(fig_plataforma, use_container_width=True)
        
    st.markdown("---")

    # --- 5. Detalhamento por Cliente (Top 10 Crescimento/Queda - Proxy) ---
    st.subheader("🔍 Detalhamento por Cliente")
    
    col8, col9 = st.columns(2)
    
    # DataFrame de Ranqueamento de Clientes
    df_ranking_clientes = df_filtered.groupby(['cnpj', 'ec']).agg(
        GMV=('bruto', 'sum'),
        Receita=('receita', 'sum'),
        Vendas=('id_venda', 'nunique') if 'id_venda' in df_filtered.columns else ('cnpj', 'count')
    ).reset_index().sort_values(by='GMV', ascending=False)
    
    # TOP 10 (Proxy para Top 10 Crescimento)
    with col8:
        st.markdown("##### Top 10 Crescimento (Proxy: Maior GMV)")
        df_top10 = df_ranking_clientes.head(10).copy()
        df_top10['GMV'] = df_top10['GMV'].apply(lambda x: f"R$ {x:,.2f}")
        df_top10['Receita'] = df_top10['Receita'].apply(lambda x: f"R$ {x:,.2f}")
        df_top10.insert(1, '% Cresc. (Proxy)', ['N/A'] * len(df_top10)) 
        df_top10.rename(columns={'ec': 'Cliente', 'GMV': 'Transacionado', 'Vendas': 'Qtd. Vendas'}, inplace=True)
        
        st.dataframe(df_top10[['Cliente', '% Cresc. (Proxy)', 'Transacionado', 'Qtd. Vendas', 'Receita']], hide_index=True, use_container_width=True)

    # BOTTOM 10 (Proxy para Top 10 Queda)
    with col9:
        st.markdown("##### Top 10 Queda (Proxy: Menor GMV)")
        df_bottom10 = df_ranking_clientes.sort_values(by='GMV', ascending=True).head(10).copy()
        df_bottom10['GMV'] = df_bottom10['GMV'].apply(lambda x: f"R$ {x:,.2f}")
        df_bottom10['Receita'] = df_bottom10['Receita'].apply(lambda x: f"R$ {x:,.2f}")
        df_bottom10.insert(1, '% Queda (Proxy)', ['N/A'] * len(df_bottom10))
        df_bottom10.rename(columns={'ec': 'Cliente', 'GMV': 'Transacionado', 'Vendas': 'Qtd. Vendas'}, inplace=True)

        st.dataframe(df_bottom10[['Cliente', '% Queda (Proxy)', 'Transacionado', 'Qtd. Vendas', 'Receita']], hide_index=True, use_container_width=True)
        
    st.markdown("---")

    # --- 6. Insights Automáticos (Última Seção) ---
    st.subheader("💡 Insights Automáticos")
    insights_list = generate_insights(df_filtered, total_gmv, receita_total)
    
    cols_insights = st.columns(len(insights_list))
    for i, insight in enumerate(insights_list):
        cols_insights[i].markdown(f"""
            <div style="background-color: #e6f7ff; padding: 10px; border-radius: 5px; height: 100%;">
                <small>Insight {i+1}</small>
                <p style="font-size: 14px; margin: 0;">{insight}</p>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # --- Tabela Detalhada (Rodapé) ---
    with st.expander("Visualizar Todos os Dados (Detalhados)"):
        st.dataframe(df_filtered)
    
    # --- Exportar CSV (Botão) ---
    csv_data = df_filtered.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Exportar CSV do Detalhamento",
        data=csv_data,
        file_name='detalhamento_transacoes_filtrado.csv',
        mime='text/csv',
        key='download-csv'
    )
