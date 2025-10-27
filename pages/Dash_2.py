# /pages/Dash_2.py (Versão Otimizada)
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sqlite3 # Usando o banco de dados

st.set_page_config(layout="wide")
st.title("💰 Dashboard Consolidado de Transações")

# --- [SEGURANÇA] --- (Mantém)
if not st.session_state.get('logged_in'):
    st.error("🔒 Você precisa estar logado...")
    st.stop()

# --- FUNÇÃO DE QUERY ---
# (Você pode mover isso para um novo 'database_analytics.py')
@st.cache_data(ttl=600) # Cache de 10 min para a query
def get_data_from_db(data_inicial, data_final, filtro_plataforma, filtro_categoria):
    conn = sqlite3.connect('analytics.db') # Conecta no BD
    
    # Constrói a query dinamicamente
    query = f"SELECT * FROM transacoes_consolidadas WHERE venda BETWEEN ? AND ?"
    params = [data_inicial, data_final]
    
    if filtro_plataforma != 'Todos':
        query += " AND plataforma = ?"
        params.append(filtro_plataforma)
        
    if filtro_categoria != 'Todos':
        query += " AND categoria_pagamento = ?"
        params.append(filtro_categoria)
        
    # O Pandas lê o SQL e retorna SÓ OS DADOS FILTRADOS
    df_filtered = pd.read_sql(query, conn, params=params)
    conn.close()
    return df_filtered

# --- FILTROS ---
st.subheader("Filtros de Análise")
col_date, col_plataforma, col_categoria_pgto = st.columns([1.5, 1.5, 1.5])

with col_date:
    data_inicial, data_final = st.date_input("Período", ...)
with col_plataforma:
    # As opções podem ser lidas do BD: 
    # ex: pd.read_sql("SELECT DISTINCT plataforma FROM transacoes_consolidadas", conn)
    plataformas = ['Todos', 'Bionio', 'Rovema Pay', 'Eliq', 'Asto'] 
    filtro_plataforma = st.selectbox("Plataforma/Produto", options=plataformas)
with col_categoria_pgto:
    categorias = ['Todos', 'Pix', 'Crédito', 'Débito', 'Outros']
    filtro_categoria = st.selectbox("Categoria Pagamento", options=categorias)

# --- A MÁGICA ---
# O app busca DO BANCO DE DADOS apenas os dados já filtrados.
# Se o BD tem 10 milhões de linhas, mas o filtro só retorna 100,
# o app só carrega 100 linhas na memória.
try:
    df_filtered = get_data_from_db(data_inicial, data_final, filtro_plataforma, filtro_categoria)
except Exception as e:
    st.error(f"Erro ao consultar o banco de dados: {e}")
    st.stop()

if df_filtered.empty:
    st.warning("Nenhum dado encontrado para os filtros selecionados.")
else:
    # --- KPIs ---
    total_gmv = df_filtered['bruto'].sum()
    receita_total = df_filtered['receita'].sum()
    # ... (O resto do seu código de KPIs e gráficos funciona igual)

    col1, col2, ... = st.columns(5)
    col1.metric("Transacionado (Bruto)", f"R$ {total_gmv:,.2f}")
    # ...

    # --- Gráficos ---
    st.subheader("Evolução...")
    # (O código do gráfico de evolução funciona igual)
    df_evolucao = df_filtered.groupby(...)
    st.plotly_chart(fig_evolucao, use_container_width=True)
    
    # ... (Restante dos gráficos)
    
    # Você pode ATIVAR novamente as tabelas de detalhe,
    # pois df_filtered será pequeno.
    st.subheader("🔍 Detalhamento e Exportação")
    st.dataframe(df_filtered)
