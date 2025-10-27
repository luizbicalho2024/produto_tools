import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import io
import numpy as np
import requests  # Necessário para chamadas de API
from datetime import datetime, timedelta, date  # Necessário para datas nas APIs e min_value
import traceback  # Para exibir erros mais detalhados
import math # Necessário para dividir os chunks

# --- Configurações de Aparência ---
st.set_page_config(
    page_title="Dashboard Consolidado - Transações",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- [SEGURANÇA] ---
if not st.session_state.get('logged_in'):
    st.error("🔒 Você precisa estar logado para acessar esta página.")
    st.info("Por favor, retorne à página de Login e insira suas credenciais.")
    st.stop()
# --- FIM SEGURANÇA ---


# --- [CONSTANTES] ---
BIONIO_COLS = {
    'cnpj': 'cnpj_da_organizao',
    'bruto': 'valor_total_do_pedido',
    'data': 'data_da_criao_do_pedido',
    'ec': 'razo_social',
    'tipo': 'nome_do_benefcio',
    'bandeira': 'tipo_de_pagamento'
}

MAQUININHA_COLS = {
    'cnpj': 'cnpj',
    'bruto': 'bruto',
    'liquido': 'liquido',
    'data': 'venda',
    'ec': 'ec',
    'tipo': 'tipo',
    'bandeira': 'bandeira'
}
# --- FIM CONSTANTES ---


# --- Funções Auxiliares (Categoria de Pagamento) ---
def categorize_payment_bionio(tipo_pgto):
    tipo_pgto_lower = str(tipo_pgto).strip().lower()
    if 'pix' in tipo_pgto_lower or 'transferência' in tipo_pgto_lower: return 'Pix'
    if 'cartão' in tipo_pgto_lower: return 'Crédito'
    if 'boleto' in tipo_pgto_lower: return 'Boleto'
    return 'Outros'

def categorize_payment_rp(row, tipo_col, bandeira_col):
    bandeira_lower = str(row.get(bandeira_col, '')).strip().lower()
    tipo_lower = str(row.get(tipo_col, '')).strip().lower()
    if 'pix' in bandeira_lower: return 'Pix'
    if tipo_lower == 'crédito': return 'Crédito'
    if tipo_lower == 'débito': return 'Débito'
    return 'Outros'


# --- Funções de Chamada de API ---

@st.cache_data(show_spinner="Buscando dados da API Eliq/UzziPay...")
def fetch_eliq_data(api_token, start_date, end_date):
    """ 
    Busca dados da API Eliq/UzziPay.
    Divide a consulta em 4 partes para evitar estouro de memória.
    """
    base_url = "https://sigyo.uzzipay.com/api/transacoes"
    headers = {'Authorization': f'Bearer {api_token}'}
    
    total_days = (end_date - start_date).days + 1
    num_chunks = 4
    chunk_size_days = max(1, int(math.ceil(total_days / num_chunks)))
    
    all_dfs = [] 
    
    st.info(f"API Eliq: Consulta de {total_days} dias dividida em {num_chunks} partes.")

    current_start = start_date
    
    try:
        for i in range(num_chunks):
            if current_start > end_date:
                break 

            current_end = min(current_start + timedelta(days=chunk_size_days - 1), end_date)
            
            start_str = current_start.strftime('%d/%m/%Y')
            end_str = current_end.strftime('%d/%m/%Y')
            
            st.toast(f"Buscando parte {i+1}/{num_chunks}: {start_str} a {end_str}")

            all_data_chunk = [] 
            page = 1
            max_pages = 50 
            
            params = {'TransacaoSearch[data_cadastro]': f'{start_str} - {end_str}'}

            while page <= max_pages:
                params['page'] = page
                response = requests.get(base_url, headers=headers, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()
                
                if isinstance(data, list) and data:
                    all_data_chunk.extend(data)
                    page += 1
                else:
                    break 
            
            if all_data_chunk:
                df_chunk = pd.json_normalize(all_data_chunk)
                all_dfs.append(df_chunk)
            
            current_start = current_end + timedelta(days=1)
        
        if not all_dfs:
            st.warning(f"API Eliq: Nenhuma transação encontrada para o período total.", icon="⚠️")
            return pd.DataFrame()

        df = pd.concat(all_dfs, ignore_index=True)
        st.success(f"API Eliq: Total de {len(df)} registros carregados de {num_chunks} partes.")

        # --- Normalização ---
        df_norm = pd.DataFrame()
        df_norm['cnpj'] = df.get('cliente_cnpj', pd.NA).astype(str).str.strip() 
        df_norm['bruto'] = pd.to_numeric(df.get('valor_total'), errors='coerce').fillna(0)

        taxa_column_name = 'cliente_taxa_adm'
        if taxa_column_name in df.columns:
            taxa_cliente_series = pd.to_numeric(df[taxa_column_name], errors='coerce').fillna(0)
        else:
            taxa_cliente_series = pd.Series(0, index=df.index, dtype=float)

        df_norm['receita'] = (df_norm['bruto'] * (taxa_cliente_series / 100)).clip(lower=0)
        df_norm['venda'] = pd.to_datetime(df.get('data_cadastro', pd.NaT), errors='coerce') 
        df_norm = df_norm.dropna(subset=['venda'])
        if df_norm.empty: return pd.DataFrame()

        df_norm['ec'] = df.get('cliente_nome', 'N/A')
        df_norm['plataforma'] = 'Eliq'
        df_norm['tipo'] = df.get('tipo_transacao_sigla', 'N/A').astype(str)
        df_norm['bandeira'] = df.get('bandeira', 'N/A').astype(str)
        df_norm['categoria_pagamento'] = 'Outros'

        df_norm['cnpj'] = df_norm['cnpj'].replace(['NA', 'None', '<NA>', ''], np.nan) 
        return df_norm.dropna(subset=['bruto', 'cnpj'])

    except requests.exceptions.Timeout:
         st.error(f"Erro API Eliq: Timeout (>60s) na parte {i+1}. Tente período menor.")
         return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        st.error(f"Erro de conexão/HTTP na API Eliq: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro inesperado ao processar dados da API Eliq: {e}")
        st.error(traceback.format_exc())
        return pd.DataFrame()

@st.cache_data(show_spinner="Buscando dados da API Asto/Logpay (Limitado)...")
def fetch_asto_data(api_username, api_password, start_date, end_date):
    """ Placeholder para API Asto/Logpay. """
    st.warning("A API Asto/Logpay não é otimizada para relatórios. Função placeholder ativa.", icon="⚠️")
    df_norm_placeholder = pd.DataFrame(columns=['cnpj', 'bruto', 'receita', 'venda', 'ec', 'plataforma', 'tipo', 'bandeira', 'categoria_pagamento'])
    return df_norm_placeholder

@st.cache_data(show_spinner="Carregando e processando Bionio...")
def load_bionio_csv(uploaded_file):
    """ Carrega o CSV do Bionio e normaliza. """
    try:
        df = None
        encodings_to_try = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
        for encoding in encodings_to_try:
            try:
                uploaded_file.seek(0)
                # [CORREÇÃO] Lê tudo como string primeiro
                df = pd.read_csv(uploaded_file, encoding=encoding, sep=None, engine='python', thousands='.', decimal=',', dtype=str)
                break
            except Exception:
                continue
        if df is None: raise ValueError("Não foi possível ler o arquivo Bionio com encodings comuns.")

        df.columns = (df.columns.str.strip().str.lower()
                      .str.replace(' ', '_', regex=False)
                      .str.replace('[^0-9a-zA-Z_]', '', regex=True))
        
        cleaned_columns = df.columns.tolist()
        missing_cols = []
        expected_cols_map = BIONIO_COLS
        for key, col_name in expected_cols_map.items():
            if col_name not in cleaned_columns:
                missing_cols.append(f"'{col_name}' (para {key})")
        
        # A verificação de colunas vem ANTES da conversão
        if missing_cols:
            st.error(f"Erro no arquivo Bionio: Colunas esperadas não encontradas: {', '.join(missing_cols)}. Colunas disponíveis: {', '.join(cleaned_columns)}.")
            return pd.DataFrame()

        df_norm = pd.DataFrame()
        df_norm['cnpj'] = df[expected_cols_map['cnpj']].astype(str).str.strip()
        # [CORREÇÃO] Conversão numérica movida para cá, após a verificação
        df_norm['bruto'] = pd.to_numeric(df[expected_cols_map['bruto']], errors='coerce').fillna(0)
        df_norm['receita'] = df_norm['bruto'] * 0.05
        df_norm['venda'] = pd.to_datetime(df[expected_cols_map['data']], errors='coerce', dayfirst=True, format='%d/%m/%Y %H:%M:%S')
        if df_norm['venda'].isnull().all(): 
            df_norm['venda'] = pd.to_datetime(df[expected_cols_map['data']], errors='coerce', dayfirst=True)
        
        df_norm = df_norm.dropna(subset=['venda'])
        if df_norm.empty: return pd.DataFrame()

        df_norm['ec'] = df[expected_cols_map['ec']]
        df_norm['plataforma'] = 'Bionio'
        df_norm['tipo'] = df[expected_cols_map['tipo']].astype(str)
        df_norm['bandeira'] = df[expected_cols_map['bandeira']].astype(str)
        df_norm['categoria_pagamento'] = df[expected_cols_map['bandeira']].apply(categorize_payment_bionio)

        st.success(f"Bionio: {len(df_norm)} registros carregados e processados.")
        df_norm['cnpj'] = df_norm['cnpj'].replace(['NA', 'None', '<NA>', ''], np.nan)
        return df_norm.dropna(subset=['bruto', 'cnpj'])

    except Exception as e: 
        st.error(f"Erro inesperado ao processar Bionio: {e}"); 
        st.error(traceback.format_exc())
    return pd.DataFrame()

@st.cache_data(show_spinner="Carregando e processando Maquininha/Veripag...")
def load_maquininha_csv(uploaded_file):
    """ Carrega o CSV da Maquininha/Veripag e normaliza. """
    try:
        df = None
        encodings_to_try = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
        for encoding in encodings_to_try:
            try:
                uploaded_file.seek(0)
                # [CORREÇÃO] Lê tudo como string primeiro
                df = pd.read_csv(uploaded_file, encoding=encoding, sep=None, engine='python', decimal=',', dtype=str)
                break
            except Exception:
                continue
        if df is None: raise ValueError("Não foi possível ler o arquivo Maquininha.")

        df.columns = (df.columns.str.strip().str.lower()
                      .str.replace(' ', '_', regex=False)
                      .str.replace('[^0-9a-zA-Z_]', '', regex=True))
        
        cleaned_columns = df.columns.tolist()
        missing_cols = []
        expected_cols_map = MAQUININHA_COLS
        for key, col_name in expected_cols_map.items():
            if col_name not in cleaned_columns:
                missing_cols.append(f"'{col_name}' (para {key})")

        if missing_cols:
            st.error(f"Erro no arquivo Maquininha: Colunas esperadas não encontradas: {', '.join(missing_cols)}. Colunas disponíveis: {', '.join(cleaned_columns)}.")
            return pd.DataFrame()

        df_norm = pd.DataFrame()
        df_norm['cnpj'] = df[expected_cols_map['cnpj']].astype(str).str.strip()
        # [CORREÇÃO] Conversão numérica movida para cá, após a verificação
        df_norm['bruto'] = pd.to_numeric(df[expected_cols_map['bruto']], errors='coerce').fillna(0)
        df_norm['liquido'] = pd.to_numeric(df[expected_cols_map['liquido']], errors='coerce').fillna(0)
        df_norm['receita'] = (df_norm['bruto'] - df_norm['liquido']).clip(lower=0)
        df_norm['venda'] = pd.to_datetime(df[expected_cols_map['data']], errors='coerce', dayfirst=True, format='%d/%m/%Y %H:%M:%S')
        if df_norm['venda'].isnull().all(): 
            df_norm['venda'] = pd.to_datetime(df[expected_cols_map['data']], errors='coerce', dayfirst=True)
        
        df_norm = df_norm.dropna(subset=['venda'])
        if df_norm.empty: return pd.DataFrame()

        df_norm['ec'] = df[expected_cols_map['ec']]
        df_norm['plataforma'] = 'Rovema Pay'
        df_norm['tipo'] = df[expected_cols_map['tipo']].astype(str)
        df_norm['bandeira'] = df[expected_cols_map['bandeira']].astype(str)
        
        df_norm['categoria_pagamento'] = df.apply(
            lambda row: categorize_payment_rp(row, expected_cols_map['tipo'], expected_cols_map['bandeira']), 
            axis=1
        )

        st.success(f"Maquininha: {len(df_norm)} registros carregados e processados.")
        df_norm['cnpj'] = df_norm['cnpj'].replace(['NA', 'None', '<NA>', ''], np.nan)
        return df_norm.dropna(subset=['bruto', 'cnpj'])

    except Exception as e: 
        st.error(f"Erro inesperado ao processar Maquininha: {e}"); 
        st.error(traceback.format_exc())
    return pd.DataFrame()

# --- Funções de Consolidação e Insights ---

def consolidate_data(df_bionio, df_maquininha, df_eliq, df_asto):
    """ Concatena todos os DataFrames normalizados. """
    all_transactions = []
    if df_bionio is not None and not df_bionio.empty: all_transactions.append(df_bionio)
    if df_maquininha is not None and not df_maquininha.empty: all_transactions.append(df_maquininha)
    if df_eliq is not None and not df_eliq.empty: all_transactions.append(df_eliq)
    if df_asto is not None and not df_asto.empty: all_transactions.append(df_asto)

    if not all_transactions: return pd.DataFrame()

    try:
        df_consolidated = pd.concat(all_transactions, ignore_index=True)
        
        if 'cnpj' in df_consolidated.columns:
            df_consolidated['cnpj'] = df_consolidated['cnpj'].astype(str)

        if 'responsavel_comercial' not in df_consolidated.columns: df_consolidated['responsavel_comercial'] = 'N/A'
        if 'produto' not in df_consolidated.columns: df_consolidated['produto'] = df_consolidated['plataforma']
        
        df_consolidated['bruto'] = pd.to_numeric(df_consolidated['bruto'], errors='coerce').fillna(0)
        df_consolidated['receita'] = pd.to_numeric(df_consolidated['receita'], errors='coerce').fillna(0).clip(lower=0)
        df_consolidated['venda'] = pd.to_datetime(df_consolidated['venda'], errors='coerce')
        df_consolidated = df_consolidated.dropna(subset=['venda'])

        st.success(f"Dados de {len(all_transactions)} fontes ({len(df_consolidated)} registros totais) consolidados com sucesso!", icon="✅")
        return df_consolidated
    except Exception as e:
        st.error(f"Erro during data concatenation: {e}")
        st.error(traceback.format_exc())
        return pd.DataFrame()

def generate_insights(df_filtered, total_gmv, receita_total):
    """Gera insights automáticos."""
    insights = []
    if df_filtered.empty: return ["Nenhum dado encontrado para os filtros selecionados."]
    if 'ec' in df_filtered.columns and not df_filtered.empty:
        df_gmv_cliente = df_filtered.groupby('ec')['bruto'].sum().nlargest(1).reset_index()
        if not df_gmv_cliente.empty: insights.append(f"Cliente principal (GMV): **{df_gmv_cliente.iloc[0]['ec']}** (R$ {df_gmv_cliente.iloc[0]['bruto']:,.2f}).")
    if 'plataforma' in df_filtered.columns and not df_filtered.empty:
         df_plataforma = df_filtered.groupby('plataforma')['bruto'].sum().nlargest(1).reset_index()
         if not df_plataforma.empty: insights.append(f"Plataforma principal (GMV): **{df_plataforma.iloc[0]['plataforma']}** (R$ {df_plataforma.iloc[0]['bruto']:,.2f}).")
    margem = (receita_total / total_gmv) * 100 if total_gmv > 0 else 0
    insights.append(f"Margem média no período: **{margem:,.2f}%**.")
    if 'categoria_pagamento' in df_filtered.columns and not df_filtered.empty:
        top_cat_series = df_filtered['categoria_pagamento'].mode()
        if not top_cat_series.empty: 
            insights.append(f"Categoria de pag. mais frequente: **{top_cat_series.iloc[0]}**.")
    return insights

# --- Interface Streamlit ---
st.title("💰 Dashboard Consolidado de Transações")

# --- Barra Lateral ---
with st.sidebar:
    st.header("Fontes de Dados")
    uploaded_bionio = st.file_uploader("1. Arquivo Bionio (.csv)", type=['csv'], key="bionio_upload")
    uploaded_maquininha = st.file_uploader("2. Arquivo Maquininha/Veripag (.csv)", type=['csv'], key="maquininha_upload")
    st.markdown("---")
    
    with st.expander("Configuração das APIs", expanded=True):
        today = datetime.now().date()
        default_start_date = today - timedelta(days=30)
        api_start_date, api_end_date = st.date_input(
             "Período para APIs", value=(default_start_date, today),
             min_value=date(2020, 1, 1), max_value=today + timedelta(days=1),
             key="api_date_range"
         )
        st.info("Credenciais das APIs são carregadas de 'secrets.toml'", icon="ℹ️")
        
    load_button_pressed = st.button("Carregar e Processar Dados", key="load_data_button", use_container_width=True, type="primary")

# --- Carregamento e Processamento Principal ---
if 'data_loaded' not in st.session_state: st.session_state.data_loaded = False
if 'df_consolidated' not in st.session_state: st.session_state.df_consolidated = pd.DataFrame()

if load_button_pressed:
    st.session_state.data_loaded = True
    st.session_state.df_consolidated = pd.DataFrame()
    with st.spinner("Processando fontes de dados... Por favor, aguarde."):
        df_bionio_processed = load_bionio_csv(uploaded_bionio) if uploaded_bionio else pd.DataFrame()
        df_maquininha_processed = load_maquininha_csv(uploaded_maquininha) if uploaded_maquininha else pd.DataFrame()
        df_eliq_fetched = pd.DataFrame()
        df_asto_fetched = pd.DataFrame()
        
        try:
            eliq_token = st.secrets.get("eliq_api_token")
            asto_user = st.secrets.get("asto_username")
            asto_pass = st.secrets.get("asto_password")

            if eliq_token:
                df_eliq_fetched = fetch_eliq_data(eliq_token, api_start_date, api_end_date)
            else: st.sidebar.warning("Token API Eliq (eliq_api_token) não encontrado nos Secrets.", icon="🔑")
            
            if asto_user and asto_pass:
                 df_asto_fetched = fetch_asto_data(asto_user, asto_pass, api_start_date, api_end_date)
            else: st.sidebar.warning("Credenciais API Asto (asto_username/asto_password) não encontradas nos Secrets.", icon="🔑")
        
        except Exception as e: 
            st.sidebar.error(f"Erro ao ler 'secrets.toml'. Verifique o arquivo. Erro: {e}", icon="❌")

        df_consolidated_loaded = consolidate_data(
            df_bionio_processed, df_maquininha_processed,
            df_eliq_fetched, df_asto_fetched
        )
        st.session_state.df_consolidated = df_consolidated_loaded
        df_consolidated = df_consolidated_loaded
else:
    df_consolidated = st.session_state.df_consolidated

# --- Dashboard Principal ---
if df_consolidated.empty:
    if st.session_state.data_loaded: 
        st.error("O carregamento falhou ou não retornou dados. Verifique os arquivos de upload e as mensagens de erro.")
    else: 
        st.warning("Faça upload dos arquivos (se houver) e clique em 'Carregar e Processar Dados' na barra lateral.", icon="⚠️")
else:
    # --- FILTROS ---
    st.subheader("Filtros de Análise")
    col_date, col_plataforma, col_bandeira, col_tipo, col_categoria_pgto = st.columns([1.5, 1.5, 1, 1, 1.5])
    
    with col_date:
        data_min = df_consolidated['venda'].min().date()
        data_max = df_consolidated['venda'].max().date()
        
        current_start, current_end = st.session_state.get('date_filter_values', (data_min, data_max))
        valid_start = max(data_min, current_start)
        valid_end = min(data_max, current_end)
        if valid_start > valid_end: 
            valid_start, valid_end = data_min, data_max
            
        data_inicial, data_final = st.date_input(
            "Período", 
            value=(valid_start, valid_end), 
            min_value=data_min, 
            max_value=data_max, 
            key='date_filter'
        )
        st.session_state.date_filter_values = (data_inicial, data_final)

    with col_plataforma:
        plataformas = ['Todos'] + sorted(df_consolidated['plataforma'].unique().tolist())
        filtro_plataforma = st.selectbox("Plataforma/Produto", options=plataformas)
    with col_bandeira:
        bandeiras = ['Todos'] + sorted(df_consolidated['bandeira'].astype(str).fillna('N/A').unique().tolist())
        filtro_bandeira = st.selectbox("Bandeira (Detalhe)", options=bandeiras)
    with col_tipo:
        tipos = ['Todos'] + sorted(df_consolidated['tipo'].astype(str).fillna('N/A').unique().tolist())
        filtro_tipo = st.selectbox("Tipo (Detalhe)", options=tipos)
    with col_categoria_pgto:
        categorias = ['Todos'] + sorted(df_consolidated['categoria_pagamento'].astype(str).fillna('N/A').unique().tolist())
        filtro_categoria = st.selectbox("Categoria Pagamento", options=categorias)

    # --- Aplicação dos Filtros ---
    df_filtered = pd.DataFrame(columns=df_consolidated.columns)
    if data_inicial <= data_final:
        mask_date = (df_consolidated['venda'].dt.date >= data_inicial) & (df_consolidated['venda'].dt.date <= data_final)
        df_filtered = df_consolidated[mask_date].copy()
        
        if filtro_plataforma != 'Todos': df_filtered = df_filtered[df_filtered['plataforma'] == filtro_plataforma]
        if filtro_bandeira != 'Todos': df_filtered = df_filtered[df_filtered['bandeira'].astype(str).fillna('N/A') == filtro_bandeira]
        if filtro_tipo != 'Todos': df_filtered = df_filtered[df_filtered['tipo'].astype(str).fillna('N/A') == filtro_tipo]
        if filtro_categoria != 'Todos': df_filtered = df_filtered[df_filtered['categoria_pagamento'].astype(str).fillna('N/A') == filtro_categoria]
    else: 
        st.warning("Data inicial não pode ser posterior à data final.")

    # --- KPIs ---
    total_gmv = df_filtered['bruto'].sum()
    receita_total = df_filtered['receita'].sum()
    clientes_ativos = df_filtered['cnpj'].nunique()
    margem_media = (receita_total / total_gmv) * 100 if total_gmv > 0 else 0
    gmv_por_cliente = df_filtered.groupby('cnpj')['bruto'].sum() if not df_filtered.empty else pd.Series(dtype=float)
    clientes_em_queda_proxy = gmv_por_cliente[gmv_por_cliente < 1000].count()

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Transacionado (Bruto)", f"R$ {total_gmv:,.2f}")
    col2.metric("Nossa Receita", f"R$ {receita_total:,.2f}")
    col3.metric("Margem Média", f"{margem_media:,.2f}%")
    col4.metric("Clientes Ativos", f"{clientes_ativos:,}")
    col5.metric("Clientes em Queda (Proxy)", f"{clientes_em_queda_proxy:,}")
    st.markdown("---")

    # --- Gráfico de Evolução ---
    st.subheader("Evolução do Valor Transacionado vs Receita")
    if not df_filtered.empty:
        df_evolucao = df_filtered.groupby(df_filtered['venda'].dt.date).agg(GMV=('bruto', 'sum'), Receita=('receita', 'sum')).reset_index().rename(columns={'venda': 'Data da Venda'})
        if not df_evolucao.empty:
            fig_evolucao = go.Figure()
            fig_evolucao.add_trace(go.Scatter(x=df_evolucao['Data da Venda'], y=df_evolucao['GMV'], mode='lines+markers', name='Valor Bruto', line=dict(color='blue', width=2), marker=dict(size=5)))
            fig_evolucao.add_trace(go.Scatter(x=df_evolucao['Data da Venda'], y=df_evolucao['Receita'], mode='lines+markers', name='Receita', line=dict(color='red', width=2), marker=dict(size=5)))
            fig_evolucao.update_layout(xaxis_title='Data', yaxis_title='Valor (R$)', hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), template="plotly_white", margin=dict(t=20))
            st.plotly_chart(fig_evolucao, use_container_width=True)
        else: st.info("Nenhum dado agregado para gráfico de evolução.")
    else: st.info("Nenhum dado para gráfico de evolução.")
    st.markdown("---")

    # --- Gráficos Inferiores ---
    col6, col7 = st.columns([1.5, 1])
    with col6:
        st.subheader("Receita por Plataforma/Produto")
        if not df_filtered.empty:
            df_receita_plataforma = df_filtered.groupby('plataforma')['receita'].sum().reset_index().sort_values(by='receita', ascending=False)
            if not df_receita_plataforma.empty and df_receita_plataforma['receita'].sum() > 0:
                fig_receita_plat = px.bar(df_receita_plataforma, x='plataforma', y='receita', labels={'plataforma': 'Plataforma', 'receita': 'Receita (R$)'}, color='plataforma', color_discrete_sequence=px.colors.qualitative.Plotly, title='Receita Gerada por Plataforma', text='receita')
                fig_receita_plat.update_traces(texttemplate='R$ %{y:,.2f}', textposition='outside')
                fig_receita_plat.update_layout(uniformtext_minsize=8, uniformtext_mode='hide', template="plotly_white", xaxis_title=None, showlegend=False, margin=dict(t=30))
                st.plotly_chart(fig_receita_plat, use_container_width=True)
            else: st.info("Nenhuma receita registrada para gráfico por plataforma.")
        else: st.info("Nenhum dado para gráfico de receita por plataforma.")

    with col7:
        st.subheader("Participação por Categoria de Pagamento")
        if not df_filtered.empty and 'categoria_pagamento' in df_filtered.columns:
            df_categoria_pgto = df_filtered.groupby('categoria_pagamento')['bruto'].sum().reset_index().sort_values(by='bruto', ascending=False)
            if not df_categoria_pgto.empty and df_categoria_pgto['bruto'].astype(float).sum() > 0:
                pull_values = [0] * len(df_categoria_pgto); pull_values[0] = 0.1
                fig_categoria = px.pie(df_categoria_pgto, values='bruto', names='categoria_pagamento', title='Participação do GMV por Cat. Pagamento', color_discrete_sequence=px.colors.qualitative.Safe)
                fig_categoria.update_traces(textinfo='percent+label', pull=pull_values, marker=dict(line=dict(color='#000000', width=1)))
                fig_categoria.update_layout(legend_title_text='Categoria', margin=dict(t=30))
                st.plotly_chart(fig_categoria, use_container_width=True)
            else: st.info("Nenhum valor bruto positivo para gráfico de pizza.")
        else: st.info("Nenhum dado para gráfico de pizza.")
    st.markdown("---")

    # --- Detalhamento por Cliente ---
    st.subheader("🔍 Detalhamento por Cliente")
    if not df_filtered.empty:
        def get_most_frequent(series):
            if series.empty or series.mode().empty: return 'N/A'
            return series.mode().iloc[0]

        df_detalhe_cliente = df_filtered.groupby(['cnpj', 'ec']).agg(
            Receita=('receita', 'sum'), N_Vendas=('cnpj', 'count'),
            Categoria_Pag_Principal=('categoria_pagamento', get_most_frequent)
        ).reset_index()
        df_detalhe_cliente['Crescimento'] = 'N/A'
        df_detalhe_cliente['Receita_Formatada'] = df_detalhe_cliente['Receita'].apply(lambda x: f"R$ {x:,.2f}")
        df_detalhe_cliente = df_detalhe_cliente.sort_values(by='Receita', ascending=False)
        
        df_display = df_detalhe_cliente[['cnpj', 'ec', 'Receita_Formatada', 'Crescimento', 'N_Vendas', 'Categoria_Pag_Principal']]
        df_display.columns = ['CNPJ', 'Cliente', 'Receita', 'Crescimento', 'Nº Vendas', 'Cat. Pag. Principal']

        st.info("""**Sobre esta tabela:**\n* **Crescimento:** 'N/A' - Requer dados de período anterior.\n* **Nº Vendas:** Contagem total de transações.\n* **Cat. Pag. Principal:** Categoria de pagamento mais frequente.""", icon="ℹ️")
        csv_detalhe_cliente = df_display.to_csv(index=False).encode('utf-8')
        st.download_button("Exportar CSV (Det. Cliente)", csv_detalhe_cliente, 'detalhamento_cliente.csv', 'text/csv', key='dl-csv-det-cli')
        
        df_display['CNPJ'] = df_display['CNPJ'].astype(str)
        st.dataframe(df_display, hide_index=True, width='stretch')
        
        st.markdown(f"**Mostrando {len(df_display)} clientes**")
    else: st.warning("Nenhum dado de cliente para exibir com os filtros atuais.")
    st.markdown("---")

    # --- Insights ---
    st.subheader("💡 Insights Automáticos")
    insights_list = generate_insights(df_filtered, total_gmv, receita_total)
    cols_insights = st.columns(len(insights_list) if insights_list else 1)
    for i, insight in enumerate(insights_list):
        cols_insights[i].markdown(f"""<div style="background-color: #e6f7ff; padding: 10px; border-radius: 5px; height: 100%;"><small>Insight {i+1}</small><p style="font-size: 14px; margin: 0;">{insight}</p></div>""", unsafe_allow_html=True)
    st.markdown("---")

    # --- Tabela Detalhada (Rodapé) ---
    with st.expander("Visualizar Todos os Dados Filtrados (Detalhados)"):
         df_filtered['cnpj'] = df_filtered['cnpj'].astype(str)
         st.dataframe(df_filtered, width='stretch')

    csv_data_filtered = df_filtered.to_csv(index=False).encode('utf-8')
    st.download_button("Exportar CSV (Dados Filtrados)", csv_data_filtered, 'detalhamento_filtrado.csv', 'text/csv', key='dl-csv-filt')
