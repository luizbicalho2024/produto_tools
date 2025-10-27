import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import io
import numpy as np
import requests  # Necessário para chamadas de API
from datetime import datetime, timedelta, date  # Necessário para datas nas APIs e min_value
import traceback  # Para exibir erros mais detalhados

# --- Configurações de Aparência ---
st.set_page_config(
    page_title="Dashboard Consolidado - Transações",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Funções de Chamada de API ---

@st.cache_data(show_spinner="Buscando dados da API Eliq/Sygio...")
def fetch_eliq_data(api_token, start_date, end_date):
    """ Busca dados da API Eliq/Sygio e normaliza. """
    base_url = "https://sigyo.uzzipay.com/api/transacoes"
    headers = {'Authorization': f'Bearer {api_token}'}
    start_str = start_date.strftime('%d/%m/%Y')
    end_str = end_date.strftime('%d/%m/%Y')
    params = {'TransacaoSearch[data_cadastro]': f'{start_str} - {end_str}'}

    all_data = []
    page = 1
    page_size_assumption = 50
    processed_pages = 0
    
    try:
        while True:
            params['page'] = page
            response = requests.get(base_url, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            processed_pages = page

            if isinstance(data, list) and data:
                all_data.extend(data)
                if len(data) < page_size_assumption:
                    break
                page += 1
            else:
                break

        if not all_data: 
            return pd.DataFrame(), 0, 0

        df = pd.json_normalize(all_data)
        if df.empty: 
            return pd.DataFrame(), processed_pages, 0

        df_norm = pd.DataFrame()
        df_norm['cnpj'] = df.get('cliente_cnpj', pd.NA).astype(str).fillna('N/A')
        bruto_series = pd.to_numeric(df.get('valor_total'), errors='coerce').fillna(0)
        df_norm['bruto'] = bruto_series

        taxa_column_name = 'cliente_taxa_adm'
        if taxa_column_name in df.columns:
            taxa_cliente_series = pd.to_numeric(df[taxa_column_name], errors='coerce').fillna(0)
        else:
            taxa_cliente_series = pd.Series(0, index=df.index, dtype=float)

        taxa_cliente_percent = taxa_cliente_series / 100
        df_norm['receita'] = (df_norm['bruto'] * taxa_cliente_percent).clip(lower=0)

        df_norm['venda'] = pd.to_datetime(df.get('data_cadastro', pd.NaT), errors='coerce', dayfirst=True)
        df_norm = df_norm.dropna(subset=['venda'])
        if df_norm.empty: 
            return pd.DataFrame(), processed_pages, 0

        df_norm['ec'] = df.get('cliente_nome', 'N/A')
        df_norm['plataforma'] = 'Eliq' # REQUISITO CUMPRIDO

        tipo_col_name = 'tipo_transacao_sigla'
        if tipo_col_name in df.columns:
            df_norm['tipo'] = df[tipo_col_name].astype(str).fillna('N/A')
        else:
            df_norm['tipo'] = 'N/A'

        bandeira_col_name = 'bandeira'
        if bandeira_col_name in df.columns:
            df_norm['bandeira'] = df[bandeira_col_name].astype(str).fillna('N/A')
        else:
            df_norm['bandeira'] = 'N/A'

        df_norm['categoria_pagamento'] = 'Outros'

        final_df = df_norm.dropna(subset=['bruto', 'cnpj'])
        return final_df, processed_pages, len(final_df)

    except requests.exceptions.Timeout:
        st.error(f"Erro API Eliq: Timeout (>60s) na página {page}. Tente período menor.")
        return pd.DataFrame(), page, 0
    except requests.exceptions.RequestException as e:
        st.error(f"Erro de conexão/HTTP na API Eliq: {e}")
        return pd.DataFrame(), page, 0
    except Exception as e:
        st.error(f"Erro inesperado ao processar dados da API Eliq: {e}")
        st.error(traceback.format_exc())
        return pd.DataFrame(), page, 0

@st.cache_data(show_spinner="Buscando dados da API Asto/Logpay (Limitado)...")
def fetch_asto_data(api_username, api_password, start_date, end_date):
    """ Placeholder para API Asto/Logpay. """
    df_norm_placeholder = pd.DataFrame(columns=[
        'cnpj', 'bruto', 'receita', 'venda', 'ec', 'plataforma', 
        'tipo', 'bandeira', 'categoria_pagamento'
    ])
    
    # Se esta função fosse implementada, os dados teriam 'plataforma' = 'Asto'
    # df_norm_placeholder['plataforma'] = 'Asto' # REQUISITO CUMPRIDO
    
    df_norm_placeholder['plataforma'] = df_norm_placeholder['plataforma'].astype(str)
    df_norm_placeholder['cnpj'] = df_norm_placeholder['cnpj'].astype(str)
    
    return df_norm_placeholder, 0

@st.cache_data(show_spinner="Carregando e processando Bionio...")
def load_bionio_csv(uploaded_file):
    """ Carrega o CSV do Bionio e normaliza. """
    try:
        df = None
        encodings_to_try = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
        for encoding in encodings_to_try:
            try:
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file, encoding=encoding, sep=None, engine='python', thousands='.', decimal=',')
                break
            except Exception:
                continue
        if df is None: 
            raise ValueError("Não foi possível ler o arquivo Bionio com encodings comuns.")

        df.columns = (df.columns.str.strip().str.lower()
                      .str.replace(' ', '_', regex=False)
                      .str.replace('[^0-9a-zA-Z_]', '', regex=True))
        cleaned_columns = df.columns.tolist()

        cnpj_col = 'cnpj_da_organizao'
        bruto_col = 'valor_total_do_pedido'
        data_col = 'data_da_criao_do_pedido'
        ec_col = 'razo_social'
        beneficio_col = 'nome_do_benefcio'
        pagamento_col = 'tipo_de_pagamento'

        expected_cols = {cnpj_col, bruto_col, data_col, ec_col, beneficio_col, pagamento_col}
        missing_cols = expected_cols - set(cleaned_columns)
        if missing_cols: 
            raise KeyError(f"Colunas esperadas não encontradas no Bionio: {', '.join(missing_cols)}.")

        df_norm = pd.DataFrame()
        df_norm['cnpj'] = df[cnpj_col].astype(str).fillna('N/A')
        df_norm['bruto'] = pd.to_numeric(df[bruto_col], errors='coerce').fillna(0)
        df_norm['receita'] = df_norm['bruto'] * 0.05
        df_norm['venda'] = pd.to_datetime(df[data_col], errors='coerce', dayfirst=True, format='%d/%m/%Y %H:%M:%S')
        if df_norm['venda'].isnull().all(): 
            df_norm['venda'] = pd.to_datetime(df[data_col], errors='coerce', dayfirst=True)
        df_norm = df_norm.dropna(subset=['venda'])
        if df_norm.empty: 
            return pd.DataFrame(), 0

        df_norm['ec'] = df[ec_col]
        df_norm['plataforma'] = 'Bionio'
        df_norm['tipo'] = df[beneficio_col].astype(str)
        df_norm['bandeira'] = df[pagamento_col].astype(str)

        def categorize_payment_bionio(tipo_pgto):
            tipo_pgto_lower = str(tipo_pgto).strip().lower()
            if 'pix' in tipo_pgto_lower or 'transferência' in tipo_pgto_lower: return 'Pix'
            if 'cartão' in tipo_pgto_lower: return 'Crédito'
            if 'boleto' in tipo_pgto_lower: return 'Boleto'
            return 'Outros'
        
        df_norm['categoria_pagamento'] = df[pagamento_col].apply(categorize_payment_bionio)

        final_df = df_norm.dropna(subset=['bruto', 'cnpj'])
        return final_df, len(final_df)

    except KeyError as e: 
        st.error(f"Erro ao processar Bionio: {e}")
    except Exception as e: 
        st.error(f"Erro inesperado ao processar Bionio: {e}"); st.error(traceback.format_exc())
    return pd.DataFrame(), 0

@st.cache_data(show_spinner="Carregando e processando Maquininha/Veripag...")
def load_maquininha_csv(uploaded_file):
    """ Carrega o CSV/Excel da Maquininha/Veripag e normaliza. """
    df = None
    file_name = uploaded_file.name
    errors_log = [] 
    
    # Define as colunas esperadas (minúsculas, limpas)
    cnpj_col = 'cnpj'; bruto_col = 'bruto'; liquido_col = 'liquido'; venda_col = 'venda'
    ec_col = 'ec'; tipo_col = 'tipo'; bandeira_col = 'bandeira'
    
    try:
        # --- TENTATIVA 1: LER COMO CSV ---
        if file_name.lower().endswith('.csv'):
            encodings_to_try = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
            for encoding in encodings_to_try:
                try:
                    uploaded_file.seek(0)
                    df = pd.read_csv(
                        uploaded_file,
                        encoding=encoding,
                        sep=None, # Autodetecta
                        engine='python',
                        decimal=','
                    )
                    # st.write(f"Maquininha: Lido como CSV (encoding: {encoding}).")
                    break 
                except Exception as e:
                    errors_log.append(f"Falha no CSV (encoding {encoding}): {str(e)}")
                    continue
        
        # --- TENTATIVA 2: LER COMO EXCEL (FALLBACK) ---
        if df is None:
            if file_name.lower().endswith('.csv'):
                # st.write("Maquininha: Falha ao ler como CSV, tentando fallback para Excel...")
                with st.expander("Ver logs de erro do CSV"):
                    st.error("\n".join(errors_log))
            
            try:
                uploaded_file.seek(0)
                df = pd.read_excel(uploaded_file, engine='openpyxl')
                # st.write("Maquininha: Lido com sucesso como Excel.")
            except Exception as e_excel:
                errors_log.append(f"Falha no Excel: {str(e_excel)}")
                st.error(f"Erro final ao tentar ler o arquivo Maquininha. Falhou como CSV e como Excel.")
                st.error(f"Erro Excel: {e_excel}")
                st.error(traceback.format_exc())
                return pd.DataFrame(), 0

        if df is None or df.empty:
            st.warning("Maquininha: Arquivo lido, mas está vazio.")
            return pd.DataFrame(), 0
            
        # --- INÍCIO DA NORMALIZAÇÃO ---
        df.columns = (df.columns.str.strip().str.lower()
                      .str.replace(' ', '_', regex=False)
                      .str.replace('[^0-9a-zA-Z_]', '', regex=True))
        cleaned_columns = df.columns.tolist()

        expected_cols = {cnpj_col, bruto_col, liquido_col, venda_col, ec_col, tipo_col, bandeira_col}
        missing_cols = expected_cols - set(cleaned_columns)
        if missing_cols: 
            raise KeyError(f"Colunas esperadas não encontradas na Maquininha: {', '.join(missing_cols)}. Colunas disponíveis: {', '.join(cleaned_columns)}")

        df[cnpj_col] = df[cnpj_col].astype(str).fillna('N/A')

        df_norm = pd.DataFrame()
        df_norm['cnpj'] = df[cnpj_col]
        df_norm['bruto'] = pd.to_numeric(df[bruto_col], errors='coerce').fillna(0)
        df_norm['liquido'] = pd.to_numeric(df[liquido_col], errors='coerce').fillna(0)
        df_norm['receita'] = (df_norm['bruto'] - df_norm['liquido']).clip(lower=0)
        
        df_norm['venda'] = pd.to_datetime(df[venda_col], errors='coerce', dayfirst=True, format='%d/%m/%Y %H:%M:%S')
        if df_norm['venda'].isnull().all(): 
            df_norm['venda'] = pd.to_datetime(df[venda_col], errors='coerce', dayfirst=True)
        df_norm = df_norm.dropna(subset=['venda'])
        if df_norm.empty: 
            return pd.DataFrame(), 0

        df_norm['ec'] = df[ec_col]
        df_norm['plataforma'] = 'Rovema Pay'
        df_norm['tipo'] = df[tipo_col].astype(str)
        df_norm['bandeira'] = df[bandeira_col].astype(str)

        bandeira_lower = df[bandeira_col].astype(str).str.strip().str.lower()
        tipo_lower = df[tipo_col].astype(str).str.strip().str.lower()
        
        conditions = [
            bandeira_lower.str.contains('pix', na=False),
            tipo_lower == 'crédito',
            tipo_lower == 'débito'
        ]
        choices = ['Pix', 'Crédito', 'Débito']
        df_norm['categoria_pagamento'] = np.select(conditions, choices, default='Outros')

        final_df = df_norm.dropna(subset=['bruto', 'cnpj'])
        return final_df, len(final_df)

    except KeyError as e: 
        st.error(f"Erro ao processar Maquininha (coluna faltando?): {e}")
        st.error(traceback.format_exc())
    except Exception as e: 
        st.error(f"Erro inesperado ao processar Maquininha: {e}")
        st.error(traceback.format_exc())
    return pd.DataFrame(), 0

def consolidate_data(df_bionio, df_maquininha, df_eliq, df_asto):
    """ Concatena todos os DataFrames normalizados. """
    all_transactions = []
    if df_bionio is not None and not df_bionio.empty: all_transactions.append(df_bionio)
    if df_maquininha is not None and not df_maquininha.empty: all_transactions.append(df_maquininha)
    if df_eliq is not None and not df_eliq.empty: all_transactions.append(df_eliq)
    if df_asto is not None and not df_asto.empty: all_transactions.append(df_asto)

    if not all_transactions: 
        return pd.DataFrame()

    try:
        df_consolidated = pd.concat(all_transactions, ignore_index=True)
        
        cols_to_check = {
            'responsavel_comercial': 'N/A',
            'produto': df_consolidated.get('plataforma', 'N/A'),
            'plataforma': 'N/A',
            'bandeira': 'N/A',
            'tipo': 'N/A',
            'categoria_pagamento': 'N/A',
            'cnpj': 'N/A'
        }
        for col, default in cols_to_check.items():
            if col not in df_consolidated.columns:
                df_consolidated[col] = default

        df_consolidated['bruto'] = pd.to_numeric(df_consolidated['bruto'], errors='coerce').fillna(0)
        df_consolidated['receita'] = pd.to_numeric(df_consolidated['receita'], errors='coerce').fillna(0).clip(lower=0)
        df_consolidated['venda'] = pd.to_datetime(df_consolidated['venda'], errors='coerce')
        
        df_consolidated['cnpj'] = df_consolidated['cnpj'].astype(str).fillna('N/A')
        
        df_consolidated = df_consolidated.dropna(subset=['venda'])
        
        for col in ['plataforma', 'bandeira', 'tipo', 'categoria_pagamento']:
             df_consolidated[col] = df_consolidated[col].astype(str).fillna('N/A')

        return df_consolidated
    
    except Exception as e:
        st.error(f"Erro durante a concatenação dos dados: {e}")
        st.error(traceback.format_exc())
        return pd.DataFrame()

def generate_insights(df_filtered, total_gmv, receita_total):
    """Gera insights automáticos."""
    insights = []
    if df_filtered.empty: 
        return [{"label": "Status", "value": "Nenhum dado encontrado para os filtros selecionados."}]
    
    if 'ec' in df_filtered.columns:
        df_gmv_cliente = df_filtered.groupby('ec')['bruto'].sum().nlargest(1).reset_index()
        if not df_gmv_cliente.empty:
            insights.append({
                "label": "Cliente Principal (GMV)",
                "value": f"**{df_gmv_cliente.iloc[0]['ec']}** (R$ {df_gmv_cliente.iloc[0]['bruto']:,.2f})"
            })
            
    if 'plataforma' in df_filtered.columns:
        df_plataforma = df_filtered.groupby('plataforma')['bruto'].sum().nlargest(1).reset_index()
        if not df_plataforma.empty:
            insights.append({
                "label": "Plataforma Principal (GMV)",
                "value": f"**{df_plataforma.iloc[0]['plataforma']}** (R$ {df_plataforma.iloc[0]['bruto']:,.2f})"
            })
            
    margem = (receita_total / total_gmv) * 100 if total_gmv > 0 else 0
    insights.append({"label": "Margem Média no Período", "value": f"**{margem:,.2f}%**"})

    if 'categoria_pagamento' in df_filtered.columns:
        top_cat = df_filtered['categoria_pagamento'].mode()
        if not top_cat.empty:
            insights.append({"label": "Categoria Pag. Mais Frequente", "value": f"**{top_cat.iloc[0]}**"})
            
    return insights

# --- Inicialização do Session State ---
if 'df_consolidated' not in st.session_state:
    st.session_state.df_consolidated = pd.DataFrame()
if 'load_summary' not in st.session_state:
    st.session_state.load_summary = {}
if 'date_filter_selection' not in st.session_state:
     st.session_state.date_filter_selection = (None, None)

# --- Interface Streamlit ---
st.title("💰 Dashboard Consolidado de Transações")

# --- Barra Lateral ---
with st.sidebar:
    st.header("Fontes de Dados")
    uploaded_bionio = st.file_uploader("1. Arquivo Bionio (.csv)", type=['csv'], key="bionio_upload")
    uploaded_maquininha = st.file_uploader("2. Arquivo Maquininha/Veripag (.csv ou .xlsx)", type=['csv', 'xlsx'], key="maquininha_upload")
    
    st.markdown("---")
    st.header("Configuração das APIs")
    today = datetime.now().date()
    default_start_date = today - timedelta(days=30)
    
    api_start_date, api_end_date = st.date_input(
        "Período para APIs", value=(default_start_date, today),
        min_value=date(2020, 1, 1), max_value=today + timedelta(days=1),
        key="api_date_range"
    )
    st.info("Credenciais das APIs são carregadas de 'secrets.toml'", icon="ℹ️")
    
    load_button_pressed = st.button("Carregar e Processar Dados", key="load_data_button", type="primary")

# --- Carregamento e Processamento Principal ---
if load_button_pressed:
    st.session_state.df_consolidated = pd.DataFrame()
    st.session_state.load_summary = {}
    st.session_state.date_filter_selection = (None, None)
    summary = {}
    
    with st.spinner("Processando fontes de dados... Por favor, aguarde."):
        df_bionio_processed, count_bionio = load_bionio_csv(uploaded_bionio) if uploaded_bionio else (pd.DataFrame(), 0)
        summary['bionio'] = f"{count_bionio} registros"
        
        df_maquininha_processed, count_maq = load_maquininha_csv(uploaded_maquininha) if uploaded_maquininha else (pd.DataFrame(), 0)
        summary['maquininha'] = f"{count_maq} registros"
        
        try:
            if 'eliq_api_token' in st.secrets:
                df_eliq_fetched, pages_eliq, count_eliq = fetch_eliq_data(st.secrets["eliq_api_token"], api_start_date, api_end_date)
                summary['eliq'] = f"{count_eliq} registros (de {pages_eliq} págs)"
            else: 
                st.sidebar.warning("Token API Eliq não encontrado.", icon="🔑")
                summary['eliq'] = "Token não configurado"
        except KeyError: 
            st.sidebar.error("Erro: 'eliq_api_token' não encontrado em secrets.toml.", icon="❌")
            summary['eliq'] = "Erro de chave (secret)"
        except Exception as e: 
            st.sidebar.error(f"Erro inesperado API Eliq: {e}", icon="❌")
            summary['eliq'] = f"Erro: {e}"

        try:
            if 'asto_username' in st.secrets and 'asto_password' in st.secrets:
                df_asto_fetched, count_asto = fetch_asto_data(st.secrets["asto_username"], st.secrets["asto_password"], api_start_date, api_end_date)
                summary['asto'] = f"{count_asto} registros (Placeholder)"
            else: 
                st.sidebar.warning("Credenciais API Asto não encontradas.", icon="🔑")
                summary['asto'] = "Credenciais não configuradas"
        except KeyError: 
            st.sidebar.error("Erro: Credenciais Asto não encontradas em secrets.toml.", icon="❌")
            summary['asto'] = "Erro de chave (secret)"
        except Exception as e: 
            st.sidebar.error(f"Erro inesperado API Asto: {e}", icon="❌")
            summary['asto'] = f"Erro: {e}"

        df_consolidated_loaded = consolidate_data(
            df_bionio_processed, df_maquininha_processed,
            df_eliq_fetched, df_asto_fetched
        )
        st.session_state.df_consolidated = df_consolidated_loaded
        st.session_state.load_summary = summary
        
        if not df_consolidated_loaded.empty:
            st.success(f"Processamento concluído! {len(df_consolidated_loaded)} registros totais consolidados.", icon="✅")
        else:
            st.warning("Processamento concluído, mas nenhum dado foi carregado.", icon="⚠️")

# Recupera o DataFrame do estado da sessão
df_consolidated = st.session_state.df_consolidated

# --- Exibe Resumo do Carregamento (se existir) ---
if st.session_state.load_summary:
    with st.expander("Resumo do Carregamento", expanded=True):
        cols = st.columns(4)
        cols[0].metric("Bionio (CSV)", st.session_state.load_summary.get('bionio', 'N/A'))
        cols[1].metric("Maquininha (CSV/Excel)", st.session_state.load_summary.get('maquininha', 'N/A'))
        cols[2].metric("Eliq/Sygio (API)", st.session_state.load_summary.get('eliq', 'N/A'))
        cols[3].metric("Asto/Logpay (API)", st.session_state.load_summary.get('asto', 'N/A'))

# --- Dashboard Principal ---
if df_consolidated.empty:
    if load_button_pressed:
        st.error("Carregamento falhou ou não retornou dados. Verifique os logs e as fontes.")
    else:
        st.info("Faça upload dos arquivos (se necessário) e clique em 'Carregar e Processar Dados' na barra lateral.", icon="ℹ️")
else:
    # --- FILTROS ---
    with st.expander("Filtros de Análise", expanded=True):
        col_date, col_plataforma, col_bandeira, col_tipo, col_categoria_pgto = st.columns([1.5, 1.5, 1, 1, 1.5])
        
        df_filtered = df_consolidated.copy()
        
        with col_date:
            data_min = df_consolidated['venda'].min().date()
            data_max = df_consolidated['venda'].max().date()
            date_key = 'date_filter_selection'

            valid_state = False
            if (date_key in st.session_state and
                isinstance(st.session_state[date_key], (list, tuple)) and
                len(st.session_state[date_key]) == 2):
                
                saved_start, saved_end = st.session_state[date_key]
                
                if (saved_start is not None and saved_end is not None and
                    saved_start >= data_min and saved_end <= data_max and
                    saved_start <= saved_end):
                    valid_state = True
            
            if not valid_state:
                st.session_state[date_key] = (data_min, data_max)

            data_inicial, data_final = st.date_input(
                "Período",
                min_value=data_min,
                max_value=data_max,
                key=date_key
            )
            
        with col_plataforma:
            plataformas = ['Todos'] + sorted(df_filtered['plataforma'].unique().tolist())
            filtro_plataforma = st.selectbox("Plataforma/Produto", options=plataformas)
            
        with col_bandeira:
            bandeiras = ['Todos'] + sorted(df_filtered['bandeira'].unique().tolist())
            filtro_bandeira = st.selectbox("Bandeira (Detalhe)", options=bandeiras)
            
        with col_tipo:
            tipos = ['Todos'] + sorted(df_filtered['tipo'].unique().tolist())
            filtro_tipo = st.selectbox("Tipo (Detalhe)", options=tipos)
            
        with col_categoria_pgto:
            categorias = ['Todos'] + sorted(df_filtered['categoria_pagamento'].unique().tolist())
            filtro_categoria = st.selectbox("Categoria Pagamento", options=categorias)

    # --- Aplicação dos Filtros ---
    if data_inicial and data_final and (data_inicial <= data_final):
        mask_date = (df_filtered['venda'].dt.date >= data_inicial) & (df_filtered['venda'].dt.date <= data_final)
        df_filtered = df_filtered[mask_date]
    elif data_inicial > data_final:
        st.warning("Data inicial não pode ser posterior à data final. Nenhum dado será exibido.")
        df_filtered = pd.DataFrame(columns=df_consolidated.columns)

    if filtro_plataforma != 'Todos': 
        df_filtered = df_filtered[df_filtered['plataforma'] == filtro_plataforma]
    if filtro_bandeira != 'Todos': 
        df_filtered = df_filtered[df_filtered['bandeira'] == filtro_bandeira]
    if filtro_tipo != 'Todos': 
        df_filtered = df_filtered[df_filtered['tipo'] == filtro_tipo]
    if filtro_categoria != 'Todos': 
        df_filtered = df_filtered[df_filtered['categoria_pagamento'] == filtro_categoria]


    # --- KPIs ---
    if not df_filtered.empty:
        total_gmv = df_filtered['bruto'].sum()
        receita_total = df_filtered['receita'].sum()
        clientes_ativos = df_filtered['cnpj'].nunique()
        margem_media = (receita_total / total_gmv) * 100 if total_gmv > 0 else 0
        gmv_por_cliente = df_filtered.groupby('cnpj')['bruto'].sum()
        clientes_em_queda_proxy = gmv_por_cliente[gmv_por_cliente < 1000].count()
    else:
        total_gmv, receita_total, clientes_ativos, margem_media, clientes_em_queda_proxy = 0, 0, 0, 0, 0

    # --- CORREÇÃO DO SYNTAX ERROR ---
    # A linha abaixo estava com '...' que é um erro de sintaxe
    col1, col2, col3, col4, col5 = st.columns(5)
    # --- FIM DA CORREÇÃO ---
    
    col1.metric("Transacionado (Bruto)", f"R$ {total_gmv:,.2f}")
    col2.metric("Nossa Receita", f"R$ {receita_total:,.2f}")
    col3.metric("Margem Média", f"{margem_media:,.2f}%")
    col4.metric("Clientes Ativos", f"{clientes_ativos:,}")
    col5.metric("Clientes em Queda (Proxy < R$1k)", f"{clientes_em_queda_proxy:,}")
    st.markdown("---")
    
    # --- Seção de Gráficos ---
    if df_filtered.empty:
        st.info("Nenhum dado encontrado para os filtros selecionados.")
    else:
        # --- Gráfico de Evolução ---
        st.subheader("Evolução do Valor Transacionado vs Receita")
        df_evolucao = df_filtered.groupby(df_filtered['venda'].dt.date).agg(GMV=('bruto', 'sum'), Receita=('receita', 'sum')).reset_index().rename(columns={'venda': 'Data da Venda'})
        if not df_evolucao.empty:
            fig_evolucao = go.Figure()
            fig_evolucao.add_trace(go.Scatter(x=df_evolucao['Data da Venda'], y=df_evolucao['GMV'], mode='lines+markers', name='Valor Bruto', line=dict(color='blue', width=2), marker=dict(size=5)))
            fig_evolucao.add_trace(go.Scatter(x=df_evolucao['Data da Venda'], y=df_evolucao['Receita'], mode='lines+markers', name='Receita', line=dict(color='red', width=2), marker=dict(size=5)))
            fig_evolucao.update_layout(xaxis_title='Data', yaxis_title='Valor (R$)', hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), template="plotly_white", margin=dict(t=20))
            st.plotly_chart(fig_evolucao, use_container_width=True)
        else: 
            st.info("Nenhum dado agregado para gráfico de evolução.")
        st.markdown("---")

        # --- Gráficos Inferiores ---
        col6, col7 = st.columns([1.5, 1])
        with col6:
            st.subheader("Receita por Plataforma/Produto")
            df_receita_plataforma = df_filtered.groupby('plataforma')['receita'].sum().reset_index().sort_values(by='receita', ascending=False)
            if not df_receita_plataforma.empty and df_receita_plataforma['receita'].sum() > 0:
                fig_receita_plat = px.bar(df_receita_plataforma, x='plataforma', y='receita', labels={'plataforma': 'Plataforma', 'receita': 'Receita (R$)'}, color='plataforma', color_discrete_sequence=px.colors.qualitative.Plotly, title='Receita Gerada por Plataforma', text='receita')
                fig_receita_plat.update_traces(texttemplate='R$ %{y:,.2f}', textposition='outside')
                fig_receita_plat.update_layout(uniformtext_minsize=8, uniformtext_mode='hide', template="plotly_white", xaxis_title=None, showlegend=False, margin=dict(t=30))
                st.plotly_chart(fig_receita_plat, use_container_width=True)
            else: 
                st.info("Nenhuma receita registrada para gráfico por plataforma.")

        with col7:
            st.subheader("Participação por Categoria de Pagamento")
            df_categoria_pgto = df_filtered.groupby('categoria_pagamento')['bruto'].sum().reset_index().sort_values(by='bruto', ascending=False)
            if not df_categoria_pgto.empty and df_categoria_pgto['bruto'].astype(float).sum() > 0:
                pull_values = [0] * len(df_categoria_pgto); pull_values[0] = 0.1
                fig_categoria = px.pie(df_categoria_pgto, values='bruto', names='categoria_pagamento', title='Participação do GMV por Cat. Pagamento', color_discrete_sequence=px.colors.qualitative.Safe)
                fig_categoria.update_traces(textinfo='percent+label', pull=pull_values, marker=dict(line=dict(color='#000000', width=1)))
                fig_categoria.update_layout(legend_title_text='Categoria', margin=dict(t=30))
                st.plotly_chart(fig_categoria, use_container_width=True)
            else: 
                st.info("Nenhum valor bruto positivo para gráfico de pizza.")
        st.markdown("---")

        # --- Detalhamento por Cliente ---
        st.subheader("🔍 Detalhamento por Cliente")
        def get_most_frequent(series):
            if series.empty or series.mode().empty: return 'N/A'
            return series.mode().iloc[0]

        df_detalhe_cliente = df_filtered.groupby(['cnpj', 'ec']).agg(
            Receita=('receita', 'sum'), 
            N_Vendas=('cnpj', 'count'),
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
        st.dataframe(df_display, hide_index=True, use_container_width=True)
        
        st.markdown(f"**Mostrando {len(df_display)} clientes**")
        st.markdown("---")

        # --- Insights ---
        st.subheader("💡 Insights Automáticos")
        insights_list = generate_insights(df_filtered, total_gmv, receita_total)
        cols_insights = st.columns(len(insights_list) if insights_list else 1) 
        
        for i, insight_data in enumerate(insights_list):
            with cols_insights[i]:
                with st.container(border=True):
                    st.markdown(f"<small>{insight_data['label']}</small>", unsafe_allow_html=True)
                    st.markdown(f"{insight_data['value']}")

        st.markdown("---")

        # --- Tabela Detalhada (Rodapé) ---
        with st.expander("Visualizar Todos os Dados Filtrados (Detalhados)"):
            df_filtered_display = df_filtered.copy()
            df_filtered_display['cnpj'] = df_filtered_display['cnpj'].astype(str)
            
            st.dataframe(df_filtered_display, use_container_width=True, hide_index=True)
            
            csv_data_filtered = df_filtered.to_csv(index=False).encode('utf-8')
            st.download_button("Exportar CSV (Dados Filtrados)", csv_data_filtered, 'detalhamento_filtrado.csv', 'text/csv', key='dl-csv-filt')
