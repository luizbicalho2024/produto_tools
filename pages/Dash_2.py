import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import io
import numpy as np
import requests # Necessário para chamadas de API
from datetime import datetime, timedelta, date # Necessário para datas nas APIs e min_value

# --- Configurações de Aparência ---
st.set_page_config(
    page_title="Dashboard Consolidado - Transações",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Funções de Chamada de API ---

@st.cache_data(show_spinner="Buscando dados da API Eliq/UzziPay...")
def fetch_eliq_data(api_token, start_date, end_date):
    """ Busca dados da API Eliq/UzziPay e normaliza. """
    base_url = "https://sigyo.uzzipay.com/api/transacoes"
    headers = {'Authorization': f'Bearer {api_token}'}

    # Formatando as datas para a API (ajuste o formato se necessário)
    start_str = start_date.strftime('%d/%m/%Y')
    end_str = end_date.strftime('%d/%m/%Y')
    params = {'TransacaoSearch[data_cadastro]': f'{start_str} - {end_str}'}

    all_data = []
    page = 1
    max_pages = 50 # Limite para evitar loops infinitos

    try:
        while page <= max_pages:
            params['page'] = page
            response = requests.get(base_url, headers=headers, params=params, timeout=30)
            response.raise_for_status() # Levanta erro para status HTTP ruins (4xx, 5xx)
            data = response.json()

            if isinstance(data, list) and data: # Verifica se a resposta é uma lista não vazia
                 all_data.extend(data)
                 # Se a API não retornar informações de paginação explícitas,
                 # podemos assumir que uma página vazia ou menor que um limite significa o fim.
                 # Neste exemplo, vamos apenas buscar algumas páginas como demonstração.
                 # Uma API real teria 'next_page', 'total_pages', etc.
                 if len(data) < 50: # Suposição: páginas têm tamanho 50
                     break
                 page += 1
            else:
                 # Se a resposta não for uma lista ou estiver vazia na primeira página
                 if page == 1 and not data:
                     st.warning("API Eliq: Nenhum dado retornado para o período selecionado.", icon="⚠️")
                 break # Sai do loop se não houver mais dados ou formato inesperado

        if not all_data:
            return pd.DataFrame()

        df = pd.json_normalize(all_data) # Transforma o JSON (lista de dicts) em DataFrame

        # Normalização (baseado na análise anterior e no código original)
        df_norm = pd.DataFrame()
        # Mapeamento cuidadoso - Verifique os nomes exatos das colunas no seu JSON
        df_norm['cnpj'] = df.get('cliente_cnpj', pd.NA)
        # Tentar encontrar o valor bruto - pode ser 'valor_total' ou outro campo
        df_norm['bruto'] = pd.to_numeric(df.get('valor_total', 0), errors='coerce')
        # Calcular receita - pode ser uma taxa específica ou diferença
        # Usando cliente_taxa_adm como proxy, assumindo que é um percentual
        taxa_cliente_percent = pd.to_numeric(df.get('cliente_taxa_adm', 0), errors='coerce') / 100
        df_norm['receita'] = df_norm['bruto'] * taxa_cliente_percent # Cálculo de receita exemplo
        df_norm['venda'] = pd.to_datetime(df.get('data_cadastro', pd.NaT), errors='coerce', dayfirst=True) # Ajuste dayfirst=True se o formato for DD/MM/YYYY
        df_norm['ec'] = df.get('cliente_nome', 'N/A')
        df_norm['plataforma'] = 'Eliq'
        df_norm['tipo'] = df.get('tipo_transacao_sigla', 'N/A').astype(str) # Ex: ABA, SRV
        df_norm['bandeira'] = df.get('bandeira', 'N/A').astype(str) # Ex: Mastercard, Visa (se disponível)
        df_norm['categoria_pagamento'] = 'Outros' # Eliq geralmente não se encaixa em Pix/Cred/Deb

        return df_norm.dropna(subset=['venda', 'bruto', 'cnpj'])

    except requests.exceptions.RequestException as e:
        st.error(f"Erro ao buscar dados da API Eliq: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro ao processar dados da API Eliq: {e}")
        return pd.DataFrame()


@st.cache_data(show_spinner="Buscando dados da API Asto/Logpay (Limitado)...")
def fetch_asto_data(api_username, api_password, start_date, end_date):
    """
    Placeholder/Tentativa para buscar dados da API Asto/Logpay.
    AVISO: Esta API não é ideal para extração em massa. Esta função pode
    retornar dados limitados ou vazios.
    """
    st.warning("A API Asto/Logpay não é otimizada para relatórios completos. "
               "A extração de dados pode ser incompleta ou lenta.", icon="⚠️")

    # --- INÍCIO DO PLACEHOLDER ---
    # Uma implementação real exigiria:
    # 1. Autenticação (obter um token ou usar Basic Auth se suportado)
    #    - Ex: auth = (api_username, api_password)
    # 2. Obter lista de TODOS os cliente_id ou establishment_id (outra chamada de API?)
    # 3. Iterar por cada cliente/estabelecimento E por cada dia no período
    # 4. Chamar endpoints como /api/Ticket/{clienteID}/{dia}/{mes}/{ano} ou /api/appmanutencao/osfinished/...
    # 5. Consolidar e normalizar CADA resposta.
    # Isso seria MUITO lento e faria milhares de chamadas.

    # Exemplo simplificado (apenas retorna vazio por enquanto):
    try:
        # TENTATIVA HIPOTÉTICA (PRECISA SER AJUSTADA À API REAL)
        # Exemplo: Tentar pegar tickets de um cliente específico (se ID fosse conhecido)
        # base_url = "https://services.host.logpay.com.br/api"
        # cliente_id_exemplo = 123 # PRECISA SER REAL
        # dia = start_date.day
        # mes = start_date.month
        # ano = start_date.year
        # response = requests.get(f"{base_url}/Ticket/{cliente_id_exemplo}/{dia}/{mes}/{ano}",
        #                         auth=(api_username, api_password), timeout=15) # Ou outra auth
        # response.raise_for_status()
        # data = response.json()
        # df = pd.json_normalize(data)
        # Normalização...
        pass # Ignora por enquanto

    except requests.exceptions.RequestException as e:
        st.error(f"Erro (exemplo) ao tentar conectar à API Asto: {e}")
        return pd.DataFrame()
    except Exception as e:
         st.error(f"Erro (exemplo) ao processar dados da Asto: {e}")
         return pd.DataFrame()

    # Retorna DataFrame vazio como placeholder
    df_norm_placeholder = pd.DataFrame(columns=['cnpj', 'bruto', 'receita', 'venda', 'ec', 'plataforma', 'tipo', 'bandeira', 'categoria_pagamento'])
    return df_norm_placeholder
    # --- FIM DO PLACEHOLDER ---


# --- Funções de Carregamento de CSV ---

@st.cache_data(show_spinner="Carregando e processando Bionio...")
def load_bionio_csv(uploaded_file):
    """ Carrega o CSV do Bionio e normaliza. """
    try:
        # Tenta ler com diferentes encodings comuns
        df = None
        for encoding in ['utf-8', 'latin1', 'cp1252']:
            try:
                uploaded_file.seek(0) # Volta ao início do arquivo para cada tentativa
                df = pd.read_csv(uploaded_file, encoding=encoding, sep=None, engine='python')
                break
            except Exception:
                continue
        if df is None:
            raise ValueError("Não foi possível ler o arquivo Bionio com encodings comuns.")

        # Limpar nomes das colunas (minúsculas, sem espaços extras)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        # Normalização
        df_norm = pd.DataFrame()
        df_norm['cnpj'] = df['cnpj_da_organização'] # Verificar nome exato após limpeza
        # Limpar e converter valor bruto
        df_norm['bruto'] = df['valor_total_do_pedido'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df_norm['bruto'] = pd.to_numeric(df_norm['bruto'], errors='coerce')
        # Estimar receita (usando 5% como no código original)
        df_norm['receita'] = df_norm['bruto'] * 0.05
        df_norm['venda'] = pd.to_datetime(df['data_da_criação_do_pedido'], errors='coerce', dayfirst=True) # Ajuste dayfirst
        df_norm['ec'] = df['razão_social'] # Verificar nome exato
        df_norm['plataforma'] = 'Bionio'
        df_norm['tipo'] = df['nome_do_benefício'].astype(str)
        df_norm['bandeira'] = df['tipo_de_pagamento'].astype(str)

        # Lógica de Categoria de Pagamento (mantida do original)
        def categorize_payment_bionio(tipo_pgto):
            tipo_pgto_lower = str(tipo_pgto).strip().lower()
            if 'pix' in tipo_pgto_lower or 'transferência' in tipo_pgto_lower: # Inclui Pix
                 return 'Pix'
            if 'cartão' in tipo_pgto_lower: # Pode precisar refinar (Crédito vs Débito?)
                 return 'Crédito' # Suposição
            if 'boleto' in tipo_pgto_lower:
                return 'Boleto'
            return 'Outros'
        df_norm['categoria_pagamento'] = df['tipo_de_pagamento'].apply(categorize_payment_bionio)

        return df_norm.dropna(subset=['venda', 'bruto', 'cnpj'])

    except Exception as e:
        st.error(f"Erro ao processar arquivo Bionio: {e}")
        return pd.DataFrame()

@st.cache_data(show_spinner="Carregando e processando Maquininha/Veripag...")
def load_maquininha_csv(uploaded_file):
    """ Carrega o CSV da Maquininha/Veripag e normaliza. """
    try:
        df = None
        for encoding in ['utf-8', 'latin1', 'cp1252']:
            try:
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file, encoding=encoding, sep=None, engine='python')
                break
            except Exception:
                continue
        if df is None:
             raise ValueError("Não foi possível ler o arquivo Maquininha com encodings comuns.")

        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        # Normalização
        df_norm = pd.DataFrame()
        df_norm['cnpj'] = df['cnpj'] # Verificar nome exato
        df_norm['bruto'] = pd.to_numeric(df['bruto'], errors='coerce')
        # Calcular receita (Bruto - Líquido parece mais preciso aqui)
        df_norm['liquido'] = pd.to_numeric(df['liquido'], errors='coerce')
        df_norm['receita'] = df_norm['bruto'] - df_norm['liquido']
        # Ajustar formato da data se necessário antes da conversão
        df_norm['venda'] = pd.to_datetime(df['venda'], errors='coerce', dayfirst=True) # Ajuste dayfirst
        df_norm['ec'] = df['ec'] # Verificar nome exato
        df_norm['plataforma'] = 'Rovema Pay' # Ou Veripag, conforme preferir
        df_norm['tipo'] = df['tipo'].astype(str)
        df_norm['bandeira'] = df['bandeira'].astype(str)

        # Lógica de Categoria de Pagamento (mantida do original, ajustada)
        def categorize_payment_rp(row):
            bandeira_lower = str(row.get('bandeira', '')).strip().lower()
            tipo_lower = str(row.get('tipo', '')).strip().lower()

            if 'pix' in bandeira_lower: # Verifica se 'pix' está na bandeira
                return 'Pix'
            if tipo_lower == 'crédito':
                return 'Crédito'
            if tipo_lower == 'débito':
                return 'Débito'
            return 'Outros'
        df_norm['categoria_pagamento'] = df.apply(categorize_payment_rp, axis=1)

        return df_norm.dropna(subset=['venda', 'bruto', 'cnpj'])

    except Exception as e:
        st.error(f"Erro ao processar arquivo Maquininha: {e}")
        return pd.DataFrame()


# --- Função Principal de Consolidação ---

def consolidate_data(df_bionio, df_maquininha, df_eliq, df_asto):
    """ Concatena todos os DataFrames normalizados. """
    all_transactions = []
    if not df_bionio.empty:
        all_transactions.append(df_bionio)
    if not df_maquininha.empty:
        all_transactions.append(df_maquininha)
    if not df_eliq.empty:
        all_transactions.append(df_eliq)
    if not df_asto.empty:
        all_transactions.append(df_asto)

    if not all_transactions:
        st.error("Nenhuma fonte de dados válida foi carregada ou processada.")
        return pd.DataFrame()

    df_consolidated = pd.concat(all_transactions, ignore_index=True)

    # --- REMOVIDO: Merge com Carteira de Clientes ---
    # Se você tiver uma fonte para 'responsavel_comercial' (ex: outro CSV ou API),
    # o merge seria feito aqui. Por enquanto, essas colunas não existirão.
    # df_merged = pd.merge(df_consolidated, df_clientes[['cnpj', 'responsavel_comercial', 'produto']], on='cnpj', how='left')
    # df_merged['responsavel_comercial'] = df_merged['responsavel_comercial'].fillna('Não Atribuído')
    # df_merged['produto'] = df_merged['produto'].fillna(df_merged['plataforma']) # Usa plataforma se produto não encontrado

    # Adiciona colunas placeholder se não existirem (para evitar erros nos gráficos/insights)
    if 'responsavel_comercial' not in df_consolidated.columns:
        df_consolidated['responsavel_comercial'] = 'N/A'
    if 'produto' not in df_consolidated.columns:
         df_consolidated['produto'] = df_consolidated['plataforma'] # Usa plataforma como produto

    st.success(f"Dados de {len(all_transactions)} fontes consolidados com sucesso!", icon="✅")
    return df_consolidated


# --- Função de Insights (AJUSTADA sem Responsável Comercial direto) ---
def generate_insights(df_filtered, total_gmv, receita_total):
    """Gera insights automáticos baseados nos dados filtrados."""
    insights = []

    if df_filtered.empty:
        return ["Nenhum dado encontrado para o período/filtros selecionados."]

    # Insight 1: Cliente com maior GMV
    if 'ec' in df_filtered.columns:
        df_gmv_cliente = df_filtered.groupby('ec')['bruto'].sum().nlargest(1).reset_index()
        if not df_gmv_cliente.empty:
            top_cliente = df_gmv_cliente.iloc[0]
            insights.append(f"O cliente **{top_cliente['ec']}** é o principal motor de vendas, com um GMV de **R$ {top_cliente['bruto']:,.2f}**.")

    # Insight 2: Plataforma/Produto com maior GMV
    if 'plataforma' in df_filtered.columns:
         df_plataforma = df_filtered.groupby('plataforma')['bruto'].sum().nlargest(1).reset_index()
         if not df_plataforma.empty:
            top_plataforma = df_plataforma.iloc[0]
            insights.append(f"A plataforma **{top_plataforma['plataforma']}** é a mais rentável por GMV, com **R$ {top_plataforma['bruto']:,.2f}**.")

    # Insight 3: Margem média
    margem_media = (receita_total / total_gmv) * 100 if total_gmv > 0 else 0
    insights.append(f"A margem média de Receita sobre o GMV no período é de **{margem_media:,.2f}%**.")

    # Insight 4: Categoria de Pagamento Dominante
    if 'categoria_pagamento' in df_filtered.columns and not df_filtered.empty:
        top_categoria = df_filtered['categoria_pagamento'].mode()
        if not top_categoria.empty:
             insights.append(f"A Categoria de Pagamento mais utilizada por GMV é **{top_categoria.iloc[0]}**.")


    return insights


# --- Interface Streamlit ---

st.title("💰 Dashboard Consolidado de Transações")

# --- Área de Upload e Config API (Barra Lateral) ---
with st.sidebar:
    st.header("Fontes de Dados")

    # Uploads
    uploaded_bionio = st.file_uploader("1. Arquivo Bionio (.csv)", type=['csv'], key="bionio_upload")
    uploaded_maquininha = st.file_uploader("2. Arquivo Maquininha/Veripag (.csv)", type=['csv'], key="maquininha_upload")

    st.markdown("---")
    st.header("Configuração das APIs")

    # Busca de datas padrão (últimos 30 dias)
    today = datetime.now().date()
    default_start_date = today - timedelta(days=30)
    default_end_date = today

    api_start_date, api_end_date = st.date_input(
         "Período para APIs",
         value=(default_start_date, default_end_date),
         min_value=date(2020, 1, 1), # Data mínima razoável
         max_value=today + timedelta(days=1), # Data máxima razoável
         key="api_date_range"
     )

    st.info("Credenciais das APIs são carregadas de 'secrets.toml'", icon="ℹ️")

    # Botão para carregar/recarregar dados
    if st.button("Carregar e Processar Dados", key="load_data_button"):
        st.session_state.data_loaded = True # Flag para indicar que o carregamento foi iniciado
    else:
        # Mantém o estado se já foi carregado antes
        if 'data_loaded' not in st.session_state:
            st.session_state.data_loaded = False


# --- Carregamento e Processamento Principal ---
df_consolidated = pd.DataFrame()

# Só executa o carregamento se o botão foi pressionado
if st.session_state.data_loaded:
    # 1. Carregar CSVs (se houver upload)
    df_bionio_processed = pd.DataFrame()
    if uploaded_bionio:
        df_bionio_processed = load_bionio_csv(uploaded_bionio)

    df_maquininha_processed = pd.DataFrame()
    if uploaded_maquininha:
        df_maquininha_processed = load_maquininha_csv(uploaded_maquininha)

    # 2. Buscar Dados das APIs (usando secrets)
    df_eliq_fetched = pd.DataFrame()
    df_asto_fetched = pd.DataFrame()
    try:
        # Eliq API Call
        if 'eliq_api_token' in st.secrets:
            df_eliq_fetched = fetch_eliq_data(st.secrets["eliq_api_token"], api_start_date, api_end_date)
        else:
            st.sidebar.warning("Token da API Eliq não encontrado nos secrets.", icon="🔑")

        # Asto API Call (Placeholder)
        if 'asto_username' in st.secrets and 'asto_password' in st.secrets:
             df_asto_fetched = fetch_asto_data(st.secrets["asto_username"], st.secrets["asto_password"], api_start_date, api_end_date)
        else:
             st.sidebar.warning("Credenciais da API Asto não encontradas nos secrets.", icon="🔑")

    except KeyError as e:
         st.sidebar.error(f"Erro: Chave '{e}' não encontrada no arquivo secrets.toml.", icon="❌")
    except Exception as e:
        st.sidebar.error(f"Erro inesperado durante a chamada das APIs: {e}", icon="❌")


    # 3. Consolidar todos os dados
    df_consolidated = consolidate_data(
        df_bionio_processed,
        df_maquininha_processed,
        df_eliq_fetched,
        df_asto_fetched
    )
    # Armazena os dados consolidados no estado da sessão para persistir entre interações
    st.session_state.df_consolidated = df_consolidated
else:
    # Tenta carregar do estado da sessão se já foi carregado antes
    if 'df_consolidated' in st.session_state:
        df_consolidated = st.session_state.df_consolidated


# --- Dashboard Principal (continua daqui se df_consolidated não estiver vazio) ---

if df_consolidated.empty or 'bruto' not in df_consolidated.columns:
     st.warning("Por favor, faça o upload dos arquivos CSV e clique em 'Carregar e Processar Dados' na barra lateral.", icon="⚠️")
else:
    # --- FILTROS (Após o Título) ---
    st.subheader("Filtros de Análise")

    # Organiza os filtros em colunas
    # Removido filtro de vendedor, adicionado filtro de categoria_pagamento
    col_date, col_plataforma, col_bandeira, col_tipo, col_categoria_pgto = st.columns([1.5, 1.5, 1, 1, 1.5])

    with col_date:
        data_min = df_consolidated['venda'].min().date()
        data_max = df_consolidated['venda'].max().date()
        # Garante que data_inicial e data_final sejam válidos
        val_inicial = max(data_min, data_inicial) if 'data_inicial' in locals() else data_min
        val_final = min(data_max, data_final) if 'data_final' in locals() else data_max
        if val_inicial > val_final: val_inicial = val_final # Evita erro

        data_inicial, data_final = st.date_input(
            "Período",
            value=(val_inicial, val_final),
            min_value=data_min,
            max_value=data_max
        )

    with col_plataforma:
        plataformas = ['Todos'] + sorted(df_consolidated['plataforma'].unique().tolist())
        filtro_plataforma = st.selectbox("Plataforma/Produto", options=plataformas)

    # Removido filtro de vendedor (responsavel_comercial)
    # with col_vendedor:
    #     vendedores = ['Todas'] + sorted(df_consolidated['responsavel_comercial'].unique().tolist())
    #     filtro_vendedor = st.selectbox("Carteira", options=vendedores)

    with col_bandeira:
        bandeiras = ['Todos'] + sorted(df_consolidated['bandeira'].astype(str).unique().tolist())
        filtro_bandeira = st.selectbox("Bandeira (Detalhe)", options=bandeiras)

    with col_tipo:
        tipos = ['Todos'] + sorted(df_consolidated['tipo'].astype(str).unique().tolist())
        filtro_tipo = st.selectbox("Tipo (Detalhe)", options=tipos)

    with col_categoria_pgto:
        categorias = ['Todos'] + sorted(df_consolidated['categoria_pagamento'].unique().tolist())
        filtro_categoria = st.selectbox("Categoria Pagamento", options=categorias)


    # --- Aplicação dos Filtros ---
    df_filtered = df_consolidated[
        (df_consolidated['venda'].dt.date >= data_inicial) &
        (df_consolidated['venda'].dt.date <= data_final)
    ].copy()

    if filtro_plataforma != 'Todos':
         df_filtered = df_filtered[df_filtered['plataforma'] == filtro_plataforma]

    # if filtro_vendedor != 'Todas':
    #      df_filtered = df_filtered[df_filtered['responsavel_comercial'] == filtro_vendedor]

    if filtro_bandeira != 'Todos':
         df_filtered = df_filtered[df_filtered['bandeira'].astype(str) == filtro_bandeira]

    if filtro_tipo != 'Todos':
         df_filtered = df_filtered[df_filtered['tipo'].astype(str) == filtro_tipo]

    if filtro_categoria != 'Todos':
        df_filtered = df_filtered[df_filtered['categoria_pagamento'] == filtro_categoria]


    # --- Restante do código do Dashboard (KPIs, Gráficos, Tabelas) ---
    # (Copiar e colar o restante do seu código original a partir daqui,
    #  ajustando se necessário, como a remoção do gráfico de receita por vendedor)

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
            .st-emotion-cache-1r6r0z6 {color: #262730;} /* Cor do texto dentro do card */
            .st-emotion-cache-1s0k10z { /* O container do card */
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
    if not df_filtered.empty:
        df_evolucao = df_filtered.groupby(df_filtered['venda'].dt.date).agg(
             GMV=('bruto', 'sum'),
             Receita=('receita', 'sum')
        ).reset_index()
        df_evolucao.columns = ['Data da Venda', 'GMV', 'Receita']

        fig_evolucao = go.Figure()
        fig_evolucao.add_trace(go.Scatter(
            x=df_evolucao['Data da Venda'], y=df_evolucao['GMV'],
            mode='lines+markers', name='Valor Transacionado (Bruto)',
            line=dict(color='blue', width=2), marker=dict(size=6, opacity=0.8)
        ))
        fig_evolucao.add_trace(go.Scatter(
            x=df_evolucao['Data da Venda'], y=df_evolucao['Receita'],
            mode='lines+markers', name='Nossa Receita',
            line=dict(color='red', width=2), marker=dict(size=6, opacity=0.8)
        ))
        fig_evolucao.update_layout(
             xaxis_title='Data', yaxis_title='Valor (R$)', hovermode="x unified",
             legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5),
             template="plotly_white"
        )
        st.plotly_chart(fig_evolucao, use_container_width=True)
    else:
        st.info("Nenhum dado para exibir no gráfico de evolução com os filtros atuais.")

    st.markdown("---")

    # --- 4. Linha de Gráficos (Receita por Plataforma e Participação por Categoria Pag.) ---
    col6, col7 = st.columns([1.5, 1])

    with col6:
        st.subheader("Receita por Plataforma/Produto")
        if not df_filtered.empty:
            df_receita_plataforma = df_filtered.groupby('plataforma')['receita'].sum().reset_index()
            df_receita_plataforma = df_receita_plataforma.sort_values(by='receita', ascending=False)

            fig_receita_plat = px.bar(
                df_receita_plataforma, x='plataforma', y='receita',
                labels={'plataforma': 'Plataforma', 'receita': 'Receita (R$)'},
                color='plataforma', color_discrete_sequence=px.colors.qualitative.Plotly,
                title='Receita Gerada por Plataforma', text='receita'
            )
            fig_receita_plat.update_traces(texttemplate='R$ %{y:,.2f}', textposition='outside')
            fig_receita_plat.update_layout(
                 uniformtext_minsize=8, uniformtext_mode='hide', template="plotly_white",
                 xaxis_title=None, showlegend=False
             )
            st.plotly_chart(fig_receita_plat, use_container_width=True)
        else:
             st.info("Nenhum dado para exibir no gráfico de receita por plataforma.")


    with col7:
        st.subheader("Participação por Categoria de Pagamento")
        if not df_filtered.empty and 'categoria_pagamento' in df_filtered.columns:
            df_categoria_pgto = df_filtered.groupby('categoria_pagamento')['bruto'].sum().reset_index()
            df_categoria_pgto = df_categoria_pgto.sort_values(by='bruto', ascending=False)

            pull_values = [0] * len(df_categoria_pgto)
            if not df_categoria_pgto.empty:
                pull_values[0] = 0.1 # Destaca a maior fatia

            fig_categoria = px.pie(
                 df_categoria_pgto, values='bruto', names='categoria_pagamento',
                 title='Participação do GMV por Cat. Pagamento',
                 color_discrete_sequence=px.colors.qualitative.Safe
            )
            fig_categoria.update_traces(
                 textinfo='percent+label', pull=pull_values,
                 marker=dict(line=dict(color='#000000', width=1))
             )
            fig_categoria.update_layout(legend_title_text='Categoria')
            st.plotly_chart(fig_categoria, use_container_width=True)
        else:
            st.info("Nenhum dado para exibir no gráfico de pizza de categoria de pagamento.")


    st.markdown("---")

    # --- 5. Detalhamento por Cliente ---
    st.subheader("🔍 Detalhamento por Cliente")
    if not df_filtered.empty:
        def get_most_frequent(series):
            if series.empty or series.mode().empty: return 'N/A'
            return series.mode().iloc[0]

        df_detalhe_cliente = df_filtered.groupby(['cnpj', 'ec']).agg(
            Receita=('receita', 'sum'),
            N_Vendas=('cnpj', 'count'),
            Categoria_Pag_Principal=('categoria_pagamento', get_most_frequent)
        ).reset_index()

        df_detalhe_cliente['Crescimento'] = 'N/A' # Placeholder
        df_detalhe_cliente['Receita_Formatada'] = df_detalhe_cliente['Receita'].apply(lambda x: f"R$ {x:,.2f}")
        df_detalhe_cliente = df_detalhe_cliente.sort_values(by='Receita', ascending=False)

        df_display = df_detalhe_cliente[[
            'cnpj', 'ec', 'Receita_Formatada', 'Crescimento', 'N_Vendas', 'Categoria_Pag_Principal'
        ]]
        df_display.columns = ['CNPJ', 'Cliente', 'Receita', 'Crescimento', 'Nº Vendas', 'Cat. Pag. Principal']

        st.info(
             """
             **Sobre esta tabela:**
             * **Crescimento:** 'N/A' - Requer dados de período anterior para cálculo.
             * **Nº Vendas:** Contagem total de transações do cliente no período.
             * **Cat. Pag. Principal:** Categoria de pagamento mais frequente (Pix, Crédito, etc.).
             """, icon="ℹ️"
        )

        csv_detalhe_cliente = df_display.to_csv(index=False).encode('utf-8')
        st.download_button(
             label="Exportar CSV (Detalhamento por Cliente)", data=csv_detalhe_cliente,
             file_name='detalhamento_por_cliente.csv', mime='text/csv', key='download-csv-detalhe'
        )
        st.dataframe(df_display, hide_index=True, use_container_width=True)
        total_clientes_display = len(df_display)
        st.markdown(f"**Mostrando {total_clientes_display} clientes**")
    else:
        st.warning("Nenhum dado de cliente para exibir com os filtros atuais.")

    st.markdown("---")

    # --- 6. Insights Automáticos ---
    st.subheader("💡 Insights Automáticos")
    insights_list = generate_insights(df_filtered, total_gmv, receita_total)
    cols_insights = st.columns(len(insights_list) if insights_list else 1)
    for i, insight in enumerate(insights_list):
        cols_insights[i].markdown(f"""
            <div style="background-color: #e6f7ff; padding: 10px; border-radius: 5px; height: 100%;">
                <small>Insight {i+1}</small>
                <p style="font-size: 14px; margin: 0;">{insight}</p>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # --- Tabela Detalhada (Rodapé) ---
    with st.expander("Visualizar Todos os Dados Filtrados (Detalhados)"):
         st.dataframe(df_filtered) # Mostra o df_filtered completo

    # --- Exportar CSV Filtrado (Botão) ---
    csv_data_filtered = df_filtered.to_csv(index=False).encode('utf-8')
    st.download_button(
         label="Exportar CSV dos Dados Filtrados", data=csv_data_filtered,
         file_name='detalhamento_transacoes_filtrado.csv', mime='text/csv', key='download-csv-filtered'
     )
