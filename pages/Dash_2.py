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
    # Temporariamente limitado para teste de desempenho. Aumente se necessário,
    # mas esteja ciente que muitas páginas podem demorar MUITO.
    max_pages = 5
    page_size_assumption = 50 # Assumir um tamanho de página para checar o fim

    try:
        st.write(f"API Eliq: Buscando dados de {start_str} a {end_str}...") # Feedback inicial
        while page <= max_pages:
            params['page'] = page
            # Aumentado timeout para 60 segundos por página
            response = requests.get(base_url, headers=headers, params=params, timeout=60)
            response.raise_for_status() # Levanta erro para status HTTP ruins (4xx, 5xx)
            data = response.json()

            if isinstance(data, list) and data: # Verifica se a resposta é uma lista não vazia
                 all_data.extend(data)
                 st.write(f"API Eliq: Recebidos {len(data)} registros da página {page}.") # Feedback por página

                 # Se a API não retornar informações de paginação explícitas,
                 # podemos assumir que uma página menor que o esperado significa o fim.
                 if len(data) < page_size_assumption:
                     st.info(f"API Eliq: Fim dos dados (ou limite de página) alcançado na página {page}.")
                     break
                 page += 1
            else:
                 # Se a resposta não for uma lista ou estiver vazia
                 if page == 1 and not data:
                     st.warning(f"API Eliq: Nenhuma transação encontrada para o período {start_str} - {end_str}.", icon="⚠️")
                 elif page > 1:
                      st.info(f"API Eliq: Fim dos dados (página vazia) alcançado na página {page}.")
                 break # Sai do loop se não houver mais dados ou formato inesperado

        if not all_data:
            return pd.DataFrame()

        df = pd.json_normalize(all_data) # Transforma o JSON (lista de dicts) em DataFrame

        # --- Normalização ---
        df_norm = pd.DataFrame()
        # Mapeamento cuidadoso - Verifique os nomes exatos das colunas no seu JSON
        df_norm['cnpj'] = df.get('cliente_cnpj', pd.NA)
        df_norm['bruto'] = pd.to_numeric(df.get('valor_total', 0), errors='coerce').fillna(0) # Trata NAs após conversão

        # Calcular receita (usando taxa_cliente_adm se existir, senão 0)
        # Convertendo para numérico e tratando erros/ausências
        taxa_cliente_raw = df.get('cliente_taxa_adm', 0)
        taxa_cliente_percent = pd.to_numeric(taxa_cliente_raw, errors='coerce').fillna(0) / 100
        df_norm['receita'] = df_norm['bruto'] * taxa_cliente_percent

        # Tratamento de Data
        df_norm['venda'] = pd.to_datetime(df.get('data_cadastro', pd.NaT), errors='coerce', dayfirst=True) # Assumindo DD/MM/YYYY
        df_norm = df_norm.dropna(subset=['venda']) # Remove linhas onde a data falhou

        df_norm['ec'] = df.get('cliente_nome', 'N/A')
        df_norm['plataforma'] = 'Eliq'
        df_norm['tipo'] = df.get('tipo_transacao_sigla', 'N/A').astype(str) # Ex: ABA, SRV
        df_norm['bandeira'] = df.get('bandeira', 'N/A').astype(str) # Ex: Mastercard, Visa (se disponível)
        df_norm['categoria_pagamento'] = 'Outros' # Eliq geralmente não se encaixa em Pix/Cred/Deb

        st.success(f"API Eliq: {len(df_norm)} registros carregados e processados de {page-1} página(s).")
        return df_norm.dropna(subset=['bruto', 'cnpj']) # Garante que colunas essenciais não são nulas

    except requests.exceptions.Timeout:
         st.error(f"Erro API Eliq: A requisição para a página {page} demorou muito (>60s). Tente um período menor ou verifique a API.")
         return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        st.error(f"Erro de conexão/HTTP ao buscar dados da API Eliq: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro inesperado ao processar dados da API Eliq: {e}")
        return pd.DataFrame()


@st.cache_data(show_spinner="Buscando dados da API Asto/Logpay (Limitado)...")
def fetch_asto_data(api_username, api_password, start_date, end_date):
    """
    Placeholder/Tentativa para buscar dados da API Asto/Logpay.
    AVISO: Esta API não é ideal para extração em massa. Esta função pode
    retornar dados limitados ou vazios. Implementação real seria complexa.
    """
    st.warning("A API Asto/Logpay não é otimizada para relatórios completos. "
               "A função atual é um placeholder e não buscará dados reais.", icon="⚠️")

    # --- INÍCIO DO PLACEHOLDER ---
    # Implementação real seria muito complexa, envolvendo múltiplas chamadas iterativas.
    # Exemplo:
    # try:
    #     # 1. Obter lista de clientes/estabelecimentos
    #     # 2. Loop por cliente/estabelecimento
    #     # 3. Loop por dia no intervalo (start_date até end_date)
    #     # 4. Chamar API (ex: /Ticket/{id}/{d}/{m}/{y}) para cada dia/cliente
    #     # 5. Normalizar e concatenar resultados
    #     pass
    # except Exception as e:
    #     st.error(f"Erro (placeholder) na API Asto: {e}")

    # Retorna DataFrame vazio como placeholder
    df_norm_placeholder = pd.DataFrame(columns=['cnpj', 'bruto', 'receita', 'venda', 'ec', 'plataforma', 'tipo', 'bandeira', 'categoria_pagamento'])
    return df_norm_placeholder
    # --- FIM DO PLACEHOLDER ---


# --- Funções de Carregamento de CSV ---

@st.cache_data(show_spinner="Carregando e processando Bionio...")
def load_bionio_csv(uploaded_file):
    """ Carrega o CSV do Bionio e normaliza. """
    try:
        df = None
        encodings_to_try = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
        for encoding in encodings_to_try:
            try:
                uploaded_file.seek(0)
                # Tenta detectar separador, comum ser ',' ou ';'
                df = pd.read_csv(uploaded_file, encoding=encoding, sep=None, engine='python', thousands='.', decimal=',')
                st.write(f"Bionio: Arquivo lido com encoding '{encoding}' e separador detectado.")
                break
            except Exception as e:
                st.write(f"Bionio: Falha ao ler com encoding '{encoding}': {e}")
                continue
        if df is None:
            raise ValueError("Não foi possível ler o arquivo Bionio com encodings comuns.")

        # Limpar nomes das colunas (minúsculas, sem espaços extras, substitui caracteres especiais)
        df.columns = (df.columns.str.strip().str.lower()
                      .str.replace(' ', '_', regex=False)
                      .str.replace('[^0-9a-zA-Z_]', '', regex=True)) # Remove caracteres não alfanuméricos

        # --- Normalização ---
        df_norm = pd.DataFrame()
        # Verificar nomes exatos após limpeza (inspecione df.columns se necessário)
        cnpj_col = 'cnpj_da_organizacao'
        bruto_col = 'valor_total_do_pedido'
        data_col = 'data_da_criacao_do_pedido'
        ec_col = 'razao_social'
        beneficio_col = 'nome_do_beneficio'
        pagamento_col = 'tipo_de_pagamento'

        if cnpj_col not in df.columns: raise KeyError(f"Coluna '{cnpj_col}' não encontrada no Bionio.")
        if bruto_col not in df.columns: raise KeyError(f"Coluna '{bruto_col}' não encontrada no Bionio.")
        if data_col not in df.columns: raise KeyError(f"Coluna '{data_col}' não encontrada no Bionio.")
        if ec_col not in df.columns: raise KeyError(f"Coluna '{ec_col}' não encontrada no Bionio.")
        if beneficio_col not in df.columns: raise KeyError(f"Coluna '{beneficio_col}' não encontrada no Bionio.")
        if pagamento_col not in df.columns: raise KeyError(f"Coluna '{pagamento_col}' não encontrada no Bionio.")


        df_norm['cnpj'] = df[cnpj_col]
        # Valor bruto já deve ter sido lido corretamente com decimal=','
        df_norm['bruto'] = pd.to_numeric(df[bruto_col], errors='coerce').fillna(0)
        # Estimar receita (usando 5% como no código original)
        df_norm['receita'] = df_norm['bruto'] * 0.05
        # Tratar datas (tentar formatos comuns PT-BR)
        df_norm['venda'] = pd.to_datetime(df[data_col], errors='coerce', dayfirst=True, format='%d/%m/%Y %H:%M:%S')
        # Tentar outro formato se o primeiro falhar
        if df_norm['venda'].isnull().all():
             df_norm['venda'] = pd.to_datetime(df[data_col], errors='coerce', dayfirst=True)

        df_norm = df_norm.dropna(subset=['venda'])

        df_norm['ec'] = df[ec_col]
        df_norm['plataforma'] = 'Bionio'
        df_norm['tipo'] = df[beneficio_col].astype(str)
        df_norm['bandeira'] = df[pagamento_col].astype(str)

        # Lógica de Categoria de Pagamento
        def categorize_payment_bionio(tipo_pgto):
            tipo_pgto_lower = str(tipo_pgto).strip().lower()
            if 'pix' in tipo_pgto_lower or 'transferência' in tipo_pgto_lower: return 'Pix'
            if 'cartão' in tipo_pgto_lower: return 'Crédito' # Suposição
            if 'boleto' in tipo_pgto_lower: return 'Boleto'
            return 'Outros'
        df_norm['categoria_pagamento'] = df[pagamento_col].apply(categorize_payment_bionio)

        st.success(f"Bionio: {len(df_norm)} registros carregados e processados.")
        return df_norm.dropna(subset=['bruto', 'cnpj'])

    except KeyError as e:
         st.error(f"Erro ao processar Bionio: Coluna esperada não encontrada - {e}. Verifique o nome da coluna no arquivo CSV.")
         return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro inesperado ao processar arquivo Bionio: {e}")
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
                # Tenta detectar separador e usar ',' como decimal
                df = pd.read_csv(uploaded_file, encoding=encoding, sep=None, engine='python', decimal=',')
                st.write(f"Maquininha: Arquivo lido com encoding '{encoding}' e separador detectado.")
                break
            except Exception as e:
                st.write(f"Maquininha: Falha ao ler com encoding '{encoding}': {e}")
                continue
        if df is None:
             raise ValueError("Não foi possível ler o arquivo Maquininha com encodings comuns.")

        df.columns = (df.columns.str.strip().str.lower()
                      .str.replace(' ', '_', regex=False)
                      .str.replace('[^0-9a-zA-Z_]', '', regex=True))

        # --- Normalização ---
        df_norm = pd.DataFrame()
        # Verificar nomes exatos após limpeza
        cnpj_col = 'cnpj'
        bruto_col = 'bruto'
        liquido_col = 'liquido' # Coluna 'liquido' parece existir no seu CSV
        venda_col = 'venda'
        ec_col = 'ec'
        tipo_col = 'tipo'
        bandeira_col = 'bandeira'

        # Validação de Colunas Essenciais
        if cnpj_col not in df.columns: raise KeyError(f"Coluna '{cnpj_col}' não encontrada na Maquininha.")
        if bruto_col not in df.columns: raise KeyError(f"Coluna '{bruto_col}' não encontrada na Maquininha.")
        if liquido_col not in df.columns: raise KeyError(f"Coluna '{liquido_col}' não encontrada na Maquininha.")
        if venda_col not in df.columns: raise KeyError(f"Coluna '{venda_col}' não encontrada na Maquininha.")
        if ec_col not in df.columns: raise KeyError(f"Coluna '{ec_col}' não encontrada na Maquininha.")
        if tipo_col not in df.columns: raise KeyError(f"Coluna '{tipo_col}' não encontrada na Maquininha.")
        if bandeira_col not in df.columns: raise KeyError(f"Coluna '{bandeira_col}' não encontrada na Maquininha.")

        df_norm['cnpj'] = df[cnpj_col]
        df_norm['bruto'] = pd.to_numeric(df[bruto_col], errors='coerce').fillna(0)
        df_norm['liquido'] = pd.to_numeric(df[liquido_col], errors='coerce').fillna(0)
        # Calcular receita (Bruto - Líquido)
        df_norm['receita'] = df_norm['bruto'] - df_norm['liquido']
        # Tratar datas (tentar formatos comuns PT-BR)
        df_norm['venda'] = pd.to_datetime(df[venda_col], errors='coerce', dayfirst=True, format='%d/%m/%Y %H:%M:%S')
        if df_norm['venda'].isnull().all():
             df_norm['venda'] = pd.to_datetime(df[venda_col], errors='coerce', dayfirst=True)

        df_norm = df_norm.dropna(subset=['venda'])

        df_norm['ec'] = df[ec_col]
        df_norm['plataforma'] = 'Rovema Pay' # Ou Veripag
        df_norm['tipo'] = df[tipo_col].astype(str)
        df_norm['bandeira'] = df[bandeira_col].astype(str)

        # Lógica de Categoria de Pagamento
        def categorize_payment_rp(row):
             # Acessa as colunas pelo nome padrão após limpeza
            bandeira_lower = str(row.get(bandeira_col, '')).strip().lower()
            tipo_lower = str(row.get(tipo_col, '')).strip().lower()

            if 'pix' in bandeira_lower: return 'Pix'
            if tipo_lower == 'crédito': return 'Crédito'
            if tipo_lower == 'débito': return 'Débito'
            return 'Outros'
        # Passa o DataFrame original 'df' para apply, pois ele contém as colunas originais
        df_norm['categoria_pagamento'] = df.apply(categorize_payment_rp, axis=1)

        st.success(f"Maquininha: {len(df_norm)} registros carregados e processados.")
        return df_norm.dropna(subset=['bruto', 'cnpj'])

    except KeyError as e:
         st.error(f"Erro ao processar Maquininha: Coluna esperada não encontrada - {e}. Verifique o nome da coluna no arquivo CSV.")
         return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro inesperado ao processar arquivo Maquininha: {e}")
        return pd.DataFrame()


# --- Função Principal de Consolidação ---

def consolidate_data(df_bionio, df_maquininha, df_eliq, df_asto):
    """ Concatena todos os DataFrames normalizados. """
    all_transactions = []
    if df_bionio is not None and not df_bionio.empty:
        all_transactions.append(df_bionio)
    if df_maquininha is not None and not df_maquininha.empty:
        all_transactions.append(df_maquininha)
    if df_eliq is not None and not df_eliq.empty:
        all_transactions.append(df_eliq)
    if df_asto is not None and not df_asto.empty:
        all_transactions.append(df_asto)

    if not all_transactions:
        st.error("Nenhuma fonte de dados válida foi carregada ou processada.")
        return pd.DataFrame()

    df_consolidated = pd.concat(all_transactions, ignore_index=True)

    # Adiciona colunas placeholder se não existirem
    if 'responsavel_comercial' not in df_consolidated.columns:
        df_consolidated['responsavel_comercial'] = 'N/A' # Placeholder
    if 'produto' not in df_consolidated.columns:
         df_consolidated['produto'] = df_consolidated['plataforma'] # Usa plataforma como fallback

    # Garante tipos corretos para colunas importantes
    df_consolidated['bruto'] = pd.to_numeric(df_consolidated['bruto'], errors='coerce').fillna(0)
    df_consolidated['receita'] = pd.to_numeric(df_consolidated['receita'], errors='coerce').fillna(0)
    df_consolidated['venda'] = pd.to_datetime(df_consolidated['venda'], errors='coerce')
    df_consolidated = df_consolidated.dropna(subset=['venda']) # Remove linhas sem data válida

    st.success(f"Dados de {len(all_transactions)} fontes ({len(df_consolidated)} registros totais) consolidados com sucesso!", icon="✅")
    return df_consolidated


# --- Função de Insights ---
def generate_insights(df_filtered, total_gmv, receita_total):
    """Gera insights automáticos baseados nos dados filtrados."""
    insights = []

    if df_filtered.empty:
        return ["Nenhum dado encontrado para o período/filtros selecionados."]

    # Insight 1: Cliente com maior GMV
    if 'ec' in df_filtered.columns and not df_filtered.empty:
        df_gmv_cliente = df_filtered.groupby('ec')['bruto'].sum().nlargest(1).reset_index()
        if not df_gmv_cliente.empty:
            top_cliente = df_gmv_cliente.iloc[0]
            insights.append(f"O cliente **{top_cliente['ec']}** é o principal motor de vendas, com um GMV de **R$ {top_cliente['bruto']:,.2f}**.")

    # Insight 2: Plataforma/Produto com maior GMV
    if 'plataforma' in df_filtered.columns and not df_filtered.empty:
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
             insights.append(f"A Categoria de Pagamento mais utilizada (por frequência) é **{top_categoria.iloc[0]}**.")

    return insights


# --- Interface Streamlit ---

st.title("💰 Dashboard Consolidado de Transações")

# --- Área de Upload e Config API (Barra Lateral) ---
with st.sidebar:
    st.header("Fontes de Dados")

    uploaded_bionio = st.file_uploader("1. Arquivo Bionio (.csv)", type=['csv'], key="bionio_upload")
    uploaded_maquininha = st.file_uploader("2. Arquivo Maquininha/Veripag (.csv)", type=['csv'], key="maquininha_upload")

    st.markdown("---")
    st.header("Configuração das APIs")

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

    load_button_pressed = st.button("Carregar e Processar Dados", key="load_data_button")


# --- Carregamento e Processamento Principal ---
# Inicializa ou busca do estado da sessão
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'df_consolidated' not in st.session_state:
    st.session_state.df_consolidated = pd.DataFrame()

# Processa os dados APENAS se o botão foi pressionado
if load_button_pressed:
    st.session_state.data_loaded = True # Marca que o carregamento foi solicitado
    st.session_state.df_consolidated = pd.DataFrame() # Limpa dados antigos

    # 1. Carregar CSVs (se houver upload)
    df_bionio_processed = load_bionio_csv(uploaded_bionio) if uploaded_bionio else pd.DataFrame()
    df_maquininha_processed = load_maquininha_csv(uploaded_maquininha) if uploaded_maquininha else pd.DataFrame()

    # 2. Buscar Dados das APIs (usando secrets)
    df_eliq_fetched = pd.DataFrame()
    df_asto_fetched = pd.DataFrame() # Placeholder
    try:
        if 'eliq_api_token' in st.secrets:
            df_eliq_fetched = fetch_eliq_data(st.secrets["eliq_api_token"], api_start_date, api_end_date)
        else:
            st.sidebar.warning("Token da API Eliq não encontrado nos secrets.", icon="🔑")

        if 'asto_username' in st.secrets and 'asto_password' in st.secrets:
             df_asto_fetched = fetch_asto_data(st.secrets["asto_username"], st.secrets["asto_password"], api_start_date, api_end_date)
        else:
             st.sidebar.warning("Credenciais da API Asto não encontradas nos secrets.", icon="🔑")

    except KeyError as e:
         st.sidebar.error(f"Erro: Chave '{e}' não encontrada no arquivo secrets.toml.", icon="❌")
    except Exception as e:
        st.sidebar.error(f"Erro inesperado durante a chamada das APIs: {e}", icon="❌")

    # 3. Consolidar todos os dados
    df_consolidated_loaded = consolidate_data(
        df_bionio_processed,
        df_maquininha_processed,
        df_eliq_fetched,
        df_asto_fetched
    )
    st.session_state.df_consolidated = df_consolidated_loaded # Armazena no estado da sessão
else:
    # Se o botão não foi pressionado, usa os dados já carregados (se existirem)
    df_consolidated = st.session_state.df_consolidated


# --- Dashboard Principal ---

if df_consolidated.empty:
     # Mensagem se nenhum dado foi carregado ainda ou se o carregamento falhou
     if st.session_state.data_loaded: # Se o botão foi pressionado mas resultou em df vazio
         st.error("O carregamento de dados falhou ou não retornou registros válidos. Verifique os logs e as fontes de dados.")
     else:
        st.warning("Por favor, faça o upload dos arquivos CSV (se aplicável), ajuste o período das APIs e clique em 'Carregar e Processar Dados' na barra lateral.", icon="⚠️")
else:
    # --- FILTROS ---
    st.subheader("Filtros de Análise")
    col_date, col_plataforma, col_bandeira, col_tipo, col_categoria_pgto = st.columns([1.5, 1.5, 1, 1, 1.5])

    with col_date:
        data_min = df_consolidated['venda'].min().date()
        data_max = df_consolidated['venda'].max().date()
        # Define valores padrão para os filtros de data apenas na primeira execução
        if 'date_filter_values' not in st.session_state:
            st.session_state.date_filter_values = (data_min, data_max)

        data_inicial, data_final = st.date_input(
            "Período",
            value=st.session_state.date_filter_values,
            min_value=data_min,
            max_value=data_max,
            key='date_filter' # Chave para o widget
        )
        # Atualiza o estado da sessão se os valores mudarem
        st.session_state.date_filter_values = (data_inicial, data_final)


    with col_plataforma:
        plataformas = ['Todos'] + sorted(df_consolidated['plataforma'].unique().tolist())
        filtro_plataforma = st.selectbox("Plataforma/Produto", options=plataformas)

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
    # Garante que as datas sejam válidas antes de filtrar
    if data_inicial <= data_final:
        df_filtered = df_consolidated[
            (df_consolidated['venda'].dt.date >= data_inicial) &
            (df_consolidated['venda'].dt.date <= data_final)
        ].copy()
    else:
        st.warning("Data inicial não pode ser posterior à data final.")
        df_filtered = pd.DataFrame(columns=df_consolidated.columns) # DataFrame vazio para evitar erros

    if filtro_plataforma != 'Todos':
         df_filtered = df_filtered[df_filtered['plataforma'] == filtro_plataforma]
    if filtro_bandeira != 'Todos':
         df_filtered = df_filtered[df_filtered['bandeira'].astype(str) == filtro_bandeira]
    if filtro_tipo != 'Todos':
         df_filtered = df_filtered[df_filtered['tipo'].astype(str) == filtro_tipo]
    if filtro_categoria != 'Todos':
        df_filtered = df_filtered[df_filtered['categoria_pagamento'] == filtro_categoria]


    # --- KPIs ---
    total_gmv = df_filtered['bruto'].sum()
    receita_total = df_filtered['receita'].sum()
    clientes_ativos = df_filtered['cnpj'].nunique()
    margem_media = (receita_total / total_gmv) * 100 if total_gmv > 0 else 0
    gmv_por_cliente = df_filtered.groupby('cnpj')['bruto'].sum()
    clientes_em_queda_proxy = gmv_por_cliente[gmv_por_cliente < 1000].count() if not gmv_por_cliente.empty else 0

    st.markdown("""<style>...</style>""", unsafe_allow_html=True) # Mantém seu estilo
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
        df_evolucao = df_filtered.groupby(df_filtered['venda'].dt.date).agg(
             GMV=('bruto', 'sum'), Receita=('receita', 'sum')
        ).reset_index().rename(columns={'venda': 'Data da Venda'})

        if not df_evolucao.empty:
            fig_evolucao = go.Figure()
            fig_evolucao.add_trace(go.Scatter(x=df_evolucao['Data da Venda'], y=df_evolucao['GMV'], mode='lines+markers', name='Valor Transacionado (Bruto)', line=dict(color='blue', width=2), marker=dict(size=6, opacity=0.8)))
            fig_evolucao.add_trace(go.Scatter(x=df_evolucao['Data da Venda'], y=df_evolucao['Receita'], mode='lines+markers', name='Nossa Receita', line=dict(color='red', width=2), marker=dict(size=6, opacity=0.8)))
            fig_evolucao.update_layout(xaxis_title='Data', yaxis_title='Valor (R$)', hovermode="x unified", legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5), template="plotly_white")
            st.plotly_chart(fig_evolucao, use_container_width=True)
        else:
            st.info("Nenhum dado agregado para exibir no gráfico de evolução com os filtros atuais.")
    else:
        st.info("Nenhum dado para exibir no gráfico de evolução com os filtros atuais.")
    st.markdown("---")

    # --- Gráficos Inferiores ---
    col6, col7 = st.columns([1.5, 1])
    with col6:
        st.subheader("Receita por Plataforma/Produto")
        if not df_filtered.empty:
            df_receita_plataforma = df_filtered.groupby('plataforma')['receita'].sum().reset_index().sort_values(by='receita', ascending=False)
            if not df_receita_plataforma.empty:
                fig_receita_plat = px.bar(df_receita_plataforma, x='plataforma', y='receita', labels={'plataforma': 'Plataforma', 'receita': 'Receita (R$)'}, color='plataforma', color_discrete_sequence=px.colors.qualitative.Plotly, title='Receita Gerada por Plataforma', text='receita')
                fig_receita_plat.update_traces(texttemplate='R$ %{y:,.2f}', textposition='outside')
                fig_receita_plat.update_layout(uniformtext_minsize=8, uniformtext_mode='hide', template="plotly_white", xaxis_title=None, showlegend=False)
                st.plotly_chart(fig_receita_plat, use_container_width=True)
            else:
                st.info("Nenhum dado agregado para exibir no gráfico de receita por plataforma.")
        else:
             st.info("Nenhum dado para exibir no gráfico de receita por plataforma.")

    with col7:
        st.subheader("Participação por Categoria de Pagamento")
        if not df_filtered.empty and 'categoria_pagamento' in df_filtered.columns:
            df_categoria_pgto = df_filtered.groupby('categoria_pagamento')['bruto'].sum().reset_index().sort_values(by='bruto', ascending=False)
            if not df_categoria_pgto.empty and df_categoria_pgto['bruto'].sum() > 0: # Evita erro se soma for 0
                pull_values = [0] * len(df_categoria_pgto)
                pull_values[0] = 0.1
                fig_categoria = px.pie(df_categoria_pgto, values='bruto', names='categoria_pagamento', title='Participação do GMV por Cat. Pagamento', color_discrete_sequence=px.colors.qualitative.Safe)
                fig_categoria.update_traces(textinfo='percent+label', pull=pull_values, marker=dict(line=dict(color='#000000', width=1)))
                fig_categoria.update_layout(legend_title_text='Categoria')
                st.plotly_chart(fig_categoria, use_container_width=True)
            else:
                 st.info("Nenhum dado com valor bruto positivo para exibir no gráfico de pizza.")
        else:
            st.info("Nenhum dado para exibir no gráfico de pizza de categoria de pagamento.")
    st.markdown("---")

    # --- Detalhamento por Cliente ---
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
        df_detalhe_cliente['Crescimento'] = 'N/A'
        df_detalhe_cliente['Receita_Formatada'] = df_detalhe_cliente['Receita'].apply(lambda x: f"R$ {x:,.2f}")
        df_detalhe_cliente = df_detalhe_cliente.sort_values(by='Receita', ascending=False)

        df_display = df_detalhe_cliente[['cnpj', 'ec', 'Receita_Formatada', 'Crescimento', 'N_Vendas', 'Categoria_Pag_Principal']]
        df_display.columns = ['CNPJ', 'Cliente', 'Receita', 'Crescimento', 'Nº Vendas', 'Cat. Pag. Principal']

        st.info("""**Sobre esta tabela:** ...""", icon="ℹ️") # Mantém sua info
        csv_detalhe_cliente = df_display.to_csv(index=False).encode('utf-8')
        st.download_button(label="Exportar CSV (Detalhamento por Cliente)", data=csv_detalhe_cliente, file_name='detalhamento_por_cliente.csv', mime='text/csv', key='download-csv-detalhe')
        st.dataframe(df_display, hide_index=True, use_container_width=True)
        total_clientes_display = len(df_display)
        st.markdown(f"**Mostrando {total_clientes_display} clientes**")
    else:
        st.warning("Nenhum dado de cliente para exibir com os filtros atuais.")
    st.markdown("---")

    # --- Insights Automáticos ---
    st.subheader("💡 Insights Automáticos")
    insights_list = generate_insights(df_filtered, total_gmv, receita_total)
    cols_insights = st.columns(len(insights_list) if insights_list else 1)
    for i, insight in enumerate(insights_list):
        cols_insights[i].markdown(f"""<div style="background-color: #e6f7ff; padding: 10px; border-radius: 5px; height: 100%;"><small>Insight {i+1}</small><p style="font-size: 14px; margin: 0;">{insight}</p></div>""", unsafe_allow_html=True)
    st.markdown("---")

    # --- Tabela Detalhada (Rodapé) ---
    with st.expander("Visualizar Todos os Dados Filtrados (Detalhados)"):
         st.dataframe(df_filtered)

    csv_data_filtered = df_filtered.to_csv(index=False).encode('utf-8')
    st.download_button(label="Exportar CSV dos Dados Filtrados", data=csv_data_filtered, file_name='detalhamento_transacoes_filtrado.csv', mime='text/csv', key='download-csv-filtered')
