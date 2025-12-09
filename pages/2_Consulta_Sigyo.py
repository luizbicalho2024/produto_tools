import streamlit as st
import pandas as pd
import requests
import json
from datetime import date, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Configuração da Página ---
st.set_page_config(
    page_title="Consulta Sigyo",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🔍 Consulta API Sigyo")

# --- Barra Lateral (Configurações Globais) ---
with st.sidebar:
    st.header("Configurações")
    default_token = st.secrets.get("eliq_api_token", "")
    api_token = st.text_input("Token de Acesso (Bearer)", value=default_token, type="password")
    
    st.markdown("---")
    tipo_relatorio = st.radio(
        "Tipo de Relatório",
        ["Transações", "Motoristas"],
        index=1
    )

# ==============================================================================
# FUNÇÕES DE REDE ROBUSTA (SESSION COM RETRY)
# ==============================================================================

def get_retry_session():
    """Cria uma sessão HTTP que tenta reconectar automaticamente em caso de falha."""
    session = requests.Session()
    # Configura 3 tentativas extras com espera exponencial (0.5s, 1s, 2s...)
    # Tenta reconectar em erros 500, 502, 503, 504 e desconexões
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# ==============================================================================
# FUNÇÕES DE BUSCA E PROCESSAMENTO (MOTORISTAS)
# ==============================================================================

@st.cache_data(show_spinner="Buscando motoristas...", ttl=300)
def fetch_motoristas_sigyo(token):
    """Busca motoristas com tolerância a falhas de rede."""
    base_url = "https://sigyo.uzzipay.com/api/motoristas"
    headers = {'Authorization': f'Bearer {token}'}
    
    # Parâmetros de paginação redundantes para tentar forçar lotes pequenos
    params = {
        'expand': 'grupos_vinculados,modulos,empresas,empresas.municipio,empresas.municipio.estado',
        'inline': 'false',
        'page': 1,
        'per-page': 100,  # Tenta pedir 100 por vez
        'limit': 100
    }
    
    all_data = []
    page = 1
    session = get_retry_session() # Usa a sessão robusta
    
    progress_text = "Conectando à API..."
    my_bar = st.progress(0, text=progress_text)
    
    try:
        while True:
            params['page'] = page
            # Timeout alto (120s) para garantir
            response = session.get(base_url, headers=headers, params=params, timeout=120)
            response.raise_for_status()
            
            try:
                data = response.json()
            except json.JSONDecodeError:
                # Se falhar no JSON, tenta ler texto cru para ver se é erro HTML
                st.error(f"Resposta inválida na página {page}. O servidor pode ter cortado a conexão.")
                break
            
            # --- Lógica de Paginação ---
            items = []
            total_pages = 1
            
            if isinstance(data, list):
                items = data
                # Tenta ler headers de paginação
                total_count = int(response.headers.get('X-Pagination-Total-Count', 0))
                per_page_sent = int(response.headers.get('X-Pagination-Per-Page', 0))
                
                # Se a API retornou mais de 1000 itens de uma vez, ela ignorou a paginação
                if len(items) > 1000:
                    total_pages = 1 # Considera página única
                elif per_page_sent > 0:
                    total_pages = (total_count // per_page_sent) + 1
                else:
                    total_pages = 0 # Indefinido
                    
            elif isinstance(data, dict) and 'items' in data:
                items = data['items']
                total_pages = data.get('_meta', {}).get('pageCount', 1)

            if not items:
                break
                
            all_data.extend(items)
            
            # Atualiza barra visual
            if total_pages > 1:
                percent = min(page / total_pages, 1.0)
                my_bar.progress(percent, text=f"Baixando página {page} de {total_pages}...")
            else:
                # Se não sabemos o total ou é página única (dump grande)
                my_bar.progress(1.0, text=f"Processando lote {page} ({len(items)} registros)...")
            
            # --- Critérios de Parada ---
            # 1. Se atingiu a última página conhecida
            if total_pages > 0 and page >= total_pages:
                break
            # 2. Se a lista veio vazia
            if len(items) == 0:
                break
            # 3. Se a API mandou um dump gigante (>5000 itens) na pág 1, assume que acabou
            if page == 1 and len(items) > 5000:
                st.info("A API retornou todos os dados em uma única página.")
                break
                
            page += 1
            
    except requests.exceptions.ChunkedEncodingError:
        st.error("Erro de Conexão: O servidor cortou a transmissão dos dados. Tente novamente.")
        return pd.DataFrame()
    except requests.exceptions.RetryError:
        st.error("Falha após várias tentativas de reconexão.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro na comunicação com a API: {e}")
        return pd.DataFrame()
    finally:
        my_bar.empty()

    if not all_data:
        return pd.DataFrame()

    # --- Processamento dos Dados ---
    
    def extract_names(item_list):
        if not isinstance(item_list, list): return ""
        return ", ".join([str(i.get('nome', '')) for i in item_list if isinstance(i, dict) and i.get('nome')])

    def extract_empresas(empresa_list):
        if not isinstance(empresa_list, list): return ""
        nomes = []
        for emp in empresa_list:
            if isinstance(emp, dict):
                nome = emp.get('nome_fantasia') or emp.get('razao_social') or 'N/A'
                cnpj = emp.get('cnpj', '')
                nomes.append(f"{nome} ({cnpj})")
        return "; ".join(nomes)

    processed_rows = []
    
    for d in all_data:
        if not isinstance(d, dict): continue
        row = {
            'ID': d.get('id'),
            'Nome': d.get('nome'),
            'CPF/CNH': d.get('cnh'),
            'Categoria CNH': d.get('cnh_categoria'),
            'Validade CNH': d.get('cnh_validade'),
            'Matrícula': d.get('matricula'),
            'Email': d.get('email'),
            'Telefone': d.get('telefone'),
            'Status': d.get('status'),
            'Ativo': 'Sim' if d.get('ativo') in [True, 1] else 'Não',
            'Data Cadastro': d.get('data_cadastro'),
            'Grupos Vinculados': extract_names(d.get('grupos_vinculados')),
            'Empresas': extract_empresas(d.get('empresas')),
            'Módulos': extract_names(d.get('modulos'))
        }
        processed_rows.append(row)

    output = pd.DataFrame(processed_rows)
    
    # Tratamento final de datas
    if 'Validade CNH' in output.columns:
        output['Validade CNH'] = pd.to_datetime(output['Validade CNH'], errors='coerce').dt.strftime('%d/%m/%Y')
    if 'Data Cadastro' in output.columns:
        output['Data Cadastro'] = pd.to_datetime(output['Data Cadastro'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
    
    return output

# ==============================================================================
# FUNÇÕES DE BUSCA E PROCESSAMENTO (TRANSAÇÕES)
# ==============================================================================

@st.cache_data(show_spinner="Buscando transações...", ttl=300)
def fetch_transacoes_sigyo(token, start_date, end_date):
    """ Busca dados da API de Transações. """
    base_url = "https://sigyo.uzzipay.com/api/transacoes"
    headers = {'Authorization': f'Bearer {token}'}
    start_str = start_date.strftime('%d/%m/%Y')
    end_str = end_date.strftime('%d/%m/%Y')
    params = {'TransacaoSearch[data_cadastro]': f'{start_str} - {end_str}'}

    all_data = []
    page = 1
    session = get_retry_session()
    
    try:
        while True:
            params['page'] = page
            response = session.get(base_url, headers=headers, params=params, timeout=60)
            
            if response.status_code != 200:
                st.error(f"Erro na API: {response.status_code} - {response.text}")
                break
                
            data = response.json()
            if isinstance(data, list) and data:
                all_data.extend(data)
                if len(data) < 20: 
                    break
                page += 1
            else:
                break
                
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return pd.DataFrame()

    if not all_data:
        return pd.DataFrame()

    df = pd.json_normalize(all_data)
    
    cols_map = {
        'id': 'ID Transação',
        'data_cadastro': 'Data',
        'valor_total': 'Valor Total',
        'cliente_nome': 'Cliente',
        'cliente_cnpj': 'CNPJ Cliente',
        'nome_fantasia': 'Estabelecimento',
        'tipo_transacao_nome': 'Tipo',
        'status_transacao_nome': 'Status',
        'usuario_nome': 'Usuário/Motorista',
        'placa': 'Placa'
    }
    
    available_cols = [c for c in cols_map.keys() if c in df.columns]
    df_final = df[available_cols].rename(columns=cols_map)
    
    if 'Data' in df_final.columns:
        df_final['Data'] = pd.to_datetime(df_final['Data']).dt.strftime('%d/%m/%Y %H:%M')
    if 'Valor Total' in df_final.columns:
        df_final['Valor Total'] = pd.to_numeric(df_final['Valor Total'], errors='coerce')

    return df_final

# ==============================================================================
# LÓGICA DA INTERFACE (UI)
# ==============================================================================

if not api_token:
    st.warning("⚠️ Por favor, insira o Token da API na barra lateral para continuar.")
    st.stop()

# --- BLOCO DE RELATÓRIO: MOTORISTAS ---
if tipo_relatorio == "Motoristas":
    st.subheader("📋 Base de Motoristas")
    st.info("Este relatório lista todos os motoristas cadastrados, com seus grupos, empresas e status.")

    if st.button("🔄 Buscar Dados de Motoristas"):
        df_motoristas = fetch_motoristas_sigyo(api_token)
        
        if not df_motoristas.empty:
            st.session_state['df_motoristas'] = df_motoristas
            st.success(f"Sucesso! {len(df_motoristas)} motoristas carregados.")
        else:
            st.warning("A busca não retornou dados. Verifique a conexão ou tente novamente.")

    if 'df_motoristas' in st.session_state and not st.session_state['df_motoristas'].empty:
        df = st.session_state['df_motoristas']
        
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            status_opts = sorted(df['Status'].astype(str).unique())
            filtro_status = st.multiselect("Filtrar por Status:", options=status_opts, default=status_opts)
        with col_f2:
            search_term = st.text_input("Buscar por Nome ou CNH:", "")

        mask = df['Status'].isin(filtro_status)
        if search_term:
            mask &= (
                df['Nome'].str.contains(search_term, case=False, na=False) | 
                df['CPF/CNH'].str.contains(search_term, na=False)
            )
        
        df_filtered = df[mask]

        st.markdown("### Selecionar Colunas para Exportação")
        all_cols = df_filtered.columns.tolist()
        cols_default = ['ID', 'Nome', 'CPF/CNH', 'Status', 'Empresas', 'Grupos Vinculados', 'Módulos']
        cols_default = [c for c in cols_default if c in all_cols]
        if not cols_default: cols_default = all_cols[:5]
        
        selected_cols = st.multiselect("Colunas:", all_cols, default=cols_default)

        if not selected_cols:
            st.error("Selecione pelo menos uma coluna.")
        else:
            df_display = df_filtered[selected_cols]
            st.markdown(f"**Total de registros filtrados:** {len(df_display)}")
            st.dataframe(df_display, use_container_width=True)
            
            csv = df_display.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📥 Baixar Planilha (CSV)",
                data=csv,
                file_name="relatorio_motoristas_sigyo.csv",
                mime="text/csv",
                type="primary"
            )

# --- BLOCO DE RELATÓRIO: TRANSAÇÕES ---
elif tipo_relatorio == "Transações":
    st.subheader("💲 Relatório de Transações")
    
    col1, col2 = st.columns(2)
    today = date.today()
    default_start = today - timedelta(days=7)
    
    with col1:
        data_inicial = st.date_input("Data Inicial", default_start, format="DD/MM/YYYY")
    with col2:
        data_final = st.date_input("Data Final", today, format="DD/MM/YYYY")

    if st.button("🔄 Buscar Transações"):
        if data_inicial > data_final:
            st.error("Data inicial não pode ser maior que a final.")
        else:
            df_transacoes = fetch_transacoes_sigyo(api_token, data_inicial, data_final)
            
            if not df_transacoes.empty:
                st.session_state['df_transacoes'] = df_transacoes
                st.success(f"{len(df_transacoes)} transações encontradas!")
            else:
                st.warning("Nenhuma transação encontrada para o período.")

    if 'df_transacoes' in st.session_state and not st.session_state['df_transacoes'].empty:
        df = st.session_state['df_transacoes']
        
        st.markdown("### Selecionar Colunas para Exportação")
        all_cols = df.columns.tolist()
        selected_cols = st.multiselect("Colunas:", all_cols, default=all_cols)
        
        if not selected_cols:
            st.error("Selecione pelo menos uma coluna.")
        else:
            df_display = df[selected_cols]
            if 'Valor Total' in df_display.columns:
                total_val = df_display['Valor Total'].sum()
                st.metric("Volume Total no Período", f"R$ {total_val:,.2f}")

            st.dataframe(df_display, use_container_width=True)
            
            csv = df_display.to_csv(index=False, sep=';', decimal=',').encode('utf-8-sig')
            st.download_button(
                label="📥 Baixar Planilha (CSV)",
                data=csv,
                file_name="relatorio_transacoes_sigyo.csv",
                mime="text/csv",
                type="primary"
            )
