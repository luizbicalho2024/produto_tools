import streamlit as st
import pandas as pd
import requests
import json
import io
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
    st.header("Selecione o Relatório")
    # AQUI VOCÊ ESCOLHE QUAL TABELA QUER VER
    tipo_relatorio = st.radio(
        "Qual base deseja consultar?",
        ["Transações / Credenciados", "Base de Motoristas"],
        index=0 
    )

# ==============================================================================
# FUNÇÕES DE REDE ROBUSTA (SESSION COM RETRY)
# ==============================================================================

def get_retry_session():
    """Cria uma sessão HTTP blindada que tenta reconectar automaticamente."""
    session = requests.Session()
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
# 1. FUNÇÕES PARA MOTORISTAS
# ==============================================================================

@st.cache_data(show_spinner=False, ttl=300)
def fetch_motoristas_sigyo(token):
    """Busca motoristas via Streaming (para suportar bases grandes)."""
    base_url = "https://sigyo.uzzipay.com/api/motoristas"
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
    }
    
    params = {
        'expand': 'grupos_vinculados,modulos,empresas,empresas.municipio,empresas.municipio.estado',
        'inline': 'false'
    }
    
    status_text = st.empty()
    progress_bar = st.progress(0)
    
    try:
        status_text.text("Conectando à API de Motoristas...")
        
        # Timeout alto e Stream ativado
        with requests.get(base_url, headers=headers, params=params, stream=True, timeout=300) as response:
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            data_buffer = io.BytesIO()
            downloaded_size = 0
            chunk_size = 100 * 1024 
            
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    data_buffer.write(chunk)
                    downloaded_size += len(chunk)
                    
                    mb_downloaded = downloaded_size / (1024 * 1024)
                    if total_size > 0:
                        progress = min(downloaded_size / total_size, 1.0)
                        status_text.text(f"Baixando Motoristas: {mb_downloaded:.2f} MB...")
                        progress_bar.progress(progress)
                    else:
                        status_text.text(f"Baixando Motoristas: {mb_downloaded:.2f} MB recebidos...")

            progress_bar.progress(1.0)
            status_text.text("Download concluído! Processando dados...")
            
            data_buffer.seek(0)
            json_content = json.load(data_buffer)
            
            if isinstance(json_content, dict) and 'items' in json_content:
                return process_motoristas_data(json_content['items'])
            elif isinstance(json_content, list):
                return process_motoristas_data(json_content)
            else:
                return pd.DataFrame()

    except Exception as e:
        st.error(f"Erro ao buscar motoristas: {e}")
        return pd.DataFrame()
    finally:
        status_text.empty()
        progress_bar.empty()

def process_motoristas_data(all_data):
    """Transforma o JSON de motoristas em Tabela."""
    if not all_data: return pd.DataFrame()

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
        processed_rows.append({
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
        })

    output = pd.DataFrame(processed_rows)
    if 'Validade CNH' in output.columns:
        output['Validade CNH'] = pd.to_datetime(output['Validade CNH'], errors='coerce').dt.strftime('%d/%m/%Y')
    if 'Data Cadastro' in output.columns:
        output['Data Cadastro'] = pd.to_datetime(output['Data Cadastro'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
    return output

# ==============================================================================
# 2. FUNÇÕES PARA TRANSAÇÕES (ANTIGO - DADOS DE CREDENCIADO)
# ==============================================================================

@st.cache_data(show_spinner="Buscando transações...", ttl=300)
def fetch_transacoes_sigyo(token, start_date, end_date):
    """Busca transações (dados de venda e credenciado)."""
    base_url = "https://sigyo.uzzipay.com/api/transacoes"
    headers = {'Authorization': f'Bearer {token}'}
    
    start_str = start_date.strftime('%d/%m/%Y')
    end_str = end_date.strftime('%d/%m/%Y')
    params = {'TransacaoSearch[data_cadastro]': f'{start_str} - {end_str}'}

    all_data = []
    page = 1
    session = get_retry_session()
    
    status_text = st.empty()
    
    try:
        while True:
            params['page'] = page
            status_text.text(f"Buscando página {page} de transações...")
            
            response = session.get(base_url, headers=headers, params=params, timeout=60)
            
            if response.status_code != 200:
                st.error(f"Erro na API: {response.status_code} - {response.text}")
                break
                
            data = response.json()
            if isinstance(data, list) and data:
                all_data.extend(data)
                # Se vier menos que 20 registros, assume que é a última página
                if len(data) < 20: 
                    break
                page += 1
            else:
                break
                
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return pd.DataFrame()
    finally:
        status_text.empty()

    if not all_data:
        return pd.DataFrame()

    df = pd.json_normalize(all_data)
    
    # Renomeia colunas para ficar igual à "antiga" consulta
    cols_map = {
        'id': 'ID Transação',
        'data_cadastro': 'Data',
        'valor_total': 'Valor Total',
        'cliente_nome': 'Cliente (Credenciado)',
        'cliente_cnpj': 'CNPJ Cliente',
        'nome_fantasia': 'Nome Fantasia',
        'tipo_transacao_nome': 'Tipo',
        'status_transacao_nome': 'Status',
        'usuario_nome': 'Motorista/Usuário',
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
# LÓGICA PRINCIPAL (DISPLAY)
# ==============================================================================

if not api_token:
    st.warning("⚠️ Por favor, insira o Token da API na barra lateral para continuar.")
    st.stop()

# ------------------------------------------------------------------------------
# OPÇÃO 1: TRANSAÇÕES / CREDENCIADOS (ANTIGO)
# ------------------------------------------------------------------------------
if tipo_relatorio == "Transações / Credenciados":
    st.subheader("💲 Relatório de Transações (Credenciados)")
    st.markdown("Consulta movimentações financeiras, postos e credenciados.")
    
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
                st.warning("Nenhuma transação encontrada para o período selecionado.")

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
                file_name="relatorio_transacoes_credenciados.csv",
                mime="text/csv",
                type="primary"
            )

# ------------------------------------------------------------------------------
# OPÇÃO 2: BASE DE MOTORISTAS (NOVO)
# ------------------------------------------------------------------------------
elif tipo_relatorio == "Base de Motoristas":
    st.subheader("📋 Base de Motoristas")
    st.markdown("Consulta cadastro completo de motoristas, CNH, grupos e empresas vinculadas.")

    if st.button("🔄 Baixar Base Completa de Motoristas"):
        df_motoristas = fetch_motoristas_sigyo(api_token)
        
        if not df_motoristas.empty:
            st.session_state['df_motoristas'] = df_motoristas
            st.success(f"Sucesso! {len(df_motoristas)} motoristas carregados.")
        else:
            st.warning("Não foi possível carregar os motoristas. Verifique logs/token.")

    if 'df_motoristas' in st.session_state and not st.session_state['df_motoristas'].empty:
        df = st.session_state['df_motoristas']
        
        # Filtros para Motoristas
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
            st.markdown(f"**Registros exibidos:** {len(df_display)}")
            
            st.dataframe(df_display, use_container_width=True)
            
            csv = df_display.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📥 Baixar Planilha (CSV)",
                data=csv,
                file_name="relatorio_motoristas_sigyo.csv",
                mime="text/csv",
                type="primary"
            )
