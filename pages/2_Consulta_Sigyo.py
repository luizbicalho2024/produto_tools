import streamlit as st
import pandas as pd
import requests
import json
import tempfile
import os
import gc
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Configuração da Página ---
st.set_page_config(
    page_title="Consulta Sigyo (Auto-Repair)",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("💻 Consulta Sigyo (Versão Blindada)")
st.caption("Modo Auto-Repair: Recupera arquivos JSON cortados ou incompletos.")

# --- Barra Lateral ---
with st.sidebar:
    st.header("Configurações")
    try:
        default_token = st.secrets.get("eliq_api_token", "")
    except:
        default_token = ""
        
    api_token = st.text_input("Token de Acesso (Bearer)", value=default_token, type="password")
    
    st.markdown("---")
    st.header("Selecione a Base")
    tipo_relatorio = st.radio(
        "Qual cadastro deseja consultar?",
        ["Motoristas", "Credenciados", "Clientes"],
        index=0
    )

# ==============================================================================
# FUNÇÕES DE REDE (PARA CREDENCIADOS E CLIENTES)
# ==============================================================================

def get_session():
    """Cria uma sessão HTTP que finge ser um navegador."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive"
    })
    
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504, 104],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_data_local(url, token, params=None):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Encoding": "gzip, deflate, br" 
    }

    session = get_session()
    fd, tmp_path = tempfile.mkstemp(suffix=".json")
    os.close(fd)

    print(f"\n[DEBUG] Iniciando download de: {url}")
    print(f"[DEBUG] Arquivo temporário: {tmp_path}")

    try:
        # Timeout longo para evitar queda
        with session.get(url, headers=headers, params=params, stream=True, timeout=(10, 600)) as response:
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            chunk_count = 0
            
            with open(tmp_path, 'wb') as f:
                # Chunk de 8MB
                for chunk in response.iter_content(chunk_size=8 * 1024 * 1024): 
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        chunk_count += 1
                        
                        if chunk_count % 5 == 0:
                            mb_downloaded = downloaded / (1024 * 1024)
                            if total_size > 0:
                                percent = (downloaded / total_size) * 100
                                print(f"\r[DEBUG] Baixando... {percent:.1f}% ({mb_downloaded:.1f} MB)", end="")
                            else:
                                print(f"\r[DEBUG] Baixando... {mb_downloaded:.1f} MB", end="")

        print(f"\n[DEBUG] Download finalizado. Tamanho em disco: {downloaded / (1024*1024):.2f} MB")
        print("[DEBUG] Lendo JSON do disco...")
        
        with open(tmp_path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                print(f"\n[ERRO FATAL] Arquivo incompleto. Erro: {e}")
                st.error("O servidor encerrou a conexão antes do fim. Tente novamente.")
                return None
            
        print("[DEBUG] JSON carregado na memória!")
        gc.collect()

        if isinstance(data, dict) and "items" in data:
            return data["items"]
        if isinstance(data, list):
            return data
        return []

    except Exception as e:
        print(f"\n[ERRO] {e}")
        st.error(f"Erro no processo: {e}")
        return None
    finally:
        session.close()
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass

# ==============================================================================
# PROCESSADORES E FUNÇÕES AUXILIARES
# ==============================================================================

def attempt_repair_json(file_bytes):
    """
    Tenta reparar um JSON incompleto fechando estruturas abertas.
    """
    encodings = ['utf-8', 'latin-1']
    
    for encoding in encodings:
        try:
            # Tenta decodificar para string
            text_content = file_bytes.decode(encoding)
            
            # Lista de tentativas de reparo (do mais provável para o menos)
            # 1. Cortou dentro de uma string (ex: "nome": "Jea...) -> fecha aspas e objetos
            # 2. Cortou num valor (ex: "idade": 2...) -> fecha objetos
            # 3. Cortou depois de um objeto -> fecha a lista
            repair_attempts = [
                text_content + '"}]',       # Fecha string, objeto e lista
                text_content + '}]',        # Fecha objeto e lista
                text_content + ']',         # Fecha lista apenas
                text_content + '"]',        # Fecha string e lista
                text_content + '}',         # Fecha objeto raiz (se for dict)
                text_content + '"}'         # Fecha string e objeto raiz
            ]
            
            for attempt in repair_attempts:
                try:
                    data = json.loads(attempt)
                    # Se chegou aqui, o reparo funcionou!
                    return data, f"✅ Arquivo recuperado com sucesso! (Encoding: {encoding})"
                except json.JSONDecodeError:
                    continue
                    
        except UnicodeDecodeError:
            continue
            
    return None, "❌ Não foi possível reparar o arquivo automaticamente. Ele está muito danificado."

def process_generic(all_data, entity_type):
    if not all_data: return pd.DataFrame()
    
    print(f"[DEBUG] Processando {len(all_data)} registros de {entity_type}...")
    
    def safe_get(d, keys, default=''):
        val = d
        for k in keys:
            if isinstance(val, dict): val = val.get(k)
            else: return default
        return str(val) if val is not None else default

    def extract_names(item_list):
        if not isinstance(item_list, list): return ""
        return ", ".join([str(i.get('nome', '')) for i in item_list if isinstance(i, dict) and i.get('nome')])

    processed_rows = []
    
    for d in all_data:
        if not isinstance(d, dict): continue
        
        row = {}
        row['ID'] = d.get('id')
        row['Email'] = d.get('email')
        row['Telefone'] = d.get('telefone')
        row['Data Cadastro'] = d.get('data_cadastro')
        row['Módulos'] = extract_names(d.get('modulos'))
        row['Ativo'] = 'Sim' if d.get('ativo') in [True, 1] else 'Não'
        
        if entity_type == "Motoristas":
            row['Nome'] = d.get('nome')
            row['CPF/CNH'] = d.get('cnh')
            row['Status'] = d.get('status')
            empresas = []
            for emp in d.get('empresas', []):
                if isinstance(emp, dict):
                    nm = emp.get('nome_fantasia') or emp.get('razao_social') or 'N/A'
                    cnpj = emp.get('cnpj', '')
                    empresas.append(f"{nm} ({cnpj})")
            row['Empresas'] = "; ".join(empresas)
            
        elif entity_type == "Credenciados":
            row['CNPJ'] = d.get('cnpj')
            row['Nome Fantasia'] = d.get('nome')
            row['Razão Social'] = d.get('razao_social')
            row['Cidade'] = safe_get(d, ['municipio', 'nome'])
            row['Responsável'] = safe_get(d, ['dadosAcesso', 'nome_responsavel'])
            
        elif entity_type == "Clientes":
            row['CNPJ'] = d.get('cnpj')
            row['Nome Fantasia'] = d.get('nome')
            row['Razão Social'] = d.get('razao_social')
            row['Cidade'] = safe_get(d, ['municipio', 'nome'])
            row['Organização'] = safe_get(d, ['organizacao', 'nome'])

        processed_rows.append(row)

    df = pd.DataFrame(processed_rows)
    # Formata data se existir
    if 'Data Cadastro' in df.columns:
        df['Data Cadastro'] = pd.to_datetime(df['Data Cadastro'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
        
    return df

# ==============================================================================
# LÓGICA DA INTERFACE (HÍBRIDA + AUTO REPAIR)
# ==============================================================================

# --- CASO 1: MOTORISTAS (UPLOAD MANUAL ROBUSTO) ---
if tipo_relatorio == "Motoristas":
    st.info("📂 **Modo de Upload Manual**: Para Motoristas, carregue o arquivo JSON exportado.")
    
    uploaded_file = st.file_uploader("Selecione o arquivo JSON de Motoristas", type=["json"])
    
    if uploaded_file is not None:
        with st.spinner("Analisando arquivo..."):
            # Lê os bytes brutos
            file_bytes = uploaded_file.getvalue()
            
            raw_data = None
            try:
                # Tentativa 1: Leitura Padrão
                json_str = file_bytes.decode('utf-8')
                raw_data = json.loads(json_str)
                st.success("Arquivo íntegro carregado com sucesso!")
            
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                # Tentativa 2: Modo de Recuperação
                st.warning(f"⚠️ O arquivo parece estar truncado/incompleto. Erro: {str(e)[:100]}...")
                st.info("🛠️ Tentando reparar o final do arquivo automaticamente...")
                
                repaired_data, msg = attempt_repair_json(file_bytes)
                
                if repaired_data:
                    raw_data = repaired_data
                    st.success(msg)
                    st.warning("⚠️ Nota: O último registro do arquivo pode ter sido perdido ou recuperado parcialmente.")
                else:
                    st.error("❌ Falha fatal: O arquivo está muito corrompido e não pôde ser recuperado.")
            
            # Processamento
            if raw_data:
                # Normaliza se vier dentro de 'items' ou direto como lista
                if isinstance(raw_data, dict) and "items" in raw_data:
                    raw_data = raw_data["items"]
                elif not isinstance(raw_data, list):
                    raw_data = []

                if raw_data:
                    df = process_generic(raw_data, "Motoristas")
                    st.session_state['df_Motoristas'] = df
                    st.success(f"Processamento concluído! {len(df)} motoristas carregados.")
                else:
                    st.error("O JSON é válido, mas não contém uma lista de dados.")

# --- CASO 2: OUTROS RELATÓRIOS (API) ---
else:
    if not api_token:
        st.warning("⚠️ Insira o Token da API na barra lateral para consultar Credenciados ou Clientes.")
        st.stop()

    if st.button(f"🚀 Baixar e Processar {tipo_relatorio} (API)"):
        gc.collect()
        
        urls = {
            "Credenciados": "https://sigyo.uzzipay.com/api/credenciados",
            "Clientes": "https://sigyo.uzzipay.com/api/clientes"
        }
        
        params_map = {
            "Credenciados": {'expand': 'dadosAcesso,municipio,municipio.estado,modulos', 'inline': 'false'},
            "Clientes": {'expand': 'municipio,municipio.estado,modulos,organizacao,tipo', 'inline': 'false'}
        }

        with st.spinner(f"Baixando dados... Acompanhe no Terminal (Chunks de 8MB)."):
            raw_data = fetch_data_local(urls[tipo_relatorio], api_token, params_map[tipo_relatorio])
            
        if raw_data:
            st.success(f"Sucesso! {len(raw_data)} registros baixados.")
            with st.spinner("Processando tabela..."):
                df = process_generic(raw_data, tipo_relatorio)
                st.session_state[f'df_{tipo_relatorio}'] = df
                st.rerun()

# --- VISUALIZAÇÃO COM SELEÇÃO DE COLUNAS ---

key_map = {"Motoristas": "df_Motoristas", "Credenciados": "df_Credenciados", "Clientes": "df_Clientes"}
current_key = key_map.get(tipo_relatorio)

if current_key in st.session_state:
    df = st.session_state[current_key]
    
    st.divider()
    st.subheader(f"📊 Dados de {tipo_relatorio}")
    
    # 1. Filtro Rápido
    col_search, _ = st.columns([1, 2])
    with col_search:
        search = st.text_input("🔍 Busca Rápida (Filtrar linhas):", placeholder="Nome, CNPJ, Email...")
        
    if search:
        mask = df.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_filtered = df[mask]
    else:
        df_filtered = df

    # 2. Seleção de Colunas
    st.markdown("#### 👁️ Seleção de Colunas")
    all_cols = df_filtered.columns.tolist()
    
    # Define padrões de visualização
    if tipo_relatorio == "Motoristas":
        default_cols = ['ID', 'Nome', 'CPF/CNH', 'Status', 'Empresas']
    elif tipo_relatorio == "Credenciados":
        default_cols = ['ID', 'Nome Fantasia', 'CNPJ', 'Cidade', 'Responsável']
    else:
        default_cols = ['ID', 'Nome Fantasia', 'CNPJ', 'Cidade', 'Organização']
        
    # Garante que as colunas padrão existam no dataframe
    default_cols = [c for c in default_cols if c in all_cols]
    if not default_cols: default_cols = all_cols[:5]
    
    selected_cols = st.multiselect(
        "Escolha as colunas para visualizar e baixar:",
        options=all_cols,
        default=default_cols
    )

    if not selected_cols:
        st.warning("Selecione pelo menos uma coluna.")
    else:
        df_display = df_filtered[selected_cols]
        
        # Mostra DataFrame
        st.dataframe(df_display, use_container_width=True)
        
        # Botão de Download
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"Mostrando {len(df_display)} linhas.")
        with col2:
            csv = df_display.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📥 Baixar Seleção (.csv)",
                data=csv,
                file_name=f"{tipo_relatorio}_local.csv",
                mime="text/csv",
                type="primary"
            )
