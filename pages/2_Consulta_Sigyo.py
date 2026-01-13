import streamlit as st
import pandas as pd
import requests
import json
import tempfile
import os
import gc
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Configuração da Página ---
st.set_page_config(
    page_title="Consulta Sigyo (Local)",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("💻 Consulta Sigyo (Versão Local - Blindada)")
st.caption("Modo de compatibilidade com Postman ativado.")

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
# FUNÇÕES DE REDE (MIMICANDO POSTMAN)
# ==============================================================================

def get_session():
    """Cria uma sessão HTTP que finge ser um navegador."""
    session = requests.Session()
    
    # Headers idênticos aos de um navegador moderno para evitar bloqueio
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
    # Headers específicos da requisição
    headers = {
        "Authorization": f"Bearer {token}",
        # Importante: Avisa o servidor que aceitamos GZIP (igual ao Postman)
        "Accept-Encoding": "gzip, deflate, br" 
    }

    session = get_session()
    
    fd, tmp_path = tempfile.mkstemp(suffix=".json")
    os.close(fd)

    print(f"\n[DEBUG] Iniciando download de: {url}")
    print(f"[DEBUG] Arquivo temporário: {tmp_path}")

    try:
        # Timeout aumentado para garantir que não caia por lentidão
        with session.get(url, headers=headers, params=params, stream=True, timeout=(10, 300)) as response:
            response.raise_for_status()
            
            # Tenta pegar o tamanho (pode vir vazio dependendo do servidor)
            total_size_header = response.headers.get('content-length')
            total_size = int(total_size_header) if total_size_header else 0
            
            downloaded = 0
            
            with open(tmp_path, 'wb') as f:
                # Chunk size otimizado para downloads rápidos
                for chunk in response.iter_content(chunk_size=1024 * 1024): # 1MB por chunk
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Log no terminal para acompanhamento
                        mb_downloaded = downloaded / (1024 * 1024)
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            print(f"\r[DEBUG] Baixando... {percent:.2f}% ({mb_downloaded:.2f} MB)", end="")
                        else:
                            print(f"\r[DEBUG] Recebido: {mb_downloaded:.2f} MB (Stream contínuo...)", end="")
            
        print(f"\n[DEBUG] Download finalizado. Tamanho final em disco: {downloaded / (1024*1024):.2f} MB")
        print("[DEBUG] Iniciando leitura e parse do JSON...")
        
        # Tenta ler o arquivo
        with open(tmp_path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                # Debug avançado se falhar
                print(f"\n[ERRO CRÍTICO] O JSON veio incompleto. Erro: {e}")
                
                # Lê os últimos caracteres para ver onde cortou
                f.seek(0, os.SEEK_END)
                size = f.tell()
                f.seek(max(size - 100, 0))
                tail = f.read()
                print(f"[DEBUG] Últimos 100 caracteres recebidos: {tail}")
                st.error("O servidor cortou a conexão antes de enviar o arquivo completo. Tente novamente.")
                return None
            
        print("[DEBUG] JSON carregado com sucesso na memória!")
        
        gc.collect()

        if isinstance(data, dict) and "items" in data:
            return data["items"]
        if isinstance(data, list):
            return data
        return []

    except Exception as e:
        print(f"\n[ERRO] Exceção geral: {e}")
        st.error(f"Erro local: {e}")
        return None
    finally:
        session.close()
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
                print("[DEBUG] Limpeza concluída.")
            except:
                pass

# ==============================================================================
# PROCESSADORES
# ==============================================================================

def process_generic(all_data, entity_type):
    if not all_data: return pd.DataFrame()
    
    print(f"[DEBUG] Processando {len(all_data)} registros de {entity_type}...")
    
    def safe_get(d, keys, default=''):
        val = d
        for k in keys:
            if isinstance(val, dict):
                val = val.get(k)
            else:
                return default
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
    print("[DEBUG] DataFrame criado com sucesso.")
    return df

# ==============================================================================
# LÓGICA DA INTERFACE
# ==============================================================================

if not api_token:
    st.warning("⚠️ Insira o Token da API na barra lateral.")
    st.stop()

if st.button(f"🚀 Baixar e Processar {tipo_relatorio}"):
    gc.collect()
    
    urls = {
        "Motoristas": "https://sigyo.uzzipay.com/api/motoristas",
        "Credenciados": "https://sigyo.uzzipay.com/api/credenciados",
        "Clientes": "https://sigyo.uzzipay.com/api/clientes"
    }
    
    params_map = {
        "Motoristas": {'expand': 'grupos_vinculados,modulos,empresas,empresas.municipio,empresas.municipio.estado', 'inline': 'false'},
        "Credenciados": {'expand': 'dadosAcesso,municipio,municipio.estado,modulos', 'inline': 'false'},
        "Clientes": {'expand': 'municipio,municipio.estado,modulos,organizacao,tipo', 'inline': 'false'}
    }

    url = urls[tipo_relatorio]
    params = params_map[tipo_relatorio]
    
    with st.spinner(f"Baixando dados de {tipo_relatorio}... Acompanhe o Terminal."):
        raw_data = fetch_data_local(url, api_token, params)
        
    if raw_data:
        st.success(f"Download concluído! Total de registros: {len(raw_data)}")
        
        with st.spinner("Gerando tabela..."):
            df = process_generic(raw_data, tipo_relatorio)
            st.session_state[f'df_{tipo_relatorio}'] = df
            
        st.success("Processamento finalizado!")

# --- VISUALIZAÇÃO ---

key_map = {"Motoristas": "df_Motoristas", "Credenciados": "df_Credenciados", "Clientes": "df_Clientes"}
current_key = key_map.get(tipo_relatorio)

if current_key in st.session_state:
    df = st.session_state[current_key]
    
    st.divider()
    st.subheader(f"📊 Dados de {tipo_relatorio}")
    
    # Filtro rápido
    col_filter, col_search = st.columns(2)
    with col_search:
        search = st.text_input("Busca Rápida:", placeholder="Digite para filtrar...")
        
    if search:
        mask = df.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df = df[mask]

    st.dataframe(df, use_container_width=True)
    
    col1, col2 = st.columns(2)
    with col1:
        st.info(f"Registros exibidos: {len(df)}")
    with col2:
        csv = df.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="📥 Baixar Excel/CSV",
            data=csv,
            file_name=f"{tipo_relatorio}_local.csv",
            mime="text/csv",
            type="primary"
        )
