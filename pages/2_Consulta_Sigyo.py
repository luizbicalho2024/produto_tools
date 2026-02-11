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
    page_title="Consulta Sigyo",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("💻 Consulta Sigyo")
st.caption("Versão API Completa | Paginação Automática para Motoristas")

# --- Barra Lateral ---
with st.sidebar:
    st.header("Configurações de Acesso")
    
    api_token = st.text_input(
        "Token de Acesso (Bearer)", 
        value="", 
        type="password",
        placeholder="Cole seu token aqui...",
        help="Por segurança, o token não é salvo. Insira-o manualmente."
    )
    
    st.markdown("---")
    st.header("Selecione a Base")
    tipo_relatorio = st.radio(
        "Qual cadastro deseja consultar?",
        ["Motoristas", "Credenciados", "Clientes"],
        index=0
    )

# ==============================================================================
# FUNÇÕES DE REDE
# ==============================================================================

def get_session():
    """Cria uma sessão HTTP com política de retry."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Connection": "keep-alive"
    })
    
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_paginated_data(url, token, params):
    """Lida com a paginação da API para evitar travamentos."""
    all_items = []
    current_page = 1
    session = get_session()
    headers = {"Authorization": f"Bearer {token}"}
    
    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        while True:
            params['page'] = current_page
            params['limit'] = 100
            
            response = session.get(url, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            items = data.get("items", [])
            if not items:
                break
                
            all_items.extend(items)
            
            # Cálculo de progresso (baseado no meta da API se disponível, ou loop contínuo)
            meta = data.get("meta", {})
            total_pages = meta.get("last_page", current_page + 1)
            
            status_text.text(f"Baixando página {current_page} de {total_pages}...")
            progress_bar.progress(min(current_page / total_pages, 1.0))
            
            if current_page >= total_pages:
                break
            current_page += 1

        status_text.empty()
        progress_bar.empty()
        return all_items

    except Exception as e:
        st.error(f"Erro na requisição: {e}")
        return None
    finally:
        session.close()

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_data_standard(url, token, params=None):
    """Consulta padrão para endpoints sem paginação complexa ou menores."""
    headers = {"Authorization": f"Bearer {token}"}
    session = get_session()
    try:
        response = session.get(url, headers=headers, params=params, timeout=300)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict) and "items" in data:
            return data["items"]
        return data if isinstance(data, list) else []
    except Exception as e:
        st.error(f"Erro: {e}")
        return None
    finally:
        session.close()

# ==============================================================================
# PROCESSADOR DE DADOS
# ==============================================================================

def process_generic(all_data, entity_type):
    if not all_data: return pd.DataFrame()
    
    processed_rows = []
    
    for d in all_data:
        if not isinstance(d, dict): continue
        
        row = {}
        row['ID'] = d.get('id')
        row['Email'] = d.get('email')
        row['Telefone'] = d.get('telefone')
        row['Ativo'] = 'Sim' if d.get('ativo') in [True, 1] else 'Não'
        
        # Módulos (Comum a todos)
        modulos = d.get('modulos', [])
        row['Módulos'] = ", ".join([m.get('nome', '') for m in modulos if isinstance(m, dict)])

        if entity_type == "Motoristas":
            row['Nome'] = d.get('nome')
            row['CPF/CNH'] = d.get('cnh')
            row['Categoria'] = d.get('cnh_categoria')
            row['Status'] = d.get('status')
            
            # Grupos Vinculados
            grupos = d.get('grupos_vinculados', [])
            row['Grupos'] = ", ".join([g.get('nome', '') for g in grupos if isinstance(g, dict)])
            
            # Empresas e Municípios
            empresas_info = []
            for emp in d.get('empresas', []):
                if isinstance(emp, dict):
                    nome_emp = emp.get('nome') or emp.get('razao_social') or 'N/A'
                    cidade = emp.get('municipio', {}).get('nome', 'N/A')
                    uf = emp.get('municipio', {}).get('estado', {}).get('sigla', '')
                    empresas_info.append(f"{nome_emp} ({cidade}-{uf})")
            row['Empresas/Localidade'] = "; ".join(empresas_info)
            
        elif entity_type == "Credenciados":
            row['CNPJ'] = d.get('cnpj')
            row['Nome Fantasia'] = d.get('nome')
            row['Razão Social'] = d.get('razao_social')
            row['Cidade'] = d.get('municipio', {}).get('nome', '')
            
        elif entity_type == "Clientes":
            row['CNPJ'] = d.get('cnpj')
            row['Nome Fantasia'] = d.get('nome')
            row['Razão Social'] = d.get('razao_social')
            row['Cidade'] = d.get('municipio', {}).get('nome', '')
            row['Organização'] = d.get('organizacao', {}).get('nome', '')

        processed_rows.append(row)

    df = pd.DataFrame(processed_rows)
    return df

# ==============================================================================
# LÓGICA DA INTERFACE
# ==============================================================================

if not api_token:
    st.warning("⚠️ Insira o Token de Acesso na barra lateral para começar.")
    st.stop()

if st.button(f"🚀 Iniciar Consulta de {tipo_relatorio}"):
    gc.collect()
    
    urls = {
        "Motoristas": "https://sigyo.uzzipay.com/api/motoristas",
        "Credenciados": "https://sigyo.uzzipay.com/api/credenciados",
        "Clientes": "https://sigyo.uzzipay.com/api/clientes"
    }
    
    # Configuração de expansão conforme a necessidade da API
    params_map = {
        "Motoristas": {
            'expand': 'grupos_vinculados,modulos,empresas,empresas.municipio,empresas.municipio.estado',
            'inline': 'false'
        },
        "Credenciados": {
            'expand': 'dadosAcesso,municipio,municipio.estado,modulos', 
            'inline': 'false'
        },
        "Clientes": {
            'expand': 'municipio,municipio.estado,modulos,organizacao,tipo', 
            'inline': 'false'
        }
    }

    with st.spinner(f"Processando {tipo_relatorio}..."):
        if tipo_relatorio == "Motoristas":
            raw_data = fetch_paginated_data(urls[tipo_relatorio], api_token, params_map[tipo_relatorio])
        else:
            raw_data = fetch_data_standard(urls[tipo_relatorio], api_token, params_map[tipo_relatorio])
            
        if raw_data:
            df = process_generic(raw_data, tipo_relatorio)
            st.session_state[f'df_{tipo_relatorio}'] = df
            st.success(f"Sucesso! {len(df)} registros carregados.")
            st.rerun()

# --- VISUALIZAÇÃO ---

key_map = {"Motoristas": "df_Motoristas", "Credenciados": "df_Credenciados", "Clientes": "df_Clientes"}
current_key = key_map.get(tipo_relatorio)

if current_key in st.session_state:
    df = st.session_state[current_key]
    
    st.divider()
    st.subheader(f"📊 Tabela de {tipo_relatorio}")
    
    search = st.text_input("🔍 Filtro rápido:", placeholder="Digite para buscar...")
    if search:
        df_display = df[df.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]
    else:
        df_display = df

    st.dataframe(df_display, use_container_width=True)
    
    csv = df_display.to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        label="📥 Baixar em CSV",
        data=csv,
        file_name=f"consulta_{tipo_relatorio.lower()}.csv",
        mime="text/csv"
    )
