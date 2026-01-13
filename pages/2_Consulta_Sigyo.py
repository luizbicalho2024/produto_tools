import streamlit as st
import pandas as pd
import json
import csv
import os
import tempfile
import gc
import time

# Tenta importar ijson (obrigatório para arquivos grandes)
try:
    import ijson
except ImportError:
    st.error("⚠️ Biblioteca 'ijson' não instalada. Adicione 'ijson' ao requirements.txt")
    st.stop()

# --- Configuração da Página ---
st.set_page_config(
    page_title="Consulta Sigyo",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🔍 Consulta Cadastral Sigyo (Modo Streaming)")

# --- Barra Lateral ---
with st.sidebar:
    st.header("📂 Tipo de Dados")
    tipo_relatorio = st.radio(
        "Selecione a base:",
        ["Motoristas", "Credenciados", "Clientes"],
        index=0
    )
    
    st.markdown("---")
    st.info("ℹ️ Este modo converte o JSON diretamente para CSV no disco, economizando memória e evitando travamentos.")

# ==============================================================================
# FUNÇÕES DE LIMPEZA (PROCESSADORES)
# ==============================================================================

def clean_motorista_record(d):
    """Processa registro de motorista."""
    if not isinstance(d, dict): return None
    
    # Extração segura de listas
    grupos = ", ".join([str(g.get('nome','')) for g in d.get('grupos_vinculados', []) if isinstance(g, dict)])
    modulos = ", ".join([str(m.get('nome','')) for m in d.get('modulos', []) if isinstance(m, dict)])
    
    emp_list = []
    empresas_raw = d.get('empresas')
    if isinstance(empresas_raw, list):
        for emp in empresas_raw:
            if isinstance(emp, dict):
                nome = emp.get('nome_fantasia') or emp.get('razao_social') or 'N/A'
                cnpj = emp.get('cnpj', '')
                emp_list.append(f"{nome} ({cnpj})")
    empresas = "; ".join(emp_list)

    return {
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
        'Grupos Vinculados': grupos,
        'Empresas': empresas,
        'Módulos': modulos
    }

def clean_credenciado_record(d):
    """Processa registro de credenciado."""
    if not isinstance(d, dict): return None
    muni = d.get('municipio') or {}
    estado = muni.get('estado') or {}
    dados_acesso = d.get('dadosAcesso') or {}
    
    parts = [d.get('logradouro'), str(d.get('numero') or ''), d.get('bairro'), muni.get('nome'), estado.get('sigla'), d.get('cep')]
    endereco = ", ".join([str(p) for p in parts if p])
    modulos = ", ".join([str(m.get('nome','')) for m in d.get('modulos', []) if isinstance(m, dict)])

    return {
        'ID': d.get('id'),
        'CNPJ': d.get('cnpj'),
        'Nome Fantasia': d.get('nome'),
        'Razão Social': d.get('razao_social'),
        'Email': d.get('email'),
        'Telefone': d.get('telefone'),
        'Situação': d.get('situacao'),
        'Ativo': 'Sim' if d.get('ativo') in [True, 1] else 'Não',
        'Cidade': muni.get('nome'),
        'UF': estado.get('sigla'),
        'Endereço Completo': endereco,
        'Responsável': dados_acesso.get('nome_responsavel'),
        'CPF Responsável': dados_acesso.get('cpf_responsavel'),
        'Email Responsável': dados_acesso.get('email_responsavel'),
        'Telefone Responsável': dados_acesso.get('telefone_responsavel'),
        'Taxa Adm (%)': d.get('limite_isencao_ir_tx_adm'),
        'Módulos': modulos,
        'Data Cadastro': d.get('data_cadastro')
    }

def clean_cliente_record(d):
    """Processa registro de cliente."""
    if not isinstance(d, dict): return None
    muni = d.get('municipio') or {}
    estado = muni.get('estado') or {}
    org = d.get('organizacao') or {}
    tipo = d.get('tipo') or {}
    
    parts = [d.get('logradouro'), str(d.get('numero') or ''), d.get('bairro'), muni.get('nome'), estado.get('sigla'), d.get('cep')]
    endereco = ", ".join([str(p) for p in parts if p])
    modulos = ", ".join([str(m.get('nome','')) for m in d.get('modulos', []) if isinstance(m, dict)])

    return {
        'ID': d.get('id'),
        'CNPJ': d.get('cnpj'),
        'Nome Fantasia': d.get('nome'),
        'Razão Social': d.get('razao_social'),
        'Email': d.get('email'),
        'Telefone': d.get('telefone'),
        'Ativo': 'Sim' if d.get('ativo') in [True, 1] else 'Não',
        'Suspenso': 'Sim' if d.get('suspenso') in [True, 1] else 'Não',
        'Cidade': muni.get('nome'),
        'UF': estado.get('sigla'),
        'Endereço Completo': endereco,
        'Organização': org.get('nome'),
        'Tipo Cliente': tipo.get('nome'),
        'Módulos': modulos,
        'Recolhimento DARF': 'Sim' if d.get('recolhimento_darf') in [True, 1] else 'Não',
        'Data Cadastro': d.get('data_cadastro')
    }

# ==============================================================================
# MOTOR DE CONVERSÃO (STREAMING TO CSV)
# ==============================================================================

def stream_json_to_csv(input_file, processor):
    """
    Lê o JSON via stream e escreve imediatamente em um CSV temporário.
    Isso mantém o uso de RAM próximo de zero.
    """
    # Cria arquivo temporário para o CSV
    temp_csv = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8-sig', newline='', suffix='.csv')
    csv_path = temp_csv.name
    
    count = 0
    writer = None
    status_text = st.empty()
    
    try:
        input_file.seek(0)
        
        # Tenta detectar a estrutura (Lista Raiz 'item' ou Objeto Wrapper 'items.item')
        # Vamos tentar primeiro como lista raiz
        try:
            parser = ijson.items(input_file, 'item')
            first_record = next(parser) # Pega o primeiro para testar
            
            # Se funcionou, processa o primeiro
            cleaned = processor(first_record)
            if cleaned:
                headers = list(cleaned.keys())
                writer = csv.DictWriter(temp_csv, fieldnames=headers, delimiter=';')
                writer.writeheader()
                writer.writerow(cleaned)
                count += 1
            
            # Processa o resto
            for record in parser:
                cleaned = processor(record)
                if cleaned:
                    writer.writerow(cleaned)
                    count += 1
                if count % 1000 == 0:
                    status_text.text(f"Convertendo: {count} registros processados...")
                    
        except (StopIteration, ijson.JSONError):
            # Se falhar logo de cara, reseta e tenta 'items.item' (estrutura {items: [...]})
            input_file.seek(0)
            try:
                parser = ijson.items(input_file, 'items.item')
                for record in parser:
                    cleaned = processor(record)
                    if cleaned:
                        if writer is None:
                            headers = list(cleaned.keys())
                            writer = csv.DictWriter(temp_csv, fieldnames=headers, delimiter=';')
                            writer.writeheader()
                        writer.writerow(cleaned)
                        count += 1
                    if count % 1000 == 0:
                        status_text.text(f"Convertendo: {count} registros processados...")
            except Exception as e:
                if count == 0:
                    st.error(f"Não foi possível ler a estrutura do JSON. Erro: {e}")
                    temp_csv.close()
                    return None, 0

    except ijson.JSONError as e:
        st.warning(f"⚠️ O arquivo terminou inesperadamente (JSON corrompido), mas recuperamos {count} registros com sucesso.")
    except Exception as e:
        st.error(f"Erro fatal: {e}")
    finally:
        temp_csv.close()
        status_text.empty()
        
    return csv_path, count

# ==============================================================================
# INTERFACE
# ==============================================================================

st.info("📂 **Instrução:** Faça o upload do JSON. O sistema irá convertê-lo para CSV e permitir o download, mesmo que o arquivo esteja incompleto.")

uploaded_file = st.file_uploader(f"Upload JSON de **{tipo_relatorio}**", type=['json'])

if uploaded_file is not None:
    # Escolhe o processador
    processor = None
    if tipo_relatorio == "Motoristas": processor = clean_motorista_record
    elif tipo_relatorio == "Credenciados": processor = clean_credenciado_record
    elif tipo_relatorio == "Clientes": processor = clean_cliente_record
    
    if st.button("🚀 Processar Arquivo"):
        start_time = time.time()
        with st.spinner("Lendo JSON e gerando CSV (isso não consome memória)..."):
            csv_path, total_rows = stream_json_to_csv(uploaded_file, processor)
            
        if csv_path and total_rows > 0:
            st.success(f"✅ Processamento concluído! {total_rows} registros recuperados em {time.time() - start_time:.1f}s")
            
            # --- Visualização de Amostra (Sem carregar tudo) ---
            st.subheader("👀 Prévia dos Dados (Primeiros 50 registros)")
            try:
                # Lê apenas as primeiras linhas para não travar
                df_preview = pd.read_csv(csv_path, sep=';', nrows=50)
                st.dataframe(df_preview, use_container_width=True)
            except Exception as e:
                st.warning("Não foi possível gerar a prévia visual, mas o download está disponível abaixo.")

            # --- Download do Arquivo Completo ---
            with open(csv_path, "rb") as f:
                st.download_button(
                    label=f"📥 Baixar CSV Completo ({total_rows} registros)",
                    data=f,
                    file_name=f"{tipo_relatorio.lower()}_convertido.csv",
                    mime="text/csv",
                    type="primary"
                )
            
            # Limpeza do arquivo temporário após uso (opcional, o OS limpa depois)
            # os.remove(csv_path) 
            
        elif total_rows == 0:
            st.warning("O arquivo foi lido, mas nenhum registro válido foi encontrado.")
