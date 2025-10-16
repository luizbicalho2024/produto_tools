import streamlit as st
import database as db  # Importa nosso novo arquivo de banco de dados
import hashlib

# --- FUNÇÕES DE HASH E LOGIN ---

def check_hashes(password, hashed_text):
    """Verifica se o hash da senha corresponde ao hash armazenado."""
    return hashlib.sha256(str.encode(password)).hexdigest() == hashed_text

def login_user(email, password):
    """Valida as credenciais do usuário consultando o banco de dados."""
    user_data = db.get_user(email)
    if user_data and check_hashes(password, user_data['password']):
        # Retorna um dicionário para manter a consistência com o session_state
        return {
            "name": user_data['name'],
            "role": user_data['role'],
            "email": user_data['email']
        }
    return None

# --- INTERFACE DA PÁGINA DE LOGIN ---
st.set_page_config(page_title="Login", layout="centered")

# Cria a tabela de usuários e o admin inicial na primeira execução
db.create_usertable()
db.setup_initial_admin()

# Inicializa o estado da sessão
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
if 'user_info' not in st.session_state:
    st.session_state['user_info'] = None

# Se o usuário já estiver logado
if st.session_state.get('logged_in'):
    user_name = st.session_state['user_info']['name']
    st.success(f"Você já está logado como **{user_name}**.")
    st.info("Navegue pelas páginas no menu à esquerda.")
    if st.button("Sair"):
        for key in st.session_state.keys():
            del st.session_state[key]
        st.rerun()
else:
    # Formulário de login
    st.title("Sistema de Acesso")
    st.write("Por favor, insira suas credenciais para continuar.")

    with st.form("login_form"):
        email = st.text_input("Email", placeholder="")
        password = st.text_input("Senha", type="password", placeholder="")
        submitted = st.form_submit_button("Entrar")

        if submitted:
            user_data = login_user(email, password)
            if user_data:
                st.session_state['logged_in'] = True
                st.session_state['user_info'] = user_data
                st.rerun()
            else:
                st.error("Email ou senha inválidos. Tente novamente.")
