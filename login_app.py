import streamlit as st
import hashlib

# --- CONFIGURAÇÃO DE SEGURANÇA E "BANCO DE DADOS" ---

def make_hashes(password):
    """Cria um hash SHA256 para uma senha."""
    return hashlib.sha256(str.encode(password)).hexdigest()

def check_hashes(password, hashed_text):
    """Verifica se o hash da senha corresponde ao hash armazenado."""
    if make_hashes(password) == hashed_text:
        return True
    return False

# "Banco de dados" de usuários. A senha para ambos é "12345"
USERS_DB = {
    "admin@email.com": {
        "name": "Administrador",
        "hashed_password": "8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92",
        "role": "admin"
    },
    "usuario@email.com": {
        "name": "Usuário Comum",
        "hashed_password": "8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92",
        "role": "user"
    }
}

def login_user(email, password):
    """Valida as credenciais do usuário."""
    if email in USERS_DB:
        user_data = USERS_DB[email]
        if check_hashes(password, user_data["hashed_password"]):
            return user_data
    return None

# --- INTERFACE DA PÁGINA DE LOGIN ---

st.set_page_config(page_title="Login", layout="centered")

# Inicializa o estado da sessão se não existir
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
if 'user_info' not in st.session_state:
    st.session_state['user_info'] = None

# Se o usuário já estiver logado, mostra mensagem e botão de sair
if st.session_state.get('logged_in'):
    user_name = st.session_state['user_info']['name']
    st.success(f"Você já está logado como **{user_name}**.")
    st.info("Navegue para a página 'Credenciados' no menu à esquerda para visualizar os dados.")
    if st.button("Sair"):
        st.session_state['logged_in'] = False
        st.session_state['user_info'] = None
        st.rerun()
else:
    # Se não estiver logado, mostra o formulário de login
    st.title("Sistema de Acesso")
    st.write("Por favor, insira suas credenciais para continuar.")

    with st.form("login_form"):
        email = st.text_input("Email", placeholder="admin@email.com")
        password = st.text_input("Senha", type="password", placeholder="12345")
        submitted = st.form_submit_button("Entrar")

        if submitted:
            if email and password:
                user_data = login_user(email, password)
                if user_data:
                    st.session_state['logged_in'] = True
                    st.session_state['user_info'] = user_data
                    st.rerun()
                else:
                    st.error("Email ou senha inválidos. Tente novamente.")
            else:
                st.warning("Por favor, preencha ambos os campos.")
