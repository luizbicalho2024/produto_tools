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

# Banco de dados inicial de usuários.
# Será copiado para o st.session_state para permitir modificações na sessão.
INITIAL_USERS_DB = {
    "admin@email.com": {
        "name": "Administrador",
        "hashed_password": "5994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5", # Senha: 12345
        "role": "admin"
    },
    "usuario@email.com": {
        "name": "Usuário Comum",
        "hashed_password": "5994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5", # Senha: 12345
        "role": "user"
    }
}

# --- LÓGICA DE LOGIN ---

def login_user(email, password):
    """Valida as credenciais do usuário usando a base de dados da sessão."""
    users_db = st.session_state.get('users_db', {})
    if email in users_db:
        user_data = users_db[email]
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
# Copia a base de dados inicial para o session_state se ainda não existir
if 'users_db' not in st.session_state:
    st.session_state['users_db'] = INITIAL_USERS_DB.copy()

# Se o usuário já estiver logado, mostra mensagem e botão de sair
if st.session_state.get('logged_in'):
    user_name = st.session_state['user_info']['name']
    st.success(f"Você já está logado como **{user_name}**.")
    st.info("Navegue pelas páginas no menu à esquerda.")
    if st.button("Sair"):
        # Limpa todo o session_state ao sair
        for key in st.session_state.keys():
            del st.session_state[key]
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
