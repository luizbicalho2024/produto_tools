import streamlit as st
import hashlib
import pandas as pd

# --- CONFIGURAÇÃO DE SEGURANÇA E "BANCO DE DADOS" ---

# Função para criar um hash seguro da senha
def make_hashes(password):
    """Cria um hash SHA256 para uma senha."""
    return hashlib.sha256(str.encode(password)).hexdigest()

def check_hashes(password, hashed_text):
    """Verifica se o hash da senha corresponde ao hash armazenado."""
    if make_hashes(password) == hashed_text:
        return True
    return False

# "Banco de dados" de usuários hardcoded no script.
# Em uma aplicação real, isso viria de um banco de dados seguro.
# As senhas aqui já estão "hasheadas". A senha original para ambos é "12345"
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

# --- LÓGICA DE LOGIN ---

def login_user(email, password):
    """
    Valida as credenciais do usuário.
    Retorna os detalhes do usuário se o login for bem-sucedido, caso contrário, None.
    """
    if email in USERS_DB:
        user_data = USERS_DB[email]
        if check_hashes(password, user_data["hashed_password"]):
            return user_data
    return None

# --- INICIALIZAÇÃO DO APP STREAMLIT ---

st.set_page_config(page_title="Tela de Login", layout="centered")

# Inicializa o estado da sessão se não existir
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
if 'user_info' not in st.session_state:
    st.session_state['user_info'] = None

# --- ESTRUTURA DA PÁGINA ---

# Se o usuário não estiver logado, mostra a tela de login
if not st.session_state['logged_in']:
    st.title("Sistema de Acesso")
    st.write("Por favor, insira suas credenciais para continuar.")

    with st.form("login_form"):
        email = st.text_input("Email", placeholder="seuemail@exemplo.com")
        password = st.text_input("Senha", type="password", placeholder="********")
        submitted = st.form_submit_button("Entrar")

        if submitted:
            if email and password:
                user_data = login_user(email, password)
                if user_data:
                    st.session_state['logged_in'] = True
                    st.session_state['user_info'] = user_data
                    st.rerun()  # Reroda o script para refletir o estado de login
                else:
                    st.error("Email ou senha inválidos. Tente novamente.")
            else:
                st.warning("Por favor, preencha ambos os campos.")

# Se o usuário estiver logado, mostra a tela correspondente ao seu perfil
else:
    user_info = st.session_state['user_info']
    user_name = user_info['name']
    user_role = user_info['role']

    # --- CABEÇALHO COMUM PARA USUÁRIOS LOGADOS ---
    col1, col2 = st.columns([4, 1])
    with col1:
        st.title(f"Bem-vindo, {user_name}!")
        st.markdown(f"Você está logado como **{user_role.capitalize()}**.")
    with col2:
        if st.button("Sair"):
            st.session_state['logged_in'] = False
            st.session_state['user_info'] = None
            st.rerun()

    st.markdown("---")

    # --- CONTEÚDO ESPECÍFICO POR PERFIL ---

    if user_role == 'admin':
        st.header("Painel do Administrador")
        st.info("Aqui você tem acesso total às configurações e dados do sistema.")
        
        # Exemplo de funcionalidade de admin: Visualizar a lista de usuários
        st.subheader("Gerenciamento de Usuários")
        
        # Cria um DataFrame para melhor visualização
        df_users = pd.DataFrame(USERS_DB).T.reset_index()
        df_users.rename(columns={'index': 'email', 'name': 'Nome', 'role': 'Perfil'}, inplace=True)
        
        # Remove a coluna de senha hasheada para não exibir na tela
        df_display = df_users[['Nome', 'email', 'Perfil']]
        
        st.dataframe(df_display, use_container_width=True, hide_index=True)
        st.success("Painel de Admin carregado com sucesso!")

    elif user_role == 'user':
        st.header("Painel do Usuário")
        st.info("Esta é a sua área de usuário com acesso limitado.")

        # Exemplo de funcionalidade de usuário
        st.subheader("Suas Informações")
        st.write(f"**Nome:** {user_name}")
        st.write(f"**Email:** {list(USERS_DB.keys())[list(USERS_DB.values()).index(user_info)]}") # Busca o email pela info
        st.write(f"**Perfil:** {user_role.capitalize()}")
        
        st.success("Painel de Usuário carregado com sucesso!")
