import streamlit as st
import pandas as pd
import hashlib

# --- FUNÇÕES AUXILIARES (REPETIDAS PARA INDEPENDÊNCIA DA PÁGINA) ---

def make_hashes(password):
    """Cria um hash SHA256 para uma senha."""
    return hashlib.sha256(str.encode(password)).hexdigest()

# --- VERIFICAÇÃO DE LOGIN E PERMISSÃO DE ADMIN ---
st.set_page_config(page_title="Gerenciamento de Usuários", layout="wide")

if not st.session_state.get('logged_in'):
    st.error("🔒 Você precisa estar logado para acessar esta página.")
    st.info("Por favor, retorne à página de Login e insira suas credenciais.")
    st.stop()

if st.session_state.get('user_info', {}).get('role') != 'admin':
    st.error("🚫 Acesso Negado. Esta página é restrita a administradores.")
    st.stop()

# --- PÁGINA DE GERENCIAMENTO ---

st.title("🛠️ Gerenciamento de Usuários")
st.warning("Atenção: As alterações feitas aqui (criar, editar, excluir) são temporárias e serão perdidas ao reiniciar a aplicação.", icon="⚠️")

# Usa abas para organizar as funcionalidades CRUD
tab1, tab2, tab3, tab4 = st.tabs(["Visualizar Usuários", "Criar Usuário", "Editar Usuário", "Excluir Usuário"])

# --- ABA 1: VISUALIZAR USUÁRIOS ---
with tab1:
    st.subheader("Lista de Usuários Cadastrados")
    if not st.session_state.get('users_db'):
        st.info("Nenhum usuário cadastrado.")
    else:
        # Prepara os dados para exibição segura (sem o hash da senha)
        users_view = []
        for email, data in st.session_state.users_db.items():
            users_view.append({
                "Nome": data["name"],
                "Email": email,
                "Perfil": data["role"]
            })
        
        df_users = pd.DataFrame(users_view)
        st.dataframe(df_users, use_container_width=True, hide_index=True)

# --- ABA 2: CRIAR USUÁRIO ---
with tab2:
    st.subheader("Criar Novo Usuário")
    with st.form("create_user_form", clear_on_submit=True):
        new_name = st.text_input("Nome Completo")
        new_email = st.text_input("Email")
        new_role = st.selectbox("Perfil de Acesso", ["user", "admin"])
        new_password = st.text_input("Senha", type="password")
        
        submitted_create = st.form_submit_button("Criar Usuário")
        
        if submitted_create:
            if new_name and new_email and new_role and new_password:
                if new_email in st.session_state.users_db:
                    st.error("Erro: Este email já está cadastrado.")
                else:
                    st.session_state.users_db[new_email] = {
                        "name": new_name,
                        "hashed_password": make_hashes(new_password),
                        "role": new_role
                    }
                    st.success(f"Usuário '{new_name}' criado com sucesso!")
            else:
                st.warning("Por favor, preencha todos os campos.")

# --- ABA 3: EDITAR USUÁRIO ---
with tab3:
    st.subheader("Editar Usuário Existente")
    
    users_emails = list(st.session_state.users_db.keys())
    email_to_edit = st.selectbox("Selecione o email do usuário para editar", options=users_emails, index=None, placeholder="Selecione um usuário")

    if email_to_edit:
        user_data = st.session_state.users_db[email_to_edit]
        
        with st.form("edit_user_form"):
            st.write(f"Editando o usuário: **{user_data['name']}** ({email_to_edit})")
            
            edited_name = st.text_input("Nome Completo", value=user_data['name'])
            edited_role = st.selectbox("Perfil de Acesso", ["user", "admin"], index=0 if user_data['role'] == 'user' else 1)
            
            st.write("Para alterar a senha, preencha o campo abaixo. Deixe em branco para manter a senha atual.")
            edited_password = st.text_input("Nova Senha (opcional)", type="password")
            
            submitted_edit = st.form_submit_button("Salvar Alterações")

            if submitted_edit:
                # Atualiza os dados
                st.session_state.users_db[email_to_edit]['name'] = edited_name
                st.session_state.users_db[email_to_edit]['role'] = edited_role
                
                # Se uma nova senha foi fornecida, atualiza o hash
                if edited_password:
                    st.session_state.users_db[email_to_edit]['hashed_password'] = make_hashes(edited_password)
                    st.success(f"Dados e senha do usuário '{edited_name}' atualizados com sucesso!")
                else:
                    st.success(f"Dados do usuário '{edited_name}' atualizados com sucesso! (Senha não alterada)")

# --- ABA 4: EXCLUIR USUÁRIO ---
with tab4:
    st.subheader("Excluir Usuário")
    
    # Previne que o admin se auto-delete
    current_admin_email = st.session_state['user_info'].get('email', list(st.session_state.users_db.keys())[0])
    users_to_delete_emails = [email for email in st.session_state.users_db.keys() if email != current_admin_email]

    email_to_delete = st.selectbox("Selecione o email do usuário para excluir", options=users_to_delete_emails, index=None, placeholder="Selecione um usuário")

    if email_to_delete:
        user_to_delete_data = st.session_state.users_db[email_to_delete]
        st.warning(f"Você está prestes a excluir o usuário: **{user_to_delete_data['name']}** ({email_to_delete}). Esta ação não pode ser desfeita.", icon="⚠️")

        confirm_delete = st.checkbox("Sim, eu confirmo que desejo excluir este usuário.")
        
        if st.button("Excluir Usuário Permanentemente"):
            if confirm_delete:
                del st.session_state.users_db[email_to_delete]
                st.success(f"Usuário '{user_to_delete_data['name']}' foi excluído com sucesso.")
                st.rerun() # Atualiza a lista de seleção
            else:
                st.error("Você precisa marcar a caixa de confirmação para poder excluir.")
