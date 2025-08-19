import streamlit as st
import pandas as pd
import database as db  # Importa nosso novo arquivo de banco de dados

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
st.success("As alterações feitas aqui serão salvas permanentemente no banco de dados.", icon="💾")

tab1, tab2, tab3, tab4 = st.tabs(["Visualizar Usuários", "Criar Usuário", "Editar Usuário", "Excluir Usuário"])

# --- ABA 1: VISUALIZAR USUÁRIOS ---
with tab1:
    st.subheader("Lista de Usuários Cadastrados")
    all_users = db.get_all_users()
    if not all_users:
        st.info("Nenhum usuário cadastrado.")
    else:
        df_users = pd.DataFrame(all_users, columns=["Nome", "Email", "Perfil"])
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
                if db.get_user(new_email):
                    st.error("Erro: Este email já está cadastrado.")
                else:
                    db.add_user(new_name, new_email, new_password, new_role)
                    st.success(f"Usuário '{new_name}' criado com sucesso!")
            else:
                st.warning("Por favor, preencha todos os campos.")

# --- ABA 3: EDITAR USUÁRIO ---
with tab3:
    st.subheader("Editar Usuário Existente")
    
    all_users_list = db.get_all_users()
    users_emails = [user['email'] for user in all_users_list]
    
    email_to_edit = st.selectbox("Selecione o email do usuário para editar", options=users_emails, index=None, placeholder="Selecione um usuário")

    if email_to_edit:
        user_data = db.get_user(email_to_edit)
        
        with st.form("edit_user_form"):
            st.write(f"Editando o usuário: **{user_data['name']}** ({user_data['email']})")
            
            edited_name = st.text_input("Nome Completo", value=user_data['name'])
            edited_role_index = 0 if user_data['role'] == 'user' else 1
            edited_role = st.selectbox("Perfil de Acesso", ["user", "admin"], index=edited_role_index)
            
            st.write("Para alterar a senha, preencha o campo abaixo. Deixe em branco para manter a senha atual.")
            edited_password = st.text_input("Nova Senha (opcional)", type="password")
            
            submitted_edit = st.form_submit_button("Salvar Alterações")

            if submitted_edit:
                db.update_user(email_to_edit, edited_name, edited_role)
                
                if edited_password:
                    db.update_user_password(email_to_edit, edited_password)
                    st.success(f"Dados e senha do usuário '{edited_name}' atualizados com sucesso!")
                else:
                    st.success(f"Dados do usuário '{edited_name}' atualizados com sucesso! (Senha não alterada)")
                
                # Força um rerun para atualizar o selectbox e o formulário
                st.rerun()

# --- ABA 4: EXCLUIR USUÁRIO ---
with tab4:
    st.subheader("Excluir Usuário")
    
    all_users_list_delete = db.get_all_users()
    current_admin_email = st.session_state['user_info']['email']
    
    users_to_delete_emails = [user['email'] for user in all_users_list_delete if user['email'] != current_admin_email]

    email_to_delete = st.selectbox("Selecione o email do usuário para excluir", options=users_to_delete_emails, index=None, placeholder="Selecione um usuário", key="delete_selectbox")

    if email_to_delete:
        user_to_delete_data = db.get_user(email_to_delete)
        st.warning(f"Você está prestes a excluir o usuário: **{user_to_delete_data['name']}** ({user_to_delete_data['email']}). Esta ação é permanente.", icon="⚠️")

        confirm_delete = st.checkbox("Sim, eu confirmo que desejo excluir este usuário.")
        
        if st.button("Excluir Usuário Permanentemente"):
            if confirm_delete:
                db.delete_user(email_to_delete)
                st.success(f"Usuário '{user_to_delete_data['name']}' foi excluído com sucesso.")
                st.rerun()
            else:
                st.error("Você precisa marcar a caixa de confirmação para poder excluir.")
