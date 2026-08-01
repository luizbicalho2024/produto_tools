from __future__ import annotations

import pandas as pd
import streamlit as st

import database as db
from core.auth import render_account_sidebar, require_admin
from core.configuration import ROLE_LABELS, VALID_USER_ROLES
from core.styles import apply_global_styles, page_header

st.set_page_config(page_title="Gestão de Acesso", page_icon="👥", layout="wide")
apply_global_styles()
current_user = require_admin()
render_account_sidebar()
db.initialize_database()

page_header(
    "Gestão de Acesso Compartilhada",
    "Os usuários desta página são os mesmos do Simulador-Telemetria. Alterações afetam as duas aplicações.",
)

role_options = [role for role in ("user", "head_comercial", "admin") if role in VALID_USER_ROLES]
role_label = lambda value: ROLE_LABELS.get(value, value)

tab_view, tab_create, tab_edit, tab_delete = st.tabs(
    ["Usuários", "Novo usuário", "Editar", "Excluir"]
)

with tab_view:
    users = db.get_all_users()
    if users:
        rows = []
        for item in users:
            rows.append(
                {
                    "Usuário": item.get("username", ""),
                    "Nome": item.get("name", ""),
                    "E-mail": item.get("email", ""),
                    "Perfil": role_label(str(item.get("role") or "user")),
                    "Ativo": "Sim" if item.get("active") is not False else "Não",
                    "Criado em": item.get("created_at"),
                    "Atualizado em": item.get("updated_at"),
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum usuário cadastrado.")

with tab_create:
    with st.form("create_shared_user", clear_on_submit=True):
        username = st.text_input("Usuário de acesso")
        name = st.text_input("Nome completo")
        email = st.text_input("E-mail")
        password = st.text_input("Senha inicial", type="password")
        role = st.selectbox("Perfil", role_options, format_func=role_label)
        submitted = st.form_submit_button("Cadastrar usuário", type="primary")
    if submitted:
        if not all([username.strip(), name.strip(), email.strip(), password]):
            st.warning("Preencha todos os campos.")
        elif len(password) < 8:
            st.warning("A senha deve possuir pelo menos 8 caracteres.")
        elif db.add_user(username, name, email, password, role):
            db.add_log(
                current_user["username"],
                "Criou usuário compartilhado",
                {"usuario": username.strip().lower(), "perfil": role},
            )
            st.success("Usuário criado e disponibilizado nas duas aplicações.")
            st.rerun()
        else:
            st.error("Não foi possível criar o usuário. Verifique se o login já existe.")

with tab_edit:
    users = db.get_all_users()
    options = {
        f"{item.get('name', item.get('username', ''))} — @{item.get('username', '')}": item
        for item in users
    }
    selected_label = st.selectbox(
        "Usuário", list(options), index=None, placeholder="Selecione", key="edit_shared_user"
    )
    if selected_label:
        selected = options[selected_label]
        selected_username = str(selected.get("username") or "")
        selected_role = str(selected.get("role") or "user")
        with st.form("edit_shared_user_form"):
            new_name = st.text_input("Nome", value=str(selected.get("name") or ""))
            new_email = st.text_input("E-mail", value=str(selected.get("email") or ""))
            new_role = st.selectbox(
                "Perfil",
                role_options,
                index=role_options.index(selected_role) if selected_role in role_options else 0,
                format_func=role_label,
            )
            active = st.checkbox("Acesso ativo", value=selected.get("active") is not False)
            new_password = st.text_input("Nova senha opcional", type="password")
            submitted = st.form_submit_button("Salvar alterações", type="primary")
        if submitted:
            if selected_username == current_user["username"] and not active:
                st.error("Você não pode desativar a própria conta durante a sessão.")
            elif new_password and len(new_password) < 8:
                st.warning("A nova senha deve possuir pelo menos 8 caracteres.")
            elif db.update_user(selected_username, new_name, new_email, new_role, active):
                if new_password:
                    db.update_user_password(selected_username, new_password)
                db.add_log(
                    current_user["username"],
                    "Editou usuário compartilhado",
                    {"usuario": selected_username, "perfil": new_role, "ativo": active},
                )
                st.success("Usuário atualizado nas duas aplicações.")
                st.rerun()
            else:
                st.error("Não foi possível atualizar o usuário.")

with tab_delete:
    users = [
        item
        for item in db.get_all_users()
        if str(item.get("username") or "") != current_user["username"]
    ]
    options = {
        f"{item.get('name', item.get('username', ''))} — @{item.get('username', '')}": item
        for item in users
    }
    selected_label = st.selectbox(
        "Usuário a excluir", list(options), index=None, placeholder="Selecione", key="delete_shared_user"
    )
    if selected_label:
        selected = options[selected_label]
        selected_username = str(selected.get("username") or "")
        st.warning(
            f"A exclusão de **@{selected_username}** será permanente e afetará também o Simulador-Telemetria."
        )
        confirmation = st.text_input("Digite o usuário para confirmar")
        if st.button(
            "Excluir usuário",
            type="primary",
            disabled=confirmation.strip().lower() != selected_username.lower(),
        ):
            if db.delete_user(selected_username):
                db.add_log(
                    current_user["username"],
                    "Excluiu usuário compartilhado",
                    {"usuario": selected_username},
                )
                st.success("Usuário excluído das duas aplicações.")
                st.rerun()
            else:
                st.error("A exclusão foi bloqueada. O último administrador ativo não pode ser removido.")
