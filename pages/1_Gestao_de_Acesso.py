from __future__ import annotations

from datetime import datetime

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
    "Gestão de Acesso e Auditoria",
    "Administre os usuários compartilhados com o Simulador-Telemetria e acompanhe as ações realizadas no Produto Tools.",
)

role_options = [role for role in ("user", "head_comercial", "admin") if role in VALID_USER_ROLES]
role_label = lambda value: ROLE_LABELS.get(value, value)
users = db.get_all_users()
active_users = [item for item in users if item.get("active") is not False]
admins = [item for item in active_users if item.get("role") == "admin"]
logs = db.get_activity_logs(limit=500)

m1, m2, m3, m4 = st.columns(4)
m1.metric("Usuários cadastrados", len(users))
m2.metric("Acessos ativos", len(active_users))
m3.metric("Administradores", len(admins))
m4.metric("Eventos auditados", len(logs))

view_tab, create_tab, edit_tab, delete_tab, audit_tab = st.tabs(
    ["Diretório", "Novo usuário", "Editar acesso", "Excluir", "Auditoria"]
)

with view_tab:
    search = st.text_input("Pesquisar usuário", placeholder="Nome, login ou e-mail")
    selected_roles = st.multiselect("Perfis", role_options, format_func=role_label)
    status_filter = st.radio("Situação", ["Todos", "Ativos", "Inativos"], horizontal=True)
    filtered = []
    for item in users:
        text = " ".join([str(item.get("username", "")), str(item.get("name", "")), str(item.get("email", ""))]).lower()
        if search and search.lower() not in text:
            continue
        if selected_roles and item.get("role") not in selected_roles:
            continue
        if status_filter == "Ativos" and item.get("active") is False:
            continue
        if status_filter == "Inativos" and item.get("active") is not False:
            continue
        filtered.append(item)
    if filtered:
        rows = [{
            "Usuário": item.get("username", ""),
            "Nome": item.get("name", ""),
            "E-mail": item.get("email", ""),
            "Perfil": role_label(str(item.get("role") or "user")),
            "Situação": "Ativo" if item.get("active") is not False else "Inativo",
            "Criado em": str(item.get("created_at") or "")[:19].replace("T", " "),
            "Atualizado em": str(item.get("updated_at") or "")[:19].replace("T", " "),
        } for item in filtered]
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum usuário atende aos filtros.")

with create_tab:
    st.info("O usuário criado aqui poderá usar as mesmas credenciais no Produto Tools e no Simulador-Telemetria.")
    with st.form("create_shared_user", clear_on_submit=True):
        left, right = st.columns(2)
        username = left.text_input("Usuário de acesso")
        name = right.text_input("Nome completo")
        email = left.text_input("E-mail")
        role = right.selectbox("Perfil", role_options, format_func=role_label)
        password = st.text_input("Senha inicial", type="password", help="Mínimo de oito caracteres.")
        submitted = st.form_submit_button("Cadastrar usuário", type="primary", use_container_width=True)
    if submitted:
        if not all([username.strip(), name.strip(), email.strip(), password]):
            st.warning("Preencha todos os campos.")
        elif len(password) < 8:
            st.warning("A senha deve possuir pelo menos oito caracteres.")
        elif db.add_user(username, name, email, password, role):
            db.add_log(current_user["username"], "Criou usuário compartilhado", {"usuario": username.strip().lower(), "perfil": role})
            st.success("Usuário criado e disponibilizado nas duas aplicações.")
            st.rerun()
        else:
            st.error("Não foi possível criar o usuário. Verifique se o login já existe.")

with edit_tab:
    options = {f"{item.get('name', item.get('username', ''))} — @{item.get('username', '')}": item for item in users}
    selected_label = st.selectbox("Usuário", list(options), index=None, placeholder="Selecione", key="edit_shared_user")
    if selected_label:
        selected = options[selected_label]
        selected_username = str(selected.get("username") or "")
        selected_role = str(selected.get("role") or "user")
        with st.form("edit_shared_user_form"):
            left, right = st.columns(2)
            new_name = left.text_input("Nome", value=str(selected.get("name") or ""))
            new_email = right.text_input("E-mail", value=str(selected.get("email") or ""))
            new_role = left.selectbox("Perfil", role_options, index=role_options.index(selected_role) if selected_role in role_options else 0, format_func=role_label)
            active = right.checkbox("Acesso ativo", value=selected.get("active") is not False)
            new_password = st.text_input("Nova senha opcional", type="password")
            submitted = st.form_submit_button("Salvar alterações", type="primary", use_container_width=True)
        if submitted:
            if selected_username == current_user["username"] and not active:
                st.error("Você não pode desativar a própria conta durante a sessão.")
            elif new_password and len(new_password) < 8:
                st.warning("A nova senha deve possuir pelo menos oito caracteres.")
            elif db.update_user(selected_username, new_name, new_email, new_role, active):
                if new_password:
                    db.update_user_password(selected_username, new_password)
                db.add_log(current_user["username"], "Editou usuário compartilhado", {"usuario": selected_username, "perfil": new_role, "ativo": active, "senha_alterada": bool(new_password)})
                st.success("Usuário atualizado nas duas aplicações.")
                st.rerun()
            else:
                st.error("Não foi possível atualizar o usuário.")

with delete_tab:
    deletable = [item for item in users if str(item.get("username") or "") != current_user["username"]]
    options = {f"{item.get('name', item.get('username', ''))} — @{item.get('username', '')}": item for item in deletable}
    selected_label = st.selectbox("Usuário a excluir", list(options), index=None, placeholder="Selecione", key="delete_shared_user")
    if selected_label:
        selected = options[selected_label]
        selected_username = str(selected.get("username") or "")
        st.warning(f"A exclusão de **@{selected_username}** será permanente e afetará também o Simulador-Telemetria.")
        confirmation = st.text_input("Digite o usuário para confirmar")
        if st.button("Excluir usuário", type="primary", disabled=confirmation.strip().lower() != selected_username.lower()):
            if db.delete_user(selected_username):
                db.add_log(current_user["username"], "Excluiu usuário compartilhado", {"usuario": selected_username})
                st.success("Usuário excluído das duas aplicações.")
                st.rerun()
            else:
                st.error("A exclusão foi bloqueada. O último administrador ativo não pode ser removido.")

with audit_tab:
    action_filter = st.text_input("Filtrar ações", placeholder="Login, salvou fluxo, publicou...")
    actor_filter = st.selectbox("Responsável", ["Todos", *sorted({str(item.get('user') or '') for item in logs if item.get('user')})])
    filtered_logs = []
    for item in logs:
        if action_filter and action_filter.lower() not in str(item.get("action") or "").lower():
            continue
        if actor_filter != "Todos" and item.get("user") != actor_filter:
            continue
        filtered_logs.append({
            "Data": str(item.get("timestamp") or "")[:19].replace("T", " "),
            "Usuário": item.get("user"),
            "Ação": item.get("action"),
            "Detalhes": str(item.get("details") or ""),
        })
    if filtered_logs:
        st.dataframe(pd.DataFrame(filtered_logs), use_container_width=True, hide_index=True)
        st.download_button("Exportar auditoria CSV", pd.DataFrame(filtered_logs).to_csv(index=False).encode("utf-8-sig"), "produto_tools_auditoria.csv", "text/csv")
    else:
        st.info("Nenhum evento de auditoria foi encontrado.")
