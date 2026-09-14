from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

from core.auth import render_account_sidebar, require_login
from core.configuration import WORKFLOW_STATUS_LABELS
from core.styles import apply_global_styles, page_header
from services.portfolio_service import portfolio_rows
from services.project_repository import list_projects

st.set_page_config(page_title="Central de Processos", page_icon="🗂️", layout="wide")
apply_global_styles()
user = require_login()
render_account_sidebar()
username = str(user["username"]).strip().lower()
is_admin = user.get("role") == "admin"

def fmt(value) -> str:
    if isinstance(value, datetime):
        try:
            return value.astimezone().strftime("%d/%m/%Y %H:%M")
        except Exception:
            return value.strftime("%d/%m/%Y %H:%M")
    return str(value or "")[:16].replace("T", " ")

page_header(
    "Central de Processos",
    "Portfólio, qualidade, governança, execução e ferramentas de melhoria dos processos acessíveis ao seu usuário.",
)

nav1, nav2, nav3, nav4, nav5 = st.columns(5)
if nav1.button("Projetos", use_container_width=True):
    st.switch_page("pages/3_Gestao_de_Projetos.py")
if nav2.button("Mapa", use_container_width=True):
    st.switch_page("pages/4_Mapa_de_Relacoes.py")
if nav3.button("Editor", type="primary", use_container_width=True):
    st.switch_page("pages/5_Editor_de_Fluxos.py")
if nav4.button("Melhoria", use_container_width=True):
    st.switch_page("pages/6_Central_de_Melhoria.py")
if nav5.button("Execução", use_container_width=True):
    st.switch_page("pages/10_Execucao_de_Processos.py")

projects = list_projects(username, include_all=is_admin, is_admin=is_admin)
project_by_id = {item["id"]: item for item in projects}
rows_raw = portfolio_rows(username, include_all=is_admin)

search = st.text_input("Pesquisar no portfólio", placeholder="Nome, responsável, status ou projeto")
project_filter = st.selectbox(
    "Projeto",
    [""] + [item["id"] for item in projects],
    format_func=lambda value: "Todos os projetos" if not value else project_by_id[value]["name"],
)
status_filter = st.multiselect(
    "Status",
    options=["draft", "in_review", "approved", "published", "archived"],
    default=[],
    format_func=lambda value: WORKFLOW_STATUS_LABELS.get(value, value),
)

rows = []
for item in rows_raw:
    if project_filter and item.get("project_id") != project_filter:
        continue
    searchable = " ".join([
        str(item.get("name") or ""), str(item.get("owner_username") or ""),
        str(item.get("workflow_status") or ""), str(project_by_id.get(item.get("project_id"), {}).get("name", "")),
    ]).lower()
    if search and search.lower() not in searchable:
        continue
    if status_filter and item.get("workflow_status") not in status_filter:
        continue
    analysis = item["analysis"]
    details = item["quality_details"]
    rows.append({
        "ID": item["id"],
        "Processo": item["name"],
        "Projeto": project_by_id.get(item.get("project_id"), {}).get("name", "Fluxo avulso"),
        "Projeto ID": item.get("project_id") or "",
        "Status": WORKFLOW_STATUS_LABELS.get(item.get("workflow_status"), item.get("workflow_status")),
        "Proprietário": item.get("owner_username"),
        "Versão": item.get("current_version"),
        "Revisão": item.get("revision"),
        "Qualidade": analysis["quality_score"],
        "Elementos": analysis["counts"]["nodes"],
        "Decisões": analysis["counts"]["decisions"],
        "Comentários abertos": item["open_comments"],
        "Cards com problema": ", ".join(dict.fromkeys(detail["Card"] for detail in details)) or "Nenhum",
        "Tipos de problema": ", ".join(dict.fromkeys(detail["Problema"] for detail in details)) or "Nenhum",
        "Detalhes de qualidade": details,
        "Atualizado em": fmt(item.get("updated_at")),
    })

published = sum(1 for row in rows if row["Status"] == WORKFLOW_STATUS_LABELS["published"])
in_review = sum(1 for row in rows if row["Status"] == WORKFLOW_STATUS_LABELS["in_review"])
average_quality = round(sum(row["Qualidade"] for row in rows) / len(rows)) if rows else 0
open_comments = sum(row["Comentários abertos"] for row in rows)
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Processos", len(rows))
col2.metric("Publicados", published)
col3.metric("Em revisão", in_review)
col4.metric("Qualidade média", f"{average_quality}/100")
col5.metric("Comentários", open_comments)

if not rows:
    st.info("Nenhum processo atende aos filtros selecionados.")
    st.stop()

frame = pd.DataFrame([{key: value for key, value in row.items() if key != "Detalhes de qualidade"} for row in rows])
st.dataframe(
    frame,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Qualidade": st.column_config.ProgressColumn("Qualidade", min_value=0, max_value=100, format="%d/100"),
        "Cards com problema": st.column_config.TextColumn("Cards com problema", width="large"),
        "Tipos de problema": st.column_config.TextColumn("Tipos de problema", width="large"),
        "ID": None,
        "Projeto ID": None,
    },
)
with st.expander("Entender e corrigir os problemas de qualidade", expanded=False):
    quality_name = st.selectbox("Processo", [row["Processo"] for row in rows], key="central_quality_process")
    quality_row = next(row for row in rows if row["Processo"] == quality_name)
    details = quality_row.get("Detalhes de qualidade") or []
    if details:
        st.dataframe(
            pd.DataFrame(details),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Card ID": None,
                "Card": st.column_config.TextColumn("Card afetado", width="medium"),
                "Por que importa": st.column_config.TextColumn("Por que isso é um problema", width="large"),
                "Como corrigir": st.column_config.TextColumn("Como corrigir", width="large"),
            },
        )
    else:
        st.success("Este processo não possui problemas de qualidade identificados.")

selected_name = st.selectbox("Abrir processo", [row["Processo"] for row in rows])
selected = next(row for row in rows if row["Processo"] == selected_name)
if st.button("Abrir no editor", type="primary"):
    st.session_state["selected_flowchart_id"] = selected["ID"]
    st.session_state["selected_project_id"] = selected.get("Projeto ID") or ""
    st.switch_page("pages/5_Editor_de_Fluxos.py")