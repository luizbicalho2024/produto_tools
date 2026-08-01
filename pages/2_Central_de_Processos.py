from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

from core.auth import render_account_sidebar, require_login
from core.configuration import WORKFLOW_STATUS_LABELS
from core.styles import apply_global_styles, page_header
from services.flow_analytics import analyze_document
from services.flowchart_repository import get_flowchart, list_comments, list_flowcharts
from services.project_repository import list_projects

st.set_page_config(page_title="Central de Processos", page_icon="◫", layout="wide")
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
    "Portfólio, qualidade, governança e pendências dos fluxos acessíveis ao seu usuário.",
)
projects = list_projects(username, include_all=is_admin, is_admin=is_admin)
project_by_id = {item["id"]: item for item in projects}
flows = list_flowcharts(username, include_all=is_admin)
search = st.text_input("Pesquisar no portfólio", placeholder="Nome, responsável ou status")
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
for item in flows:
    if project_filter and item.get("project_id") != project_filter:
        continue
    if search and search.lower() not in " ".join([item["name"], item.get("owner_username", ""), item.get("workflow_status", ""), project_by_id.get(item.get("project_id"), {}).get("name", "")]).lower():
        continue
    if status_filter and item.get("workflow_status") not in status_filter:
        continue
    record = get_flowchart(item["id"], actor_username=username, is_admin=is_admin)
    if not record:
        continue
    analysis = analyze_document(record["document"])
    comments = list_comments(item["id"], include_resolved=False)
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
        "Comentários abertos": len(comments),
        "Atualizado em": fmt(item.get("updated_at")),
    })

published = sum(1 for row in rows if row["Status"] == WORKFLOW_STATUS_LABELS["published"])
in_review = sum(1 for row in rows if row["Status"] == WORKFLOW_STATUS_LABELS["in_review"])
average_quality = round(sum(row["Qualidade"] for row in rows) / len(rows)) if rows else 0
open_comments = sum(row["Comentários abertos"] for row in rows)
col1, col2, col3, col4 = st.columns(4)
col1.metric("Processos", len(rows))
col2.metric("Publicados", published)
col3.metric("Em revisão", in_review)
col4.metric("Qualidade média", f"{average_quality}/100", help=f"{open_comments} comentários abertos no portfólio")

if rows:
    frame = pd.DataFrame(rows)
    st.dataframe(
        frame,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Qualidade": st.column_config.ProgressColumn("Qualidade", min_value=0, max_value=100, format="%d/100"),
            "ID": None,
            "Projeto ID": None,
        },
    )
    selected_name = st.selectbox("Abrir processo", [row["Processo"] for row in rows])
    selected = next(row for row in rows if row["Processo"] == selected_name)
    if st.button("Abrir no editor", type="primary"):
        st.session_state["selected_flowchart_id"] = selected["ID"]
        st.session_state["selected_project_id"] = selected.get("Projeto ID") or ""
        st.switch_page("pages/5_Editor_de_Fluxos.py")
else:
    st.info("Nenhum processo atende aos filtros selecionados.")
