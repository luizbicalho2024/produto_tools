from __future__ import annotations

import pandas as pd
import streamlit as st

from core.auth import render_account_sidebar, require_login
from core.styles import apply_global_styles, page_header
from services.flow_analytics import analyze_document, build_raci_rows
from services.flowchart_repository import get_flowchart, list_flowcharts
from services.report_export import full_documentation_pdf

st.set_page_config(page_title="Portal de Processos", page_icon="📚", layout="wide")
apply_global_styles(full_width=True)
user = require_login()
render_account_sidebar()
username = str(user["username"]).strip().lower()
is_admin = user.get("role") == "admin"

page_header("Portal de Processos Publicados", "Consulta simples da versão vigente, responsáveis, SLA, RACI e documentação oficial.")

flows = [item for item in list_flowcharts(username, include_all=is_admin) if item.get("workflow_status") == "published"]
search = st.text_input("Pesquisar", placeholder="Processo, responsável, projeto...")
if search:
    flows = [item for item in flows if search.lower() in " ".join([str(item.get("name")), str(item.get("owner_username")), str(item.get("project_id"))]).lower()]
if not flows:
    st.info("Nenhum processo publicado encontrado.")
    st.stop()

flow_id = st.selectbox("Processo", [item["id"] for item in flows], format_func=lambda value: next(item["name"] for item in flows if item["id"] == value))
record = get_flowchart(flow_id, actor_username=username, is_admin=is_admin)
doc = record["document"]
analysis = analyze_document(doc)

m1, m2, m3, m4 = st.columns(4)
m1.metric("Versão", record.get("current_version"))
m2.metric("Qualidade", f"{analysis['quality_score']}/100")
m3.metric("Etapas", analysis["counts"]["nodes"])
m4.metric("SLA cadastrado", f"{analysis['total_sla_minutes']:.0f} min")

st.markdown(f"### {record['name']}")
st.write(doc.get("flow", {}).get("description") or "Sem descrição geral.")
rows = []
lane_map = {str(l.get("id")): str(l.get("name") or "") for l in doc.get("lanes", [])}
for node in doc.get("nodes", []):
    data = node.get("data") or {}
    if node.get("type") == "note":
        continue
    rows.append({
        "Etapa": data.get("label"), "Tipo": node.get("type"), "Área": lane_map.get(str(node.get("laneId")), ""),
        "Responsável": data.get("owner"), "SLA (min)": data.get("slaMinutes"), "Criticidade": data.get("criticality"),
        "Descrição": data.get("description"),
    })
st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

with st.expander("Matriz RACI"):
    st.dataframe(pd.DataFrame(build_raci_rows(doc)), use_container_width=True, hide_index=True)

metadata = {"name": record["name"], "version": record.get("current_version"), "revision": record.get("revision")}
st.download_button("Baixar documentação oficial em PDF", full_documentation_pdf(doc, metadata), file_name=f"{record['name'].replace(' ', '_').lower()}_oficial.pdf", mime="application/pdf", type="primary")