from __future__ import annotations

import io

import pandas as pd
import streamlit as st

from core.auth import render_account_sidebar, require_login
from core.styles import apply_global_styles, page_header
from services.enterprise_tools import conformance_check, process_mining
from services.flowchart_repository import get_flowchart, list_flowcharts

st.set_page_config(page_title="Process Mining", page_icon="⛏️", layout="wide")
apply_global_styles(full_width=True)
user = require_login()
render_account_sidebar()
username = str(user["username"]).strip().lower()
is_admin = user.get("role") == "admin"

page_header("Process Mining e Conformance", "Reconstrua o processo real a partir de eventos e compare-o com o fluxo documentado.")

uploaded = st.file_uploader("Log de eventos", type=["csv", "xlsx", "xls"])
if not uploaded:
    st.info("Envie CSV/XLSX contendo pelo menos caso, atividade e timestamp.")
    st.stop()

try:
    if uploaded.name.lower().endswith(".csv"):
        frame = pd.read_csv(uploaded)
    else:
        frame = pd.read_excel(uploaded)
except Exception as exc:
    st.error(f"Não foi possível ler o arquivo: {exc}")
    st.stop()

st.dataframe(frame.head(50), use_container_width=True, hide_index=True)
columns = list(frame.columns)
c1, c2, c3 = st.columns(3)
case_col = c1.selectbox("Case ID", columns)
activity_col = c2.selectbox("Atividade", columns, index=min(1, len(columns)-1))
timestamp_col = c3.selectbox("Timestamp", columns, index=min(2, len(columns)-1))

if st.button("Minerar processo", type="primary"):
    try:
        st.session_state["mining_result"] = process_mining(frame, case_col, activity_col, timestamp_col)
    except Exception as exc:
        st.error(str(exc))

result = st.session_state.get("mining_result")
if not result:
    st.stop()

m1, m2, m3, m4 = st.columns(4)
m1.metric("Casos", result["case_count"])
m2.metric("Eventos", result["event_count"])
m3.metric("Atividades", result["activity_count"])
m4.metric("Duração média", f"{result['mean_case_minutes']:.1f} min")

st.markdown("### Fluxo descoberto")
if result["dfg"]:
    lines = ['digraph G { rankdir="LR"; node [shape=box, style="rounded"];']
    for row in result["dfg"][:100]:
        source = str(row["source"]).replace('"', '\\"')
        target = str(row["target"]).replace('"', '\\"')
        lines.append(f'"{source}" -> "{target}" [label="{row["count"]}"];')
    lines.append("}")
    st.graphviz_chart("\n".join(lines), use_container_width=True)
    st.dataframe(pd.DataFrame(result["dfg"]), use_container_width=True, hide_index=True)

st.markdown("### Variantes")
st.dataframe(pd.DataFrame(result["variants"]), use_container_width=True, hide_index=True)

flows = list_flowcharts(username, include_all=is_admin)
if flows:
    flow_id = st.selectbox("Comparar com processo documentado", [item["id"] for item in flows], format_func=lambda value: next(item["name"] for item in flows if item["id"] == value))
    if st.button("Executar conformance checking"):
        record = get_flowchart(flow_id, actor_username=username, is_admin=is_admin)
        check = conformance_check(result, record["document"])
        st.metric("Fitness", f"{check['fitness_percent']:.1f}%")
        c1, c2 = st.columns(2)
        with c1:
            st.write("**Desvios observados**")
            st.dataframe(pd.DataFrame(check["unexpected"]), use_container_width=True, hide_index=True)
        with c2:
            st.write("**Caminhos documentados não observados**")
            st.dataframe(pd.DataFrame(check["not_observed"]), use_container_width=True, hide_index=True)