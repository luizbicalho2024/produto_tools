from __future__ import annotations

import json

import pandas as pd
import streamlit as st

from core.auth import render_account_sidebar, require_login
from core.styles import apply_global_styles, page_header
from services.enterprise_repository import initialize_enterprise_tables, list_records, save_record
from services.enterprise_tools import bpmn_export, bpmn_import, dmn_evaluate
from services.flowchart_repository import get_flowchart, list_flowcharts, save_flowchart

st.set_page_config(page_title="BPMN e DMN", page_icon="🔷", layout="wide")
apply_global_styles(full_width=True)
user = require_login()
render_account_sidebar()
username = str(user["username"]).strip().lower()
email = str(user.get("email") or "")
is_admin = user.get("role") == "admin"
initialize_enterprise_tables()

page_header("BPMN 2.0 e Tabelas de Decisão", "Interoperabilidade BPMN e decisões reutilizáveis com DMN simplificado.")

tabs = st.tabs(["BPMN 2.0", "DMN"])

with tabs[0]:
    flows = list_flowcharts(username, include_all=is_admin)
    if flows:
        flow_id = st.selectbox("Exportar processo", [item["id"] for item in flows], format_func=lambda value: next(item["name"] for item in flows if item["id"] == value))
        record = get_flowchart(flow_id, actor_username=username, is_admin=is_admin)
        bpmn = bpmn_export(record["document"])
        st.download_button("Baixar BPMN 2.0 (.bpmn)", bpmn, file_name=f"{record['name'].replace(' ', '_').lower()}.bpmn", mime="application/xml", type="primary")
    st.divider()
    upload = st.file_uploader("Importar BPMN", type=["bpmn", "xml"])
    if upload:
        try:
            imported = bpmn_import(upload.getvalue(), email or username)
            st.success(f"Processo reconhecido: {imported['flow']['name']} · {len(imported['nodes'])} elementos")
            st.download_button("Baixar JSON convertido", json.dumps(imported, ensure_ascii=False, indent=2), "bpmn_convertido.json", "application/json")
            if st.button("Salvar BPMN importado como novo fluxo", type="primary"):
                created = save_flowchart(imported, username, email, actor_username=username, is_admin=is_admin, save_reason="bpmn_import")
                st.success(f"Fluxo criado: {created['name']}")
        except Exception as exc:
            st.error(f"Falha na importação BPMN: {exc}")

with tabs[1]:
    st.caption("Cada regra possui condições de entrada e saídas. Use * como curinga.")
    table = st.data_editor(pd.DataFrame([
        {"regra": "R1", "campo": "valor", "operador": "igual", "esperado": "*", "saida": "Aprovar"}
    ]), num_rows="dynamic", use_container_width=True, key="dmn_editor")
    decision_name = st.text_input("Nome da decisão", value="Nova decisão")
    if st.button("Salvar tabela DMN", type="primary"):
        rules = []
        for _, row in table.fillna("").iterrows():
            rules.append({"when": {str(row["campo"]): str(row["esperado"])}, "then": {"resultado": str(row["saida"])}, "operator": str(row["operador"])})
        save_record("dmn", {"name": decision_name, "rules": rules}, username)
        st.success("Decisão salva.")
    decisions = list_records("dmn", username)
    if decisions:
        selected = st.selectbox("Testar decisão salva", [item["id"] for item in decisions], format_func=lambda value: next(item.get("name", value) for item in decisions if item["id"] == value))
        decision = next(item for item in decisions if item["id"] == selected)
        keys = sorted({key for rule in decision.get("rules", []) for key in (rule.get("when") or {})})
        inputs = {key: st.text_input(f"Entrada: {key}", key=f"dmn_input_{key}") for key in keys}
        if st.button("Avaliar decisão"):
            result = dmn_evaluate(decision.get("rules", []), inputs)
            st.json(result if result is not None else {"resultado": "Nenhuma regra correspondente"})