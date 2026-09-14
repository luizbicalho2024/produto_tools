from __future__ import annotations

import json
from datetime import date

import pandas as pd
import streamlit as st

from core.auth import render_account_sidebar, require_login
from core.styles import apply_global_styles, page_header
from services.enterprise_repository import initialize_enterprise_tables, list_records, save_record
from services.enterprise_tools import five_whys, pareto_rows, sipoc_from_document, vsm_metrics
from services.flowchart_repository import get_flowchart, list_flowcharts

st.set_page_config(page_title="Melhoria Contínua", page_icon="🧠", layout="wide")
apply_global_styles(full_width=True)
user = require_login()
render_account_sidebar()
username = str(user["username"]).strip().lower()
is_admin = user.get("role") == "admin"
initialize_enterprise_tables()

page_header(
    "Central de Melhoria Contínua",
    "SIPOC, 5W2H, riscos/FMEA, causa raiz, Pareto e Value Stream Mapping conectados aos fluxos existentes.",
)

flows = list_flowcharts(username, include_all=is_admin)
if not flows:
    st.info("Nenhum fluxo disponível.")
    st.stop()

flow_id = st.selectbox("Processo", [item["id"] for item in flows], format_func=lambda value: next(item["name"] for item in flows if item["id"] == value))
record = get_flowchart(flow_id, actor_username=username, is_admin=is_admin)
if not record:
    st.stop()
document = record["document"]
project_id = str(record.get("project_id") or "")

tabs = st.tabs(["SIPOC", "5W2H", "Riscos e FMEA", "Ishikawa / 5 Porquês", "Pareto", "VSM"])

with tabs[0]:
    suggested = sipoc_from_document(document)
    st.caption("O sistema preenche uma primeira versão com base nas raias, etapas e documentos do fluxo.")
    values = {}
    columns = st.columns(5)
    for index, key in enumerate(["Suppliers", "Inputs", "Process", "Outputs", "Customers"]):
        values[key] = columns[index].text_area(key, value="\n".join(suggested[key]), height=230, key=f"sipoc_{key}")
    if st.button("Salvar SIPOC", type="primary"):
        payload = {key: [line.strip() for line in text.splitlines() if line.strip()] for key, text in values.items()}
        save_record("sipoc", payload, username, project_id=project_id, flow_id=flow_id)
        st.success("SIPOC salvo.")
    history = list_records("sipoc", username, flow_id=flow_id, limit=20)
    if history:
        st.dataframe(pd.DataFrame([{"Atualizado": item.get("updated_at"), "Responsável": item.get("updated_by"), "ID": item.get("id")} for item in history]), hide_index=True, use_container_width=True)

with tabs[1]:
    st.write("Transforme um problema ou recomendação em plano de ação.")
    with st.form("five_w_two_h"):
        what = st.text_input("What — O que será feito?")
        why = st.text_area("Why — Por quê?", height=90)
        where = st.text_input("Where — Onde?")
        when = st.date_input("When — Quando?", value=date.today())
        who = st.text_input("Who — Quem?")
        how = st.text_area("How — Como?", height=90)
        how_much = st.number_input("How much — Custo estimado", min_value=0.0, step=100.0)
        status = st.selectbox("Status", ["planned", "in_progress", "blocked", "done"])
        submitted = st.form_submit_button("Salvar 5W2H", type="primary")
    if submitted:
        save_record("action_plan", {
            "what": what, "why": why, "where": where, "when": str(when), "who": who,
            "how": how, "how_much": float(how_much), "status": status,
        }, username, project_id=project_id, flow_id=flow_id)
        st.success("Plano salvo.")
    rows = list_records("action_plan", username, flow_id=flow_id)
    if rows:
        st.dataframe(pd.DataFrame(rows)[["what", "why", "who", "when", "how_much", "status", "updated_at"]], use_container_width=True, hide_index=True)

with tabs[2]:
    st.caption("FMEA: RPN = Severidade × Ocorrência × Detecção. Para riscos simples, use probabilidade e impacto.")
    with st.form("risk_form"):
        title = st.text_input("Risco / modo de falha")
        cause = st.text_area("Causa", height=80)
        effect = st.text_area("Efeito", height=80)
        c1, c2, c3, c4, c5 = st.columns(5)
        severity = c1.number_input("Severidade", 1, 10, 5)
        occurrence = c2.number_input("Ocorrência", 1, 10, 5)
        detection = c3.number_input("Detecção", 1, 10, 5)
        probability = c4.number_input("Probabilidade", 1, 5, 3)
        impact = c5.number_input("Impacto", 1, 5, 3)
        owner = st.text_input("Responsável")
        mitigation = st.text_area("Mitigação / controle", height=80)
        node_id = st.text_input("Card ID relacionado (opcional)")
        submitted = st.form_submit_button("Salvar risco/FMEA", type="primary")
    if submitted:
        save_record("risk", {
            "title": title, "cause": cause, "effect": effect, "severity": severity,
            "occurrence": occurrence, "detection": detection, "rpn": severity * occurrence * detection,
            "probability": probability, "impact": impact, "risk_score": probability * impact,
            "owner": owner, "mitigation": mitigation, "node_id": node_id,
        }, username, project_id=project_id, flow_id=flow_id)
        st.success("Risco salvo.")
    risks = list_records("risk", username, flow_id=flow_id)
    if risks:
        frame = pd.DataFrame(risks)
        cols = [c for c in ["title", "owner", "rpn", "risk_score", "mitigation", "updated_at"] if c in frame.columns]
        st.dataframe(frame[cols], use_container_width=True, hide_index=True)

with tabs[3]:
    problem = st.text_input("Problema para análise", key="root_problem")
    categories = {}
    default_categories = ["Método", "Máquina/Sistema", "Mão de obra", "Material/Dado", "Medição", "Meio ambiente"]
    cols = st.columns(3)
    for index, category in enumerate(default_categories):
        categories[category] = cols[index % 3].text_area(category, height=90, key=f"fish_{index}")
    st.markdown("#### 5 Porquês")
    answers = [st.text_input(f"Resposta {index + 1}", key=f"why_{index}") for index in range(5)]
    why_rows = five_whys(problem, answers)
    st.dataframe(pd.DataFrame(why_rows), use_container_width=True, hide_index=True)
    if st.button("Salvar análise de causa raiz", type="primary"):
        save_record("root_cause", {
            "problem": problem,
            "ishikawa": {key: [line.strip() for line in value.splitlines() if line.strip()] for key, value in categories.items()},
            "five_whys": why_rows,
            "root_cause": next((row["Resposta"] for row in reversed(why_rows) if row["Resposta"]), ""),
        }, username, project_id=project_id, flow_id=flow_id)
        st.success("Análise salva.")

with tabs[4]:
    st.caption("Cole uma causa por linha ou repita causas para representar múltiplas ocorrências.")
    occurrences = st.text_area("Ocorrências", height=220, placeholder="Atraso fornecedor\nCadastro incorreto\nAtraso fornecedor\n...")
    rows = pareto_rows([line.strip() for line in occurrences.splitlines()])
    if rows:
        frame = pd.DataFrame(rows)
        st.dataframe(frame, use_container_width=True, hide_index=True)
        st.bar_chart(frame.set_index("Causa")["Ocorrências"])
    else:
        st.info("Informe ocorrências para montar o Pareto.")

with tabs[5]:
    st.caption("Modele tempo de valor agregado e espera para identificar desperdícios.")
    seed = pd.DataFrame([
        {"etapa": str((node.get("data") or {}).get("label") or node.get("id")), "va_minutes": float((node.get("data") or {}).get("slaMinutes") or 0), "wait_minutes": 0.0, "classification": "VA"}
        for node in document.get("nodes", []) if node.get("type") not in {"start", "end", "note"}
    ])
    edited = st.data_editor(seed, num_rows="dynamic", use_container_width=True, key="vsm_editor")
    metrics = vsm_metrics(edited.to_dict("records"))
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Valor agregado", f"{metrics['value_added_minutes']:.1f} min")
    c2.metric("Espera", f"{metrics['wait_minutes']:.1f} min")
    c3.metric("Lead time", f"{metrics['lead_time_minutes']:.1f} min")
    c4.metric("Eficiência de fluxo", f"{metrics['flow_efficiency_percent']:.1f}%")
    if st.button("Salvar VSM", type="primary"):
        save_record("vsm", {"steps": edited.to_dict("records"), "metrics": metrics}, username, project_id=project_id, flow_id=flow_id)
        st.success("VSM salvo.")