from __future__ import annotations

import pandas as pd
import streamlit as st

from core.auth import render_account_sidebar, require_login
from core.styles import apply_global_styles, page_header
from services.enterprise_repository import initialize_enterprise_tables, list_records, save_record
from services.enterprise_tools import simulate_process
from services.flowchart_repository import get_flowchart, list_flowcharts
from services.process_execution import cockpit_metrics, initialize_execution_tables, list_instances

st.set_page_config(page_title="Simulação e Custos", page_icon="📊", layout="wide")
apply_global_styles(full_width=True)
user = require_login()
render_account_sidebar()
username = str(user["username"]).strip().lower()
is_admin = user.get("role") == "admin"
initialize_enterprise_tables()
initialize_execution_tables()

page_header("Simulação, SLA e Custos", "Compare cenários antes de alterar o processo e acompanhe indicadores das instâncias executadas.")

flows = list_flowcharts(username, include_all=is_admin)
if not flows:
    st.info("Nenhum fluxo disponível.")
    st.stop()
flow_id = st.selectbox("Processo", [item["id"] for item in flows], format_func=lambda value: next(item["name"] for item in flows if item["id"] == value))
record = get_flowchart(flow_id, actor_username=username, is_admin=is_admin)
document = record["document"]

tabs = st.tabs(["What-if", "Comparar cenários", "Cockpit SLA"])

with tabs[0]:
    c1, c2, c3, c4 = st.columns(4)
    iterations = c1.number_input("Simulações", 100, 20000, 2000, step=100)
    default_minutes = c2.number_input("Tempo padrão por etapa (min)", 0.1, 1440.0, 15.0)
    hourly_cost = c3.number_input("Custo médio por hora (R$)", 0.0, 10000.0, 50.0)
    variability = c4.slider("Variabilidade (%)", 0, 100, 20)
    scenario_name = st.text_input("Nome do cenário", value="Cenário base")
    if st.button("Executar simulação", type="primary"):
        result = simulate_process(document, iterations=int(iterations), default_task_minutes=float(default_minutes), hourly_cost=float(hourly_cost), variability_percent=float(variability))
        st.session_state["simulation_result"] = result
    result = st.session_state.get("simulation_result")
    if result:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Tempo médio", f"{result['mean_minutes']:.1f} min")
        m2.metric("P50", f"{result['p50_minutes']:.1f} min")
        m3.metric("P95", f"{result['p95_minutes']:.1f} min")
        m4.metric("Custo médio", f"R$ {result['mean_cost']:.2f}")
        if result["paths"]:
            st.dataframe(pd.DataFrame(result["paths"]), use_container_width=True, hide_index=True)
        if st.button("Salvar cenário"):
            save_record("scenario", {
                "name": scenario_name,
                "parameters": {"iterations": iterations, "default_minutes": default_minutes, "hourly_cost": hourly_cost, "variability": variability},
                "result": result,
            }, username, project_id=str(record.get("project_id") or ""), flow_id=flow_id)
            st.success("Cenário salvo.")

with tabs[1]:
    scenarios = list_records("scenario", username, flow_id=flow_id)
    if scenarios:
        frame = pd.DataFrame([{
            "Cenário": item.get("name"),
            "Tempo médio": (item.get("result") or {}).get("mean_minutes"),
            "P95": (item.get("result") or {}).get("p95_minutes"),
            "Custo médio": (item.get("result") or {}).get("mean_cost"),
            "Atualizado": item.get("updated_at"),
        } for item in scenarios])
        st.dataframe(frame, use_container_width=True, hide_index=True)
        st.bar_chart(frame.set_index("Cenário")[["Tempo médio", "P95"]])
    else:
        st.info("Ainda não há cenários salvos.")

with tabs[2]:
    metrics = cockpit_metrics()
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Tarefas abertas", metrics["open_tasks"])
    m2.metric("Tarefas atrasadas", metrics["overdue_tasks"])
    m3.metric("Instâncias em execução", metrics["running_instances"])
    m4.metric("Instâncias concluídas", metrics["completed_instances"])
    instances = list_instances(flow_id)
    if instances:
        frame = pd.DataFrame([{
            "ID": item.get("id"), "Status": item.get("status"), "Iniciado por": item.get("started_by"),
            "Início": item.get("started_at"), "Conclusão": item.get("completed_at"), "Etapa atual": item.get("current_node_id"),
        } for item in instances])
        st.dataframe(frame, use_container_width=True, hide_index=True)
    else:
        st.info("Esse processo ainda não possui instâncias executadas.")