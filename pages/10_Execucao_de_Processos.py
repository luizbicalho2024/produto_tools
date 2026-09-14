from __future__ import annotations

import json

import pandas as pd
import streamlit as st

from core.auth import render_account_sidebar, require_login
from core.styles import apply_global_styles, page_header
from services.flowchart_repository import get_flowchart, list_flowcharts
from services.process_execution import (
    cockpit_metrics,
    complete_task,
    initialize_execution_tables,
    list_forms,
    list_instances,
    list_tasks,
    save_form,
    start_instance,
)

st.set_page_config(page_title="Execução de Processos", page_icon="▶️", layout="wide")
apply_global_styles(full_width=True)
user = require_login()
render_account_sidebar()
username = str(user["username"]).strip().lower()
is_admin = user.get("role") == "admin"
initialize_execution_tables()

page_header("Execução de Processos e Tarefas", "Formulários, instâncias reais, tarefas, decisões operacionais e acompanhamento de SLA.")

flows = list_flowcharts(username, include_all=is_admin)
if not flows:
    st.info("Nenhum fluxo disponível.")
    st.stop()

tabs = st.tabs(["Form Builder", "Iniciar processo", "Minhas tarefas", "Cockpit"])

with tabs[0]:
    flow_id = st.selectbox("Processo do formulário", [item["id"] for item in flows], format_func=lambda value: next(item["name"] for item in flows if item["id"] == value), key="form_flow")
    fields = st.data_editor(
        pd.DataFrame([
            {"name": "solicitante", "label": "Solicitante", "type": "text", "required": True, "options": ""},
            {"name": "observacao", "label": "Observação", "type": "textarea", "required": False, "options": ""},
        ]),
        num_rows="dynamic",
        use_container_width=True,
        key="form_fields",
    )
    form_name = st.text_input("Nome do formulário", value="Formulário de abertura")
    if st.button("Salvar formulário", type="primary"):
        save_form(flow_id, form_name, fields.fillna("").to_dict("records"), username)
        st.success("Formulário salvo.")
    forms = list_forms(flow_id)
    if forms:
        st.dataframe(pd.DataFrame([{"Nome": f["name"], "Campos": len(f.get("fields", [])), "Atualizado": f.get("updated_at")} for f in forms]), use_container_width=True, hide_index=True)

with tabs[1]:
    flow_id = st.selectbox("Processo", [item["id"] for item in flows], format_func=lambda value: next(item["name"] for item in flows if item["id"] == value), key="start_flow")
    forms = list_forms(flow_id)
    form_data = {}
    selected_form = None
    if forms:
        selected_id = st.selectbox("Formulário", [""] + [f["id"] for f in forms], format_func=lambda value: "Sem formulário" if not value else next(f["name"] for f in forms if f["id"] == value))
        selected_form = next((f for f in forms if f["id"] == selected_id), None)
    if selected_form:
        st.markdown("#### Dados de abertura")
        for field in selected_form.get("fields", []):
            name = str(field.get("name") or "").strip()
            label = str(field.get("label") or name)
            field_type = str(field.get("type") or "text")
            options = [item.strip() for item in str(field.get("options") or "").split(",") if item.strip()]
            if not name:
                continue
            if field_type == "textarea":
                form_data[name] = st.text_area(label, key=f"instance_{name}")
            elif field_type == "number":
                form_data[name] = st.number_input(label, key=f"instance_{name}")
            elif field_type == "date":
                form_data[name] = str(st.date_input(label, key=f"instance_{name}"))
            elif field_type in {"select", "radio"} and options:
                form_data[name] = st.selectbox(label, options, key=f"instance_{name}")
            elif field_type == "checkbox":
                form_data[name] = st.checkbox(label, key=f"instance_{name}")
            else:
                form_data[name] = st.text_input(label, key=f"instance_{name}")
    if st.button("Iniciar instância", type="primary"):
        try:
            instance = start_instance(flow_id, username, form_data=form_data, is_admin=is_admin)
            st.success(f"Instância {instance['id']} iniciada.")
        except Exception as exc:
            st.error(str(exc))

with tabs[2]:
    include_all = is_admin and st.checkbox("Exibir tarefas de todos", value=False)
    tasks = list_tasks(username, include_all=include_all)
    if not tasks:
        st.info("Nenhuma tarefa aberta.")
    else:
        frame = pd.DataFrame(tasks)
        st.dataframe(frame[[c for c in ["id", "label", "assignee", "due_at", "instance_id", "flow_id"] if c in frame.columns]], use_container_width=True, hide_index=True)
        task_id = st.selectbox("Tarefa", [item["id"] for item in tasks], format_func=lambda value: next(f"{item['label']} · {item['assignee']}" for item in tasks if item["id"] == value))
        pending_choice = st.session_state.get("task_next_choice")
        if pending_choice and pending_choice.get("task_id") == task_id:
            selected_next = st.selectbox("Próxima etapa", [item["id"] for item in pending_choice["candidates"]], format_func=lambda value: next(item["label"] for item in pending_choice["candidates"] if item["id"] == value))
            if st.button("Confirmar conclusão", type="primary"):
                result = complete_task(task_id, username, next_node_id=selected_next, is_admin=is_admin)
                st.session_state.pop("task_next_choice", None)
                st.success("Tarefa concluída.")
                st.rerun()
        elif st.button("Concluir tarefa", type="primary"):
            try:
                result = complete_task(task_id, username, is_admin=is_admin)
                if result.get("needs_choice"):
                    st.session_state["task_next_choice"] = {"task_id": task_id, "candidates": result["candidates"]}
                    st.rerun()
                st.success("Tarefa concluída.")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))

with tabs[3]:
    metrics = cockpit_metrics()
    cols = st.columns(4)
    cols[0].metric("Tarefas abertas", metrics["open_tasks"])
    cols[1].metric("Atrasadas", metrics["overdue_tasks"])
    cols[2].metric("Instâncias em execução", metrics["running_instances"])
    cols[3].metric("Concluídas", metrics["completed_instances"])
    instances = list_instances()
    if instances:
        st.dataframe(pd.DataFrame([{
            "Instância": item.get("id"), "Processo": item.get("flow_name"), "Status": item.get("status"),
            "Iniciado por": item.get("started_by"), "Início": item.get("started_at"),
            "Conclusão": item.get("completed_at"), "Etapa atual": item.get("current_node_id"),
        } for item in instances]), use_container_width=True, hide_index=True)