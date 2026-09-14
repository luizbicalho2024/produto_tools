from __future__ import annotations

import pandas as pd
import streamlit as st

from core.auth import render_account_sidebar, require_login
from core.styles import apply_global_styles, page_header
from services.enterprise_repository import (
    initialize_enterprise_tables,
    list_records,
    load_evidence,
    save_evidence,
    save_record,
)
from services.flow_diff import compare_documents
from services.flowchart_repository import get_flowchart, get_version, list_flowcharts, list_versions

st.set_page_config(page_title="Governança Enterprise", page_icon="🛡️", layout="wide")
apply_global_styles(full_width=True)
user = require_login()
render_account_sidebar()
username = str(user["username"]).strip().lower()
is_admin = user.get("role") == "admin"
initialize_enterprise_tables()

page_header(
    "Governança, Compliance e Arquitetura",
    "Change Requests, controles, sistemas, capacidades, jornada, evidências e comparação visual de versões.",
)

flows = list_flowcharts(username, include_all=is_admin)
flow_id = st.selectbox("Processo de referência", [""] + [item["id"] for item in flows], format_func=lambda value: "Sem vínculo de processo" if not value else next(item["name"] for item in flows if item["id"] == value))
record = get_flowchart(flow_id, actor_username=username, is_admin=is_admin) if flow_id else None
project_id = str((record or {}).get("project_id") or "")

tabs = st.tabs(["Change Requests", "Compliance", "Sistemas", "Capacidades", "Customer Journey", "Evidências", "Diff visual"])

with tabs[0]:
    with st.form("change_request"):
        title = st.text_input("Título da mudança")
        justification = st.text_area("Justificativa", height=90)
        impact = st.text_area("Impacto esperado", height=90)
        target_version = st.text_input("Versão alvo")
        priority = st.selectbox("Prioridade", ["low", "medium", "high", "critical"])
        status = st.selectbox("Status", ["requested", "analysis", "approved", "rejected", "implemented"])
        submitted = st.form_submit_button("Registrar Change Request", type="primary")
    if submitted:
        save_record("change_request", {
            "title": title, "justification": justification, "impact": impact, "target_version": target_version,
            "priority": priority, "status": status,
        }, username, project_id=project_id, flow_id=flow_id)
        st.success("Change Request registrada.")
    rows = list_records("change_request", username, flow_id=flow_id if flow_id else None)
    if rows:
        st.dataframe(pd.DataFrame(rows)[[c for c in ["title", "priority", "status", "target_version", "updated_by", "updated_at"] if c in pd.DataFrame(rows).columns]], use_container_width=True, hide_index=True)

with tabs[1]:
    with st.form("compliance_form"):
        control = st.text_input("Controle / requisito")
        framework = st.text_input("Referencial", placeholder="LGPD, ISO 27001, contrato, política interna...")
        requirement = st.text_area("Descrição do requisito", height=90)
        node_id = st.text_input("Card ID relacionado")
        owner = st.text_input("Responsável pelo controle")
        evidence_required = st.text_area("Evidência esperada", height=70)
        submitted = st.form_submit_button("Salvar controle", type="primary")
    if submitted:
        save_record("compliance", {
            "control": control, "framework": framework, "requirement": requirement, "node_id": node_id,
            "owner": owner, "evidence_required": evidence_required,
        }, username, project_id=project_id, flow_id=flow_id)
        st.success("Controle salvo.")
    controls = list_records("compliance", username, flow_id=flow_id if flow_id else None)
    if controls:
        st.dataframe(pd.DataFrame(controls)[[c for c in ["control", "framework", "node_id", "owner", "updated_at"] if c in pd.DataFrame(controls).columns]], use_container_width=True, hide_index=True)

with tabs[2]:
    with st.form("system_form"):
        name = st.text_input("Sistema / integração")
        system_type = st.selectbox("Tipo", ["Sistema", "API", "Banco de dados", "Fornecedor", "Fila", "Arquivo"])
        technical_owner = st.text_input("Responsável técnico")
        business_owner = st.text_input("Responsável de negócio")
        environment = st.text_input("Ambiente")
        endpoint = st.text_input("URL / endpoint / referência")
        data_processed = st.text_area("Dados tratados", height=70)
        submitted = st.form_submit_button("Salvar item", type="primary")
    if submitted:
        save_record("system", {
            "name": name, "system_type": system_type, "technical_owner": technical_owner,
            "business_owner": business_owner, "environment": environment, "endpoint": endpoint,
            "data_processed": data_processed,
        }, username, project_id=project_id, flow_id=flow_id)
        st.success("Item salvo.")
    systems = list_records("system", username)
    if systems:
        st.dataframe(pd.DataFrame(systems)[[c for c in ["name", "system_type", "technical_owner", "business_owner", "environment", "flow_id"] if c in pd.DataFrame(systems).columns]], use_container_width=True, hide_index=True)

with tabs[3]:
    st.caption("Estruture capacidades L0 → L4 e conecte cada uma aos processos que as realizam.")
    with st.form("capability_form"):
        name = st.text_input("Capacidade")
        level = st.selectbox("Nível", ["L0", "L1", "L2", "L3", "L4"])
        parent = st.text_input("Capacidade pai")
        owner = st.text_input("Owner")
        description = st.text_area("Descrição", height=80)
        submitted = st.form_submit_button("Salvar capacidade", type="primary")
    if submitted:
        save_record("capability", {"name": name, "level": level, "parent": parent, "owner": owner, "description": description}, username, project_id=project_id, flow_id=flow_id)
        st.success("Capacidade salva.")
    capabilities = list_records("capability", username)
    if capabilities:
        cap_frame = pd.DataFrame(capabilities)
        st.dataframe(cap_frame[[c for c in ["level", "name", "parent", "owner", "flow_id"] if c in cap_frame.columns]], use_container_width=True, hide_index=True)
        lines = ['digraph Capabilities { rankdir="LR"; node [shape=box, style="rounded"];']
        names = {str(item.get("name") or "") for item in capabilities}
        for item in capabilities:
            name_value = str(item.get("name") or "").replace('"', '\\"')
            level_value = str(item.get("level") or "")
            lines.append(f'"{name_value}" [label="{level_value} · {name_value}"];')
            parent_value = str(item.get("parent") or "").replace('"', '\\"')
            if parent_value:
                lines.append(f'"{parent_value}" -> "{name_value}";')
        lines.append("}")
        st.graphviz_chart("\n".join(lines), use_container_width=True)

with tabs[4]:
    with st.form("journey_form"):
        journey = st.text_input("Jornada")
        stage = st.text_input("Etapa da jornada")
        persona = st.text_input("Persona")
        channel = st.text_input("Canal")
        sentiment = st.select_slider("Sentimento", options=["Muito negativo", "Negativo", "Neutro", "Positivo", "Muito positivo"], value="Neutro")
        pain = st.text_area("Dor / oportunidade", height=70)
        kpi = st.text_input("KPI")
        submitted = st.form_submit_button("Salvar etapa da jornada", type="primary")
    if submitted:
        save_record("journey", {
            "journey": journey, "stage": stage, "persona": persona, "channel": channel,
            "sentiment": sentiment, "pain": pain, "kpi": kpi,
        }, username, project_id=project_id, flow_id=flow_id)
        st.success("Etapa salva.")
    journey_rows = list_records("journey", username)
    if journey_rows:
        st.dataframe(pd.DataFrame(journey_rows)[[c for c in ["journey", "stage", "persona", "channel", "sentiment", "kpi", "flow_id"] if c in pd.DataFrame(journey_rows).columns]], use_container_width=True, hide_index=True)

with tabs[5]:
    upload = st.file_uploader("Documento / evidência", type=None)
    node_id = st.text_input("Card ID relacionado", key="evidence_node")
    description = st.text_area("Descrição", height=80, key="evidence_desc")
    if upload and st.button("Salvar evidência", type="primary"):
        try:
            save_evidence(username, upload.name, upload.type, upload.getvalue(), project_id=project_id, flow_id=flow_id, node_id=node_id, description=description)
            st.success("Evidência armazenada no MongoDB.")
        except Exception as exc:
            st.error(str(exc))
    evidence = list_records("evidence", username, flow_id=flow_id if flow_id else None)
    if evidence:
        st.dataframe(pd.DataFrame(evidence)[[c for c in ["id", "filename", "size", "node_id", "description", "updated_at"] if c in pd.DataFrame(evidence).columns]], use_container_width=True, hide_index=True)
        evid_id = st.selectbox("Baixar evidência", [item["id"] for item in evidence])
        loaded = load_evidence(evid_id)
        if loaded:
            content, metadata = loaded
            st.download_button("Baixar arquivo", content, file_name=metadata.get("filename") or "evidencia.bin", mime=metadata.get("mime_type") or "application/octet-stream")

with tabs[6]:
    if not record:
        st.info("Selecione um processo para comparar versões.")
    else:
        versions = list_versions(flow_id)
        values = [item["version"] for item in versions]
        if len(values) < 2:
            st.info("São necessárias pelo menos duas versões.")
        else:
            c1, c2 = st.columns(2)
            left = c1.selectbox("Versão base", values, index=min(1, len(values)-1))
            right = c2.selectbox("Versão comparada", values, index=0)
            if st.button("Gerar diff visual", type="primary"):
                a = get_version(flow_id, left)
                b = get_version(flow_id, right)
                diff = compare_documents(a, b)
                added = set((diff.get("nodes") or {}).get("added", []))
                removed = set((diff.get("nodes") or {}).get("removed", []))
                modified = {str(item.get("id")) for item in (diff.get("nodes") or {}).get("modified", [])}
                node_map = {str(n.get("id")): n for n in (b or {}).get("nodes", [])}
                old_map = {str(n.get("id")): n for n in (a or {}).get("nodes", [])}
                all_ids = list(dict.fromkeys([*old_map, *node_map]))
                lines = ['digraph G { rankdir="LR"; node [shape=box, style="rounded,filled"];']
                for node_id in all_ids:
                    node = node_map.get(node_id) or old_map.get(node_id) or {}
                    label = str((node.get("data") or {}).get("label") or node_id).replace('"', '\\"')
                    fill = "#dcfce7" if node_id in added else "#fee2e2" if node_id in removed else "#fef3c7" if node_id in modified else "#f8fafc"
                    lines.append(f'"{node_id}" [label="{label}", fillcolor="{fill}"];')
                for edge in (b or {}).get("edges", []):
                    if edge.get("enabled", True):
                        lines.append(f'"{edge.get("source")}" -> "{edge.get("target")}";')
                lines.append("}")
                st.graphviz_chart("\n".join(lines), use_container_width=True)
                st.caption("Verde = adicionado · Vermelho = removido · Amarelo = alterado · Branco = sem mudança.")
                st.json(diff.get("summary") or {})