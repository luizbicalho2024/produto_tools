from __future__ import annotations

import json
import os

import pandas as pd
import streamlit as st

from core.auth import render_account_sidebar, require_login
from core.styles import apply_global_styles, page_header
from services.enterprise_repository import (
    initialize_enterprise_tables,
    list_notifications,
    list_records,
    mark_notification_read,
    save_record,
)
from services.enterprise_tools import (
    call_openai_compatible,
    flow_from_text,
    generate_sop,
    quality_copilot,
    send_webhook,
)
from services.flowchart_repository import get_flowchart, list_flowcharts, save_flowchart

st.set_page_config(page_title="IA e Automações", page_icon="🤖", layout="wide")
apply_global_styles(full_width=True)
user = require_login()
render_account_sidebar()
username = str(user["username"]).strip().lower()
email = str(user.get("email") or "")
is_admin = user.get("role") == "admin"
initialize_enterprise_tables()

page_header("Copiloto de IA e Automações", "Geração e análise de processos, POP/SOP, webhooks e central de notificações.")

flows = list_flowcharts(username, include_all=is_admin)
tabs = st.tabs(["Copiloto", "Texto → Processo", "POP/SOP", "Webhooks", "Notificações"])

with tabs[0]:
    if flows:
        flow_id = st.selectbox("Processo", [item["id"] for item in flows], format_func=lambda value: next(item["name"] for item in flows if item["id"] == value), key="ai_flow")
        record = get_flowchart(flow_id, actor_username=username, is_admin=is_admin)
        local_analysis = quality_copilot(record["document"])
        st.info(local_analysis)
        question = st.text_area("Pergunte ao copiloto", placeholder="Ex.: quais gargalos e riscos eu deveria tratar primeiro?")
        try:
            api_url = str(st.secrets.get("AI_API_URL") or os.getenv("AI_API_URL") or "").strip()
            api_key = str(st.secrets.get("AI_API_KEY") or os.getenv("AI_API_KEY") or "").strip()
            model = str(st.secrets.get("AI_MODEL") or os.getenv("AI_MODEL") or "").strip()
        except Exception:
            api_url, api_key, model = "", "", ""
        if st.button("Analisar com IA", type="primary", disabled=not question.strip()):
            prompt = f"Contexto do processo:\n{local_analysis}\n\nPergunta:\n{question}"
            if api_url and api_key and model:
                try:
                    st.write(call_openai_compatible(api_url, api_key, model, prompt))
                except Exception as exc:
                    st.warning(f"Provedor externo indisponível: {exc}")
                    st.write(local_analysis)
            else:
                st.write(local_analysis)
                st.caption("Para IA generativa externa configure AI_API_URL, AI_API_KEY e AI_MODEL com um endpoint compatível com Chat Completions.")
    else:
        st.info("Nenhum processo disponível.")

with tabs[1]:
    text = st.text_area("Cole etapas, atas, requisitos ou instruções", height=260, placeholder="Receber solicitação\nValidar dados\nAprovar proposta\nEnviar ao cliente")
    generated_name = st.text_input("Nome do processo", value="Processo gerado por texto")
    if st.button("Gerar fluxo", type="primary", disabled=not text.strip()):
        try:
            doc = flow_from_text(text, email or username)
            doc["flow"]["name"] = generated_name
            st.session_state["generated_flow_doc"] = doc
        except Exception as exc:
            st.error(str(exc))
    generated = st.session_state.get("generated_flow_doc")
    if generated:
        st.json({"nome": generated["flow"]["name"], "etapas": len(generated["nodes"]), "conexoes": len(generated["edges"])})
        st.download_button("Baixar JSON gerado", json.dumps(generated, ensure_ascii=False, indent=2), "processo_gerado.json", "application/json")
        if st.button("Salvar como novo processo"):
            created = save_flowchart(generated, username, email, actor_username=username, is_admin=is_admin, save_reason="ai_text_generation")
            st.success(f"Processo salvo: {created['name']}")

with tabs[2]:
    if flows:
        flow_id = st.selectbox("Processo para POP/SOP", [item["id"] for item in flows], format_func=lambda value: next(item["name"] for item in flows if item["id"] == value), key="sop_flow")
        record = get_flowchart(flow_id, actor_username=username, is_admin=is_admin)
        sop = generate_sop(record["document"])
        st.text_area("Documento gerado", value=sop, height=420)
        st.download_button("Baixar POP/SOP em Markdown", sop.encode("utf-8"), f"{record['name'].replace(' ', '_').lower()}_sop.md", "text/markdown")

with tabs[3]:
    with st.form("webhook_form"):
        name = st.text_input("Nome")
        url = st.text_input("URL HTTPS")
        events = st.multiselect("Eventos", ["process.published", "process.approved", "task.created", "task.completed", "sla.expired", "comment.mentioned", "change.approved"], default=["process.published"])
        secret = st.text_input("Segredo opcional", type="password")
        active = st.checkbox("Ativo", value=True)
        submitted = st.form_submit_button("Salvar webhook", type="primary")
    if submitted:
        save_record("webhook", {"name": name, "url": url, "events": events, "secret": secret, "active": active}, username)
        st.success("Webhook salvo.")
    hooks = list_records("webhook", username)
    if hooks:
        st.dataframe(pd.DataFrame([{"ID": h["id"], "Nome": h.get("name"), "URL": h.get("url"), "Eventos": ", ".join(h.get("events") or []), "Ativo": h.get("active")} for h in hooks]), use_container_width=True, hide_index=True)
        hook_id = st.selectbox("Webhook para teste", [h["id"] for h in hooks], format_func=lambda value: next(h.get("name") or value for h in hooks if h["id"] == value))
        event = st.selectbox("Evento de teste", ["process.published", "process.approved", "task.created", "task.completed", "sla.expired", "comment.mentioned", "change.approved"])
        if st.button("Enviar teste"):
            hook = next(h for h in hooks if h["id"] == hook_id)
            try:
                status, response = send_webhook(str(hook.get("url") or ""), event, {"source": "produto_tools", "test": True, "user": username}, str(hook.get("secret") or ""))
                st.success(f"HTTP {status}")
                st.code(response[:2000])
            except Exception as exc:
                st.error(str(exc))

with tabs[4]:
    unread_only = st.checkbox("Somente não lidas", value=False)
    notifications = list_notifications(username, unread_only=unread_only)
    if not notifications:
        st.info("Nenhuma notificação.")
    for item in notifications:
        with st.container(border=True):
            st.markdown(f"**{item.get('title')}**")
            st.write(item.get("message"))
            st.caption(f"{item.get('category')} · {item.get('created_at')}")
            if not item.get("read") and st.button("Marcar como lida", key=f"notif_{item['id']}"):
                mark_notification_read(item["id"], username)
                st.rerun()