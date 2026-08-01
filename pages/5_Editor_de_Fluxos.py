from __future__ import annotations

import json
from datetime import datetime
from html import escape

import streamlit as st

from components.flow_editor import flow_editor
from core.auth import render_account_sidebar, require_login
from core.styles import apply_global_styles, get_ui_theme, page_header
from schemas.flowchart_schema import demo_flowchart_document, new_flowchart_document, validate_document
from services.flowchart_repository import (
    delete_flowchart,
    duplicate_flowchart,
    get_flowchart,
    get_version,
    initialize_flowchart_tables,
    list_flowcharts,
    list_versions,
    save_flowchart,
)

st.set_page_config(page_title="Editor de Processos", page_icon="🧩", layout="wide")
apply_global_styles(full_width=True)
user = require_login()
render_account_sidebar()

try:
    initialize_flowchart_tables()
except RuntimeError as exc:
    st.error(str(exc))
    st.stop()

is_admin = user.get("role") == "admin"
owner_username = str(user["username"]).strip().lower()
owner_email = str(user.get("email") or "")


def format_datetime(value) -> str:
    if isinstance(value, datetime):
        return value.astimezone().strftime("%d/%m/%Y %H:%M")
    text = str(value or "")
    return text[:19].replace("T", " ") if text else "Sem data"


if "flow_flash" in st.session_state:
    message, kind = st.session_state.pop("flow_flash")
    getattr(st, kind)(message)

try:
    flows = list_flowcharts(owner_username, include_all=is_admin)
    if not flows:
        created = save_flowchart(
            demo_flowchart_document(owner_username), owner_username, owner_email
        )
        st.session_state["selected_flowchart_id"] = created["id"]
        flows = list_flowcharts(owner_username, include_all=is_admin)
except Exception as exc:
    st.error(f"Não foi possível acessar os fluxos no MongoDB: {exc}")
    st.stop()

available_ids = [item["id"] for item in flows]
selected_id = st.session_state.get("selected_flowchart_id")
if selected_id not in available_ids:
    selected_id = available_ids[0]
    st.session_state["selected_flowchart_id"] = selected_id

with st.sidebar:
    st.header("Processos")
    filter_text = st.text_input("Buscar", placeholder="Nome do fluxo")
    filtered = [item for item in flows if filter_text.lower() in item["name"].lower()]
    options = {
        f"{item['name']} · v{item['current_version']}": item["id"] for item in filtered
    }
    current_label = next(
        (label for label, flow_id in options.items() if flow_id == selected_id), None
    )
    selected_label = (
        st.selectbox(
            "Fluxo aberto",
            list(options),
            index=list(options).index(current_label) if current_label in options else 0,
            label_visibility="collapsed",
        )
        if options
        else None
    )
    if selected_label and options[selected_label] != selected_id:
        st.session_state["selected_flowchart_id"] = options[selected_label]
        st.rerun()

    col_new, col_copy = st.columns(2)
    with col_new:
        if st.button("Novo", use_container_width=True, type="primary"):
            created = save_flowchart(
                new_flowchart_document("Novo processo", owner_username),
                owner_username,
                owner_email,
            )
            st.session_state["selected_flowchart_id"] = created["id"]
            st.session_state["flow_flash"] = ("Novo processo criado.", "success")
            st.rerun()
    with col_copy:
        if st.button("Duplicar", use_container_width=True):
            duplicated = duplicate_flowchart(
                selected_id, owner_username, owner_email, is_admin=is_admin
            )
            st.session_state["selected_flowchart_id"] = duplicated["id"]
            st.session_state["flow_flash"] = ("Processo duplicado.", "success")
            st.rerun()

record = get_flowchart(selected_id)
if not record:
    st.error("O processo selecionado não foi encontrado.")
    st.stop()

if not is_admin and record["owner_username"] != owner_username:
    st.error("Você não possui acesso a este processo.")
    st.stop()

page_header(
    "Editor de Processos",
    "Modele processos extensos com raias, decisões ramificadas, organização automática, destaque de rotas e armazenamento no MongoDB.",
)

meta1, meta2, meta3, meta4 = st.columns([2.2, 1, 1, 1])
meta1.markdown(
    f"<strong>{escape(record['name'])}</strong><br><small>{escape(record['description'] or 'Sem descrição')}</small>",
    unsafe_allow_html=True,
)
meta2.metric("Versão", record["current_version"])
meta3.metric("Elementos", len(record["document"].get("nodes", [])))
meta4.metric("Conexões", len(record["document"].get("edges", [])))
st.caption(
    f"Proprietário: @{record['owner_username']} • Última atualização: {format_datetime(record['updated_at'])}"
)

with st.expander("Versões, restauração e exclusão", expanded=False):
    col_history, col_delete = st.columns([2, 1])
    with col_history:
        versions = list_versions(selected_id)
        if versions:
            version_options = {
                f"Versão {item['version']} · {format_datetime(item.get('created_at'))} · @{item.get('created_by', '')}": item["version"]
                for item in versions
            }
            version_label = st.selectbox("Histórico", list(version_options), index=0)
            if st.button("Restaurar como nova versão"):
                restored = get_version(selected_id, version_options[version_label])
                if restored:
                    restored["flow"]["id"] = selected_id
                    save_flowchart(
                        restored,
                        record["owner_username"],
                        record.get("owner_email", ""),
                    )
                    st.session_state["flow_flash"] = (
                        "Versão restaurada com sucesso.",
                        "success",
                    )
                    st.rerun()
        else:
            st.info("Não há versões anteriores.")
    with col_delete:
        st.warning("A exclusão remove também o histórico salvo no MongoDB.")
        confirm_delete = st.checkbox("Confirmar exclusão", key="confirm_flow_delete")
        if st.button(
            "Excluir processo", disabled=not confirm_delete, use_container_width=True
        ):
            if delete_flowchart(
                selected_id, owner_username, is_admin=is_admin
            ):
                st.session_state.pop("selected_flowchart_id", None)
                st.session_state["flow_flash"] = (
                    "Processo excluído.",
                    "success",
                )
                st.rerun()
            else:
                st.error("Não foi possível excluir o processo.")

component_key = f"flow_editor_{selected_id}"
result = flow_editor(
    record["document"],
    key=component_key,
    height=920,
    theme=get_ui_theme(),
    on_save_change=lambda: None,
)

save_payload = getattr(result, "save", None)
if save_payload:
    try:
        # Um JSON importado atualiza o fluxo atualmente aberto; o ID persistido
        # não pode ser trocado pelo conteúdo enviado pelo navegador.
        save_payload.setdefault("flow", {})["id"] = selected_id
        errors = validate_document(save_payload)
        if errors:
            st.session_state["flow_flash"] = (
                "Não foi possível salvar: " + " | ".join(errors[:4]),
                "error",
            )
        else:
            saved = save_flowchart(
                save_payload,
                record["owner_username"],
                record.get("owner_email", ""),
            )
            st.session_state["selected_flowchart_id"] = saved["id"]
            st.session_state["flow_flash"] = (
                f"Processo salvo no MongoDB na versão {saved['version']}.",
                "success",
            )
        st.rerun()
    except Exception as exc:
        st.error(f"Falha ao salvar o processo: {exc}")

with st.expander("Estrutura JSON atual", expanded=False):
    st.json(record["document"], expanded=False)
    st.download_button(
        "Baixar JSON salvo no MongoDB",
        data=json.dumps(record["document"], ensure_ascii=False, indent=2),
        file_name=f"{record['name'].replace(' ', '_').lower()}.json",
        mime="application/json",
    )
