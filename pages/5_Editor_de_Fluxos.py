from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime
from html import escape
from uuid import uuid4

import pandas as pd
import streamlit as st

import database as db
from components.flow_editor import flow_editor
from core.auth import render_account_sidebar, require_login
from core.configuration import (
    FLOW_ACCESS_LABELS,
    WORKFLOW_STATUS_LABELS,
)
from core.styles import apply_global_styles, get_ui_theme, page_header
from schemas.flowchart_schema import demo_flowchart_document, new_flowchart_document, normalize_document
from services.flow_analytics import analyze_document, build_raci_rows
from services.flow_diff import compare_documents
from services.flowchart_repository import (
    FlowPermissionError,
    RevisionConflictError,
    add_comment,
    can_approve,
    can_edit,
    can_review,
    compare_versions,
    create_template,
    delete_flowchart,
    delete_template,
    discard_draft,
    duplicate_flowchart,
    get_draft,
    get_flowchart,
    get_version,
    initialize_flowchart_tables,
    list_approval_history,
    list_comments,
    list_custom_templates,
    list_flowcharts,
    list_presence,
    list_versions,
    permission_for,
    resolve_comment,
    save_draft,
    save_flowchart,
    set_collaborators,
    touch_presence,
    transition_workflow,
)
from services.report_export import html_report, nodes_csv, pdf_report, raci_csv
from services.template_library import built_in_templates, clone_template

st.set_page_config(page_title="Editor de Processos Pro", page_icon="◈", layout="wide")
apply_global_styles(full_width=True)
user = require_login()
render_account_sidebar()

try:
    initialize_flowchart_tables()
except RuntimeError as exc:
    st.error(str(exc))
    st.stop()

is_admin = user.get("role") == "admin"
username = str(user["username"]).strip().lower()
user_email = str(user.get("email") or "")


def format_datetime(value) -> str:
    if isinstance(value, datetime):
        try:
            return value.astimezone().strftime("%d/%m/%Y %H:%M")
        except Exception:
            return value.strftime("%d/%m/%Y %H:%M")
    text = str(value or "")
    return text[:19].replace("T", " ") if text else "Sem data"


def serialize_comments(items: list[dict]) -> list[dict]:
    result = []
    for item in items:
        result.append({
            "id": str(item.get("_id") or ""),
            "target_kind": str(item.get("target_kind") or "flow"),
            "target_id": str(item.get("target_id") or ""),
            "content": str(item.get("content") or ""),
            "author": str(item.get("author") or ""),
            "resolved": bool(item.get("resolved")),
            "created_at": format_datetime(item.get("created_at")),
        })
    return result


def flash(message: str, kind: str = "success") -> None:
    st.session_state["flow_flash"] = (message, kind)


def selected_flow_id() -> str | None:
    return st.session_state.get("selected_flowchart_id")


if "flow_flash" in st.session_state:
    message, kind = st.session_state.pop("flow_flash")
    getattr(st, kind)(message)

try:
    flows = list_flowcharts(username, include_all=is_admin)
    if not flows:
        created = save_flowchart(
            demo_flowchart_document(username), username, user_email,
            actor_username=username, is_admin=is_admin, save_reason="initial_demo",
        )
        st.session_state["selected_flowchart_id"] = created["id"]
        flows = list_flowcharts(username, include_all=is_admin)
except Exception as exc:
    st.error(f"Não foi possível acessar os fluxos no MongoDB: {exc}")
    st.stop()

available_ids = [item["id"] for item in flows]
selected_id = selected_flow_id()
if selected_id not in available_ids:
    selected_id = available_ids[0]
    st.session_state["selected_flowchart_id"] = selected_id

# Navegação de subprocessos vinculados.
flow_stack: list[str] = st.session_state.setdefault("flow_navigation_stack", [])

with st.sidebar:
    st.markdown("### Processos")
    if flow_stack and st.button("← Voltar ao fluxo anterior", use_container_width=True):
        previous = flow_stack.pop()
        st.session_state["selected_flowchart_id"] = previous
        st.rerun()

    filter_text = st.text_input("Buscar processo", placeholder="Nome, status ou proprietário")
    filtered = [
        item for item in flows
        if filter_text.lower() in " ".join([
            item["name"], item.get("workflow_status", ""), item.get("owner_username", "")
        ]).lower()
    ]
    options = {
        f"{item['name']} · v{item['current_version']} · {WORKFLOW_STATUS_LABELS.get(item.get('workflow_status', 'draft'), item.get('workflow_status', ''))}": item["id"]
        for item in filtered
    }
    current_label = next((label for label, flow_id in options.items() if flow_id == selected_id), None)
    selected_label = st.selectbox(
        "Fluxo aberto", list(options),
        index=list(options).index(current_label) if current_label in options else 0,
        label_visibility="collapsed",
    ) if options else None
    if selected_label and options[selected_label] != selected_id:
        st.session_state["selected_flowchart_id"] = options[selected_label]
        st.rerun()

    col_new, col_copy = st.columns(2)
    with col_new:
        if st.button("Novo", use_container_width=True, type="primary"):
            created = save_flowchart(
                new_flowchart_document("Novo processo", username), username, user_email,
                actor_username=username, save_reason="new",
            )
            st.session_state["selected_flowchart_id"] = created["id"]
            flash("Novo processo criado.")
            st.rerun()
    with col_copy:
        if st.button("Duplicar", use_container_width=True):
            duplicated = duplicate_flowchart(selected_id, username, user_email, is_admin=is_admin)
            st.session_state["selected_flowchart_id"] = duplicated["id"]
            flash("Processo duplicado.")
            st.rerun()

    st.divider()
    st.markdown("#### Biblioteca de templates")
    builtin = built_in_templates(username)
    custom = list_custom_templates(username, include_all=is_admin)
    template_map = {
        f"{item.get('category', 'Geral')} · {item.get('name')}": item
        for item in [*builtin, *custom]
    }
    template_label = st.selectbox("Template", list(template_map), label_visibility="collapsed")
    if st.button("Criar processo pelo template", use_container_width=True):
        template = template_map[template_label]
        doc = clone_template(template, username)
        created = save_flowchart(doc, username, user_email, actor_username=username, save_reason="template")
        st.session_state["selected_flowchart_id"] = created["id"]
        flash("Processo criado a partir do template.")
        st.rerun()

record = get_flowchart(selected_id, actor_username=username, is_admin=is_admin)
if not record:
    st.error("O processo selecionado não foi encontrado ou você não possui acesso.")
    st.stop()

permission = record.get("permission") or permission_for(record, username, is_admin=is_admin)
editable = can_edit(permission)
reviewable = can_review(permission)
approvable = can_approve(permission)

# Presença colaborativa com expiração automática no MongoDB.
touch_presence(selected_id, username, str(user.get("name") or username))
presence = list_presence(selected_id, exclude_username=username)

# Usa o rascunho automático somente quando ainda corresponde à revisão atual.
draft = get_draft(selected_id, username)
editor_document = record["document"]
using_draft = bool(draft and draft.get("base_revision") == record["revision"])
if using_draft:
    editor_document = draft["document"]

page_header(
    "Editor de Processos Professional",
    "Modele, simule, revise, publique e documente processos com governança, colaboração e versionamento no MongoDB.",
)

status_label = WORKFLOW_STATUS_LABELS.get(record["workflow_status"], record["workflow_status"])
meta1, meta2, meta3, meta4, meta5 = st.columns([2.3, .85, .85, .85, 1])
meta1.markdown(
    f"<strong>{escape(record['name'])}</strong><br><small>{escape(record['description'] or 'Sem descrição')}</small>",
    unsafe_allow_html=True,
)
meta2.metric("Versão", record["current_version"])
meta3.metric("Revisão", record["revision"])
meta4.metric("Status", status_label)
analysis = analyze_document(editor_document)
meta5.metric("Qualidade", f"{analysis['quality_score']}/100")

presence_text = ", ".join(f"{item.get('display_name', item.get('username'))}" for item in presence)
st.caption(
    f"Proprietário: @{record['owner_username']} · Permissão: {FLOW_ACCESS_LABELS.get(permission, 'Proprietário' if permission == 'owner' else permission)} · "
    f"Atualizado por @{record['last_saved_by']} em {format_datetime(record['updated_at'])}"
)
if presence_text:
    st.info(f"Também visualizando este fluxo: {presence_text}")
if using_draft:
    col_draft_info, col_draft_discard = st.columns([4, 1])
    col_draft_info.warning(f"Rascunho automático recuperado de {format_datetime(draft.get('updated_at'))}.")
    if col_draft_discard.button("Descartar rascunho", use_container_width=True):
        discard_draft(selected_id, username)
        flash("Rascunho automático descartado.", "info")
        st.rerun()

# Conflito de concorrência pendente.
conflict = st.session_state.get("flow_conflict")
if conflict and conflict.get("flow_id") == selected_id:
    st.error(
        f"Conflito de edição: a revisão local era {conflict['expected_revision']}, mas o MongoDB já está na revisão {conflict['current_revision']}."
    )
    local_doc = conflict["document"]
    current_doc = conflict.get("current_document") or record["document"]
    conflict_diff = compare_documents(current_doc, local_doc)
    st.json(conflict_diff["summary"], expanded=False)
    reload_col, copy_col, force_col = st.columns(3)
    if reload_col.button("Recarregar versão atual", use_container_width=True):
        st.session_state.pop("flow_conflict", None)
        discard_draft(selected_id, username)
        st.rerun()
    if copy_col.button("Salvar alterações como cópia", use_container_width=True):
        copy_doc = normalize_document(deepcopy(local_doc), username)
        copy_doc["flow"]["id"] = f"flow_{uuid4().hex[:12]}"
        copy_doc["flow"]["name"] = f"Cópia conflitante de {copy_doc['flow'].get('name', 'Processo')}"
        created = save_flowchart(copy_doc, username, user_email, actor_username=username, save_reason="conflict_copy")
        st.session_state["selected_flowchart_id"] = created["id"]
        st.session_state.pop("flow_conflict", None)
        flash("Alterações preservadas em uma nova cópia.")
        st.rerun()
    if force_col.button("Sobrescrever como proprietário", disabled=permission != "owner", use_container_width=True):
        saved = save_flowchart(
            local_doc, record["owner_username"], record.get("owner_email", ""),
            expected_revision=conflict["current_revision"], actor_username=username,
            is_admin=is_admin, force=True, save_reason="force_conflict",
        )
        st.session_state.pop("flow_conflict", None)
        flash(f"Conflito resolvido na revisão {saved['revision']}.")
        st.rerun()

# Painéis de gestão.
manage_tabs = st.tabs(["Governança", "Versões", "Colaboração", "Comentários", "Indicadores e relatórios", "Templates"])

with manage_tabs[0]:
    history = list_approval_history(selected_id)
    action_map: list[tuple[str, str]] = []
    current_status = record["workflow_status"]
    if current_status == "draft" and editable:
        action_map.append(("submit_review", "Enviar para revisão"))
    if current_status in {"in_review", "approved"} and reviewable:
        action_map.append(("request_changes", "Solicitar alterações"))
    if current_status == "in_review" and approvable:
        action_map.append(("approve", "Aprovar"))
    if current_status == "approved" and approvable:
        action_map.append(("publish", "Publicar versão"))
    if current_status in {"published", "approved", "draft"} and approvable:
        action_map.append(("archive", "Arquivar"))
    if current_status == "archived" and approvable:
        action_map.append(("reopen", "Reabrir como rascunho"))

    gov_col1, gov_col2 = st.columns([1.2, 2])
    with gov_col1:
        if action_map:
            selected_action = st.selectbox("Ação", [item[0] for item in action_map], format_func=lambda value: dict(action_map)[value])
            governance_comment = st.text_area("Comentário da decisão", height=90)
            if st.button("Executar transição", type="primary", use_container_width=True):
                try:
                    transition_workflow(selected_id, username, selected_action, comment=governance_comment, is_admin=is_admin)
                    flash("Status de governança atualizado.")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))
        else:
            st.info("Não há transições disponíveis para seu perfil neste status.")
    with gov_col2:
        if history:
            st.dataframe(pd.DataFrame([{
                "Data": format_datetime(item.get("created_at")),
                "De": WORKFLOW_STATUS_LABELS.get(item.get("from_status"), item.get("from_status")),
                "Para": WORKFLOW_STATUS_LABELS.get(item.get("to_status"), item.get("to_status")),
                "Responsável": item.get("created_by"),
                "Comentário": item.get("comment", ""),
            } for item in history]), use_container_width=True, hide_index=True)
        else:
            st.caption("Nenhuma transição registrada.")

with manage_tabs[1]:
    versions = list_versions(selected_id)
    if versions:
        version_values = [item["version"] for item in versions]
        vcol1, vcol2, vcol3 = st.columns([1, 1, 1])
        left_version = vcol1.selectbox("Versão base", version_values, index=min(1, len(version_values)-1), key="diff_left")
        right_version = vcol2.selectbox("Versão comparada", version_values, index=0, key="diff_right")
        if vcol3.button("Comparar versões", use_container_width=True):
            st.session_state["version_diff"] = compare_versions(selected_id, left_version, right_version)
        version_diff = st.session_state.get("version_diff")
        if version_diff:
            st.write("**Resumo das alterações**")
            summary_labels = {
                "nodes_added": "Elementos adicionados", "nodes_removed": "Elementos removidos", "nodes_modified": "Elementos alterados",
                "edges_added": "Conexões adicionadas", "edges_removed": "Conexões removidas", "edges_modified": "Conexões alteradas",
                "lanes_added": "Raias adicionadas", "lanes_removed": "Raias removidas", "lanes_modified": "Raias alteradas",
            }
            summary_cols = st.columns(3)
            for index, (key, label) in enumerate(summary_labels.items()):
                value = int(version_diff["summary"].get(key, 0))
                if value:
                    summary_cols[index % 3].metric(label, value)
            detail_rows = []
            for group, group_label in (("nodes", "Elemento"), ("edges", "Conexão"), ("lanes", "Raia")):
                details = version_diff.get(group, {})
                detail_rows.extend({"Tipo": group_label, "Alteração": "Adicionado", "ID": item_id, "Campos": ""} for item_id in details.get("added", []))
                detail_rows.extend({"Tipo": group_label, "Alteração": "Removido", "ID": item_id, "Campos": ""} for item_id in details.get("removed", []))
                detail_rows.extend({"Tipo": group_label, "Alteração": "Alterado", "ID": item.get("id"), "Campos": ", ".join(item.get("fields", []))} for item in details.get("modified", []))
            if detail_rows:
                st.dataframe(pd.DataFrame(detail_rows), use_container_width=True, hide_index=True)
            else:
                st.success("As versões selecionadas não possuem diferenças estruturais.")
        restore_version = st.selectbox("Restaurar versão", version_values, key="restore_version")
        if st.button("Restaurar como nova versão", disabled=not editable):
            restored = get_version(selected_id, restore_version)
            if restored:
                restored["flow"]["id"] = selected_id
                save_flowchart(
                    restored, record["owner_username"], record.get("owner_email", ""),
                    expected_revision=record["revision"], actor_username=username,
                    is_admin=is_admin, save_reason=f"restore_v{restore_version}",
                )
                flash(f"Versão {restore_version} restaurada como nova versão.")
                st.rerun()
    else:
        st.info("Não há versões anteriores.")

with manage_tabs[2]:
    users = [item for item in db.get_all_users() if item.get("active") is not False and item.get("username") != record["owner_username"]]
    if permission == "owner":
        visibility = st.radio("Visibilidade", ["private", "organization"], index=1 if record.get("visibility") == "organization" else 0, format_func=lambda value: "Toda a organização pode visualizar" if value == "organization" else "Somente convidados", horizontal=True)
        collaborator_rows = {str(item.get("username")): str(item.get("level") or "viewer") for item in record.get("collaborators", [])}
        selected_users = st.multiselect("Colaboradores", [item.get("username") for item in users], default=list(collaborator_rows))
        levels: dict[str, str] = {}
        if selected_users:
            level_columns = st.columns(min(3, len(selected_users)))
            for index, collaborator in enumerate(selected_users):
                levels[collaborator] = level_columns[index % len(level_columns)].selectbox(
                    f"@{collaborator}", list(FLOW_ACCESS_LABELS),
                    index=list(FLOW_ACCESS_LABELS).index(collaborator_rows.get(collaborator, "viewer")),
                    format_func=lambda value: FLOW_ACCESS_LABELS[value], key=f"level_{selected_id}_{collaborator}",
                )
        if st.button("Salvar compartilhamento", type="primary"):
            set_collaborators(selected_id, username, [{"username": item, "level": levels[item]} for item in selected_users], visibility, is_admin=is_admin)
            flash("Compartilhamento atualizado.")
            st.rerun()
    else:
        st.info("Somente o proprietário pode alterar colaboradores.")
    if record.get("collaborators"):
        st.dataframe(pd.DataFrame([{"Usuário": item.get("username"), "Permissão": FLOW_ACCESS_LABELS.get(item.get("level"), item.get("level"))} for item in record["collaborators"]]), use_container_width=True, hide_index=True)

with manage_tabs[3]:
    comments = list_comments(selected_id, include_resolved=True)
    unresolved = [item for item in comments if not item.get("resolved")]
    st.metric("Comentários abertos", len(unresolved))
    if comments:
        for item in comments[:50]:
            state_label = "Resolvido" if item.get("resolved") else "Aberto"
            with st.container(border=True):
                st.markdown(f"**@{item.get('author')}** · {state_label} · {format_datetime(item.get('created_at'))}")
                st.caption(f"{item.get('target_kind')} · {item.get('target_id')}")
                st.write(item.get("content"))
                if st.button("Reabrir" if item.get("resolved") else "Resolver", key=f"resolve_{item.get('_id')}"):
                    resolve_comment(str(item.get("_id")), username, not item.get("resolved"))
                    st.rerun()
    else:
        st.info("Nenhum comentário registrado.")

with manage_tabs[4]:
    score_cols = st.columns(5)
    for index, (key, label) in enumerate([
        ("structure", "Estrutura"), ("documentation", "Documentação"),
        ("responsibility", "Responsáveis"), ("sla", "SLA"), ("subprocesses", "Subprocessos"),
    ]):
        score_cols[index].metric(label, f"{analysis['scores'][key]}%")
    count_df = pd.DataFrame([{"Indicador": key, "Valor": value} for key, value in analysis["counts"].items()])
    st.dataframe(count_df, use_container_width=True, hide_index=True)
    r1, r2, r3, r4 = st.columns(4)
    safe_name = record["name"].replace(" ", "_").lower()
    r1.download_button("Relatório PDF", pdf_report(editor_document), f"{safe_name}.pdf", "application/pdf", use_container_width=True)
    r2.download_button("Relatório HTML", html_report(editor_document), f"{safe_name}.html", "text/html", use_container_width=True)
    r3.download_button("Etapas CSV", nodes_csv(editor_document), f"{safe_name}_etapas.csv", "text/csv", use_container_width=True)
    r4.download_button("Matriz RACI", raci_csv(editor_document), f"{safe_name}_raci.csv", "text/csv", use_container_width=True)
    with st.expander("Problemas identificados"):
        st.json(analysis["issues"], expanded=False)

with manage_tabs[5]:
    template_name = st.text_input("Nome do template", value=f"Template — {record['name']}")
    template_description = st.text_input("Descrição do template")
    template_category = st.text_input("Categoria", value="Corporativo")
    template_org = st.checkbox("Disponibilizar para toda a organização", disabled=not is_admin)
    if st.button("Salvar fluxo atual como template", disabled=not editable):
        create_template(template_name, template_description, template_category, editor_document, username, organization=template_org and is_admin)
        flash("Template salvo na biblioteca.")
        st.rerun()
    custom_templates = list_custom_templates(username, include_all=is_admin)
    if custom_templates:
        template_options = {f"{item.get('category')} · {item.get('name')}": str(item.get('_id')) for item in custom_templates}
        delete_label = st.selectbox("Template personalizado", list(template_options))
        if st.button("Excluir template selecionado"):
            if delete_template(template_options[delete_label], username, is_admin=is_admin):
                flash("Template excluído.")
                st.rerun()

st.divider()
flow_catalog = [{"id": item["id"], "name": item["name"], "status": item.get("workflow_status", "draft")} for item in flows]
comments_for_editor = serialize_comments(list_comments(selected_id, include_resolved=True))

component_key = f"flow_editor_v3_{selected_id}_{record['revision']}"
result = flow_editor(
    editor_document,
    key=component_key,
    height=980,
    theme=get_ui_theme(),
    revision=record["revision"],
    permission=permission or "viewer",
    flow_catalog=flow_catalog,
    comments=comments_for_editor,
    autosave_seconds=int(editor_document.get("settings", {}).get("autosaveSeconds") or 10),
    on_save_change=lambda: None,
    on_autosave_change=lambda: None,
    on_open_flow_change=lambda: None,
    on_comment_create_change=lambda: None,
)

# Eventos emitidos pelo componente V2.
autosave_payload = getattr(result, "autosave", None)
if autosave_payload and editable:
    try:
        autosave_doc = autosave_payload.get("document") if isinstance(autosave_payload, dict) else None
        autosave_revision = int(autosave_payload.get("revision", record["revision"])) if isinstance(autosave_payload, dict) else record["revision"]
        if autosave_doc:
            autosave_doc.setdefault("flow", {})["id"] = selected_id
            save_draft(selected_id, username, autosave_doc, autosave_revision)
    except Exception as exc:
        st.warning(f"O rascunho automático não pôde ser salvo: {exc}")

comment_payload = getattr(result, "comment_create", None)
if comment_payload and editable:
    try:
        add_comment(
            selected_id,
            str(comment_payload.get("targetKind") or "flow"),
            str(comment_payload.get("targetId") or selected_id),
            str(comment_payload.get("content") or ""),
            username,
            list(comment_payload.get("mentions") or []),
        )
        flash("Comentário registrado.")
        st.rerun()
    except Exception as exc:
        st.error(str(exc))

open_payload = getattr(result, "open_flow", None)
if open_payload:
    linked_id = str(open_payload.get("flowId") or "") if isinstance(open_payload, dict) else str(open_payload)
    if linked_id and linked_id in available_ids:
        flow_stack.append(selected_id)
        st.session_state["selected_flowchart_id"] = linked_id
        st.rerun()
    elif linked_id:
        st.warning("O fluxo vinculado não existe ou você não possui acesso.")

save_payload = getattr(result, "save", None)
if save_payload and editable:
    try:
        payload_document = save_payload.get("document") if isinstance(save_payload, dict) and "document" in save_payload else save_payload
        expected_revision = int(save_payload.get("revision", record["revision"])) if isinstance(save_payload, dict) else record["revision"]
        payload_document.setdefault("flow", {})["id"] = selected_id
        saved = save_flowchart(
            payload_document,
            record["owner_username"],
            record.get("owner_email", ""),
            expected_revision=expected_revision,
            actor_username=username,
            is_admin=is_admin,
            save_reason="manual",
        )
        discard_draft(selected_id, username)
        st.session_state["selected_flowchart_id"] = saved["id"]
        flash(f"Processo salvo na versão {saved['version']} e revisão {saved['revision']}.")
        st.rerun()
    except RevisionConflictError as exc:
        st.session_state["flow_conflict"] = {
            "flow_id": selected_id,
            "expected_revision": expected_revision,
            "current_revision": exc.current_revision,
            "document": payload_document,
            "current_document": (exc.current_record or {}).get("document"),
        }
        st.rerun()
    except (FlowPermissionError, ValueError, RuntimeError) as exc:
        st.error(str(exc))

with st.expander("Administração do processo", expanded=False):
    st.download_button(
        "Baixar JSON salvo no MongoDB",
        data=json.dumps(record["document"], ensure_ascii=False, indent=2),
        file_name=f"{record['name'].replace(' ', '_').lower()}.json",
        mime="application/json",
    )
    if permission == "owner":
        st.warning("A exclusão remove o fluxo, as versões, comentários, aprovações e rascunhos.")
        confirm_delete = st.checkbox("Confirmar exclusão permanente", key="confirm_flow_delete")
        if st.button("Excluir processo", disabled=not confirm_delete):
            if delete_flowchart(selected_id, username, is_admin=is_admin):
                st.session_state.pop("selected_flowchart_id", None)
                flash("Processo excluído.")
                st.rerun()
