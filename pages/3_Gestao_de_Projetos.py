from __future__ import annotations

import json
from html import escape
from pathlib import Path

import pandas as pd
import streamlit as st

import database as db
from core.auth import render_account_sidebar, require_login
from core.configuration import (
    FLOW_ACCESS_LABELS,
    PROJECT_ROLE_LABELS,
    PROJECT_STATUS_LABELS,
)
from core.styles import apply_global_styles, page_header
from services.project_repository import (
    ProjectImportError,
    ProjectPermissionError,
    analyze_project,
    assign_flow_to_project,
    can_edit_project,
    can_manage_project,
    create_project,
    create_project_release,
    delete_project,
    detach_flow_from_project,
    export_project_bundle,
    get_project,
    import_documents_as_project,
    import_project_bundle,
    initialize_project_tables,
    list_project_flows,
    list_project_releases,
    list_projects,
    project_impact,
    project_links,
    remove_project_flow_references,
    search_project,
    set_project_members,
    shortest_project_path,
    update_project,
)
from services.flowchart_repository import delete_flowchart, get_flowchart, list_flowcharts, save_flowchart
from schemas.flowchart_schema import new_flowchart_document

st.set_page_config(page_title="Gestão de Projetos", page_icon="📁", layout="wide")
apply_global_styles(full_width=True)
user = require_login()
render_account_sidebar()

username = str(user["username"]).strip().lower()
user_email = str(user.get("email") or "")
is_admin = user.get("role") == "admin"

try:
    initialize_project_tables()
except RuntimeError as exc:
    st.error(str(exc))
    st.stop()


def flash(message: str, kind: str = "success") -> None:
    st.session_state["project_flash"] = (message, kind)


def open_flow(project_id: str, flow_id: str, node_id: str = "") -> None:
    st.session_state["selected_project_id"] = project_id
    st.session_state["selected_flowchart_id"] = flow_id
    tabs = st.session_state.setdefault("project_open_flow_tabs", {})
    open_ids = tabs.setdefault(project_id, [])
    if flow_id not in open_ids:
        open_ids.append(flow_id)
    if node_id:
        st.session_state["project_focus_node"] = {"flow_id": flow_id, "node_id": node_id}
    st.switch_page("pages/5_Editor_de_Fluxos.py")


def clean_dot(value: str) -> str:
    return str(value or "").replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def project_dot(project: dict, flows: list[dict], graph: dict) -> str:
    status_colors = {
        "draft": "#dbeafe", "in_review": "#fef3c7", "approved": "#dcfce7",
        "published": "#bbf7d0", "archived": "#e2e8f0",
    }
    lines = [
        "digraph project {",
        'rankdir="LR";',
        'graph [bgcolor="transparent", pad="0.3", nodesep="0.55", ranksep="0.85", splines="ortho"];',
        'node [shape="box", style="rounded,filled", fontname="Arial", fontsize="10", margin="0.18,0.12", color="#8aa6a0", fontcolor="#102a43"];',
        'edge [color="#5f7f77", penwidth="1.4", arrowsize="0.75", fontname="Arial", fontsize="8", fontcolor="#486581"];',
    ]
    by_id = {item["id"]: item for item in flows}
    for item in flows:
        role = PROJECT_ROLE_LABELS.get(item.get("project_role"), item.get("project_role") or "Fluxo")
        label = f"{item['name']}\\n{role} · v{item.get('current_version', 1)}"
        fill = status_colors.get(item.get("workflow_status", "draft"), "#f7fafc")
        penwidth = "2.4" if item["id"] == project.get("default_flow_id") else "1.2"
        lines.append(f'"{clean_dot(item["id"])}" [label="{clean_dot(label)}", fillcolor="{fill}", penwidth="{penwidth}"];')
    seen: set[tuple[str, str, str]] = set()
    for link in graph.get("links", []):
        source, target = link["source_flow_id"], link["target_flow_id"]
        if source not in by_id or target not in by_id:
            continue
        key = (source, target, link.get("source_node_id", ""))
        if key in seen:
            continue
        seen.add(key)
        label = link.get("source_node_label") or "Subprocesso"
        lines.append(f'"{clean_dot(source)}" -> "{clean_dot(target)}" [label="{clean_dot(label)}"];')
    lines.append("}")
    return "\n".join(lines)


if "project_flash" in st.session_state:
    message, kind = st.session_state.pop("project_flash")
    getattr(st, kind)(message)

import_warnings = st.session_state.pop("project_import_warnings", [])
if import_warnings:
    st.warning(f"A importação aplicou {len(import_warnings)} correção(ões) estrutural(is) segura(s).")
    with st.expander("Ver correções da importação"):
        for warning in import_warnings:
            st.write(f"- {warning}")

page_header(
    "Projetos e fluxos vinculados",
    "Agrupe visões executivas, fluxos operacionais e subprocessos em um workspace único, com busca, impacto, execução guiada e releases consolidadas.",
)

try:
    projects = list_projects(username, include_all=is_admin, is_admin=is_admin)
except Exception as exc:
    st.error(f"Não foi possível listar os projetos: {exc}")
    st.stop()

with st.sidebar:
    st.markdown("### Projetos")
    project_options = {f"{item['name']} · {item.get('flow_count', 0)} fluxos": item["id"] for item in projects}
    selected_project_id = st.session_state.get("selected_project_id")
    selected_label = next((label for label, value in project_options.items() if value == selected_project_id), None)
    if project_options:
        label = st.selectbox(
            "Projeto aberto",
            list(project_options),
            index=list(project_options).index(selected_label) if selected_label in project_options else 0,
            label_visibility="collapsed",
        )
        if project_options[label] != selected_project_id:
            selected_project_id = project_options[label]
            st.session_state["selected_project_id"] = selected_project_id
            st.rerun()
    st.divider()
    with st.expander("Novo projeto", expanded=not projects):
        with st.form("new_project_form"):
            new_name = st.text_input("Nome", placeholder="Ex.: SIGYO Modular")
            new_code = st.text_input("Código", placeholder="SIGYO")
            new_description = st.text_area("Descrição", height=90)
            if st.form_submit_button("Criar projeto", type="primary", use_container_width=True):
                try:
                    created = create_project(new_name, new_description, username, user_email, code=new_code)
                    st.session_state["selected_project_id"] = created["id"]
                    flash("Projeto criado com sucesso.")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))

    sample_path = Path(__file__).resolve().parents[1] / "examples" / "sigyo_modular_project.zip"
    if sample_path.exists() and st.button("Importar exemplo SIGYO", use_container_width=True):
        try:
            imported = import_project_bundle(sample_path.read_bytes(), username, user_email, preserve_ids=False, is_admin=is_admin)
            st.session_state["selected_project_id"] = imported["project"]["id"]
            st.session_state["project_import_warnings"] = imported.get("warnings") or []
            flash("Projeto SIGYO importado com seus fluxos vinculados.")
            st.rerun()
        except Exception as exc:
            st.error(str(exc))

    if st.button("Abrir mapa de relações", use_container_width=True):
        st.switch_page("pages/4_Mapa_de_Relacoes.py")

if not projects:
    st.info("Crie um projeto ou importe o pacote SIGYO para começar.")
    st.stop()

selected_project_id = st.session_state.get("selected_project_id") or projects[0]["id"]
st.session_state["selected_project_id"] = selected_project_id
project = get_project(selected_project_id, username, is_admin=is_admin)
if not project:
    st.error("O projeto selecionado não foi encontrado ou você não possui acesso.")
    st.stop()

flows = list_project_flows(selected_project_id, username, is_admin=is_admin)
flow_by_id = {item["id"]: item for item in flows}
graph = project_links(selected_project_id, username, is_admin=is_admin)
analysis = analyze_project(selected_project_id, username, is_admin=is_admin)
permission = project.get("permission")
editable = can_edit_project(permission)
manageable = can_manage_project(permission)

st.markdown(
    f"""
    <section class="pt-hero">
      <h1>{escape(project['name'])}</h1>
      <p>{escape(project.get('description') or 'Projeto sem descrição')} · Código {escape(project.get('code') or '—')} · {escape(PROJECT_STATUS_LABELS.get(project.get('status'), project.get('status', '')))}</p>
    </section>
    """,
    unsafe_allow_html=True,
)

metrics = st.columns(6)
metrics[0].metric("Fluxos", analysis["flow_count"])
metrics[1].metric("Elementos", analysis["node_count"])
metrics[2].metric("Conexões", analysis["edge_count"])
metrics[3].metric("Vínculos", analysis["link_count"])
metrics[4].metric("Qualidade", f"{analysis['quality_score']}/100")
metrics[5].metric("Release", project.get("current_release") or "—")

st.caption(
    f"Proprietário: @{project['owner_username']} · Permissão: {FLOW_ACCESS_LABELS.get(permission, 'Proprietário' if permission == 'owner' else permission)} · "
    f"Vínculos quebrados: {analysis['broken_count']} · Ciclos entre fluxos: {analysis['cycle_count']}"
)

main_tabs = st.tabs([
    "Mapa do projeto", "Fluxos", "Busca global", "Execução entre fluxos",
    "Impacto e qualidade", "Releases", "Importar e exportar", "Configurações e exclusão",
])

with main_tabs[0]:
    if flows:
        st.graphviz_chart(project_dot(project, flows, graph), use_container_width=True)
        open_col, action_col = st.columns([3, 1])
        selected_map_flow = open_col.selectbox(
            "Abrir fluxo do mapa",
            [item["id"] for item in flows],
            format_func=lambda value: flow_by_id[value]["name"],
            key="project_map_flow",
        )
        if action_col.button("Abrir no editor", type="primary", use_container_width=True):
            open_flow(selected_project_id, selected_map_flow)
        if st.button("Explorar no mapa de relações", use_container_width=False):
            st.switch_page("pages/4_Mapa_de_Relacoes.py")
    else:
        st.info("O projeto ainda não possui fluxos.")

    if graph["broken"]:
        st.error(f"Foram encontrados {len(graph['broken'])} vínculos quebrados.")
        st.dataframe(pd.DataFrame([{
            "Fluxo de origem": item["source_flow_name"],
            "Card": item["source_node_label"],
            "Destino": item["target_flow_id"],
            "Problema": ", ".join(item["reasons"]),
        } for item in graph["broken"]]), use_container_width=True, hide_index=True)

with main_tabs[1]:
    create_col, spacer_col = st.columns([1, 3])
    if create_col.button("Novo fluxo no projeto", type="primary", disabled=not editable, use_container_width=True):
        document = new_flowchart_document("Novo fluxo do projeto", username)
        document["flow"].update({
            "projectId": selected_project_id,
            "projectRole": "subprocess",
            "projectGroup": "Geral",
            "projectOrder": len(flows) + 1,
        })
        created = save_flowchart(document, username, user_email, actor_username=username, is_admin=is_admin, save_reason="project_new_flow")
        if not project.get("default_flow_id"):
            update_project(selected_project_id, username, default_flow_id=created["id"], is_admin=is_admin)
        flash("Novo fluxo criado dentro do projeto.")
        open_flow(selected_project_id, created["id"])
    if flows:
        st.dataframe(pd.DataFrame([{
            "Ordem": item.get("project_order", 0),
            "Fluxo": item["name"],
            "Papel": PROJECT_ROLE_LABELS.get(item.get("project_role"), item.get("project_role") or "Não definido"),
            "Grupo": item.get("project_group") or "—",
            "Versão": item.get("current_version"),
            "Revisão": item.get("revision"),
            "Status": item.get("workflow_status"),
        } for item in flows]), use_container_width=True, hide_index=True)
        flow_select = st.selectbox("Fluxo para administrar", [item["id"] for item in flows], format_func=lambda value: flow_by_id[value]["name"], key="manage_flow")
        f1, f2, f3, f4 = st.columns([1.4, 1, 1, 1])
        current = flow_by_id[flow_select]
        new_role = f1.selectbox("Papel", list(PROJECT_ROLE_LABELS), index=list(PROJECT_ROLE_LABELS).index(current.get("project_role")) if current.get("project_role") in PROJECT_ROLE_LABELS else 2, format_func=lambda value: PROJECT_ROLE_LABELS[value])
        new_group = f2.text_input("Grupo", value=current.get("project_group") or "")
        new_order = f3.number_input("Ordem", min_value=0, value=int(current.get("project_order") or 0), step=1)
        if f4.button("Aplicar", disabled=not editable, use_container_width=True):
            try:
                assign_flow_to_project(selected_project_id, flow_select, username, role=new_role, group=new_group, order=int(new_order), is_admin=is_admin)
                flash("Metadados do fluxo atualizados.")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))
        a1, a2 = st.columns(2)
        if a1.button("Abrir fluxo", type="primary", use_container_width=True):
            open_flow(selected_project_id, flow_select)
        if a2.button("Desvincular do projeto", disabled=not editable, use_container_width=True):
            try:
                detach_flow_from_project(selected_project_id, flow_select, username, is_admin=is_admin)
                flash("Fluxo removido do projeto e mantido como fluxo avulso.", "info")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))

        selected_flow_record = get_flowchart(flow_select, actor_username=username, is_admin=is_admin)
        flow_can_delete = bool(selected_flow_record and (selected_flow_record.get("permission") == "owner" or is_admin))
        with st.expander("Excluir fluxo permanentemente", expanded=False):
            impacts = project_impact(selected_project_id, username, flow_select, is_admin=is_admin)
            if impacts:
                st.warning(f"Este fluxo é usado por {len(impacts)} card(s) de outros fluxos.")
                st.dataframe(pd.DataFrame([{
                    "Fluxo pai": item.get("source_flow_name"),
                    "Card": item.get("source_node_label"),
                } for item in impacts]), use_container_width=True, hide_index=True)
            clean_references = st.checkbox(
                "Remover automaticamente os vínculos que apontam para este fluxo",
                value=True,
                key=f"clean_refs_{flow_select}",
            )
            flow_confirmation = st.text_input(
                "Digite o nome exato do fluxo para confirmar",
                key=f"delete_flow_confirmation_{flow_select}",
            )
            delete_disabled = not flow_can_delete or flow_confirmation != current["name"]
            if st.button(
                "Excluir fluxo e seu histórico",
                disabled=delete_disabled,
                key=f"delete_flow_permanent_{flow_select}",
                type="primary",
            ):
                try:
                    if clean_references:
                        remove_project_flow_references(selected_project_id, flow_select, username, is_admin=is_admin)
                    if project.get("default_flow_id") == flow_select:
                        remaining_id = next((item["id"] for item in flows if item["id"] != flow_select), "")
                        update_project(selected_project_id, username, default_flow_id=remaining_id, is_admin=is_admin)
                    if not delete_flowchart(flow_select, username, is_admin=is_admin):
                        raise ProjectPermissionError("Somente o proprietário do fluxo pode excluí-lo permanentemente.")
                    flash("Fluxo excluído permanentemente.", "info")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))
            if not flow_can_delete:
                st.caption("Somente o proprietário do fluxo ou um administrador pode realizar a exclusão permanente.")
    else:
        st.info("Nenhum fluxo vinculado.")

    accessible = list_flowcharts(username, include_all=is_admin)
    available = [item for item in accessible if item.get("project_id") != selected_project_id]
    with st.expander("Adicionar fluxo existente", expanded=not flows):
        if available:
            existing_id = st.selectbox("Fluxo", [item["id"] for item in available], format_func=lambda value: next(item["name"] for item in available if item["id"] == value))
            role = st.selectbox("Papel inicial", list(PROJECT_ROLE_LABELS), format_func=lambda value: PROJECT_ROLE_LABELS[value], key="attach_role")
            group = st.text_input("Grupo", key="attach_group")
            if st.button("Adicionar ao projeto", disabled=not editable):
                try:
                    assign_flow_to_project(selected_project_id, existing_id, username, role=role, group=group, order=len(flows) + 1, is_admin=is_admin)
                    flash("Fluxo adicionado ao projeto.")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))
        else:
            st.caption("Não há outros fluxos disponíveis para vincular.")

with main_tabs[2]:
    query = st.text_input("Buscar em todos os fluxos", placeholder="Card, descrição, responsável, tag, raia ou ID")
    results = search_project(selected_project_id, username, query, is_admin=is_admin) if query.strip() else []
    if query and not results:
        st.info("Nenhum resultado encontrado.")
    for index, item in enumerate(results):
        col_text, col_open = st.columns([5, 1])
        col_text.markdown(f"**{escape(item['label'])}** · {escape(item['flow_name'])}")
        col_text.caption(item.get("context") or item.get("kind"))
        if col_open.button("Abrir", key=f"search_open_{index}", use_container_width=True):
            open_flow(selected_project_id, item["flow_id"], item["target_id"] if item["kind"] == "node" else "")

with main_tabs[3]:
    if len(flows) < 2:
        st.info("Adicione pelo menos dois fluxos para calcular uma rota entre eles.")
    else:
        c1, c2 = st.columns(2)
        source_id = c1.selectbox("Fluxo de origem", [item["id"] for item in flows], format_func=lambda value: flow_by_id[value]["name"], key="route_source")
        target_id = c2.selectbox("Fluxo de destino", [item["id"] for item in flows], index=min(1, len(flows) - 1), format_func=lambda value: flow_by_id[value]["name"], key="route_target")
        route = shortest_project_path(selected_project_id, username, source_id, target_id, is_admin=is_admin)
        if route:
            st.success(" → ".join(flow_by_id[item]["name"] for item in route))
            st.caption("A execução guiada abre cada fluxo em uma aba interna e mantém a sequência macro do projeto.")
            if st.button("Iniciar execução guiada", type="primary"):
                st.session_state["project_execution"] = {
                    "project_id": selected_project_id,
                    "path": route,
                    "index": 0,
                    "started_by": username,
                }
                open_flow(selected_project_id, route[0])
        else:
            st.warning("Não existe um caminho de subprocessos entre os fluxos selecionados.")
        if graph["links"]:
            st.dataframe(pd.DataFrame([{
                "Origem": item["source_flow_name"],
                "Card de transferência": item["source_node_label"],
                "Destino": flow_by_id.get(item["target_flow_id"], {}).get("name", item["target_flow_id"]),
                "Entrada": item.get("entry_node_id") or "Automática",
                "Saída": item.get("exit_node_id") or "Automática",
            } for item in graph["links"]]), use_container_width=True, hide_index=True)

with main_tabs[4]:
    qcols = st.columns(4)
    qcols[0].metric("Qualidade consolidada", f"{analysis['quality_score']}/100")
    qcols[1].metric("Problemas internos", analysis["issue_count"])
    qcols[2].metric("Vínculos quebrados", analysis["broken_count"])
    qcols[3].metric("Fluxos órfãos", len(analysis["orphans"]))
    if analysis["quality_rows"]:
        st.dataframe(pd.DataFrame([{
            "Fluxo": item["name"], "Qualidade": item["quality_score"],
            "Problemas": item["issues"], "Elementos": item["nodes"], "Conexões": item["edges"],
        } for item in analysis["quality_rows"]]), use_container_width=True, hide_index=True)
    if flows:
        impacted_id = st.selectbox("Analisar impacto de alteração em", [item["id"] for item in flows], format_func=lambda value: flow_by_id[value]["name"], key="impact_flow")
        impacts = project_impact(selected_project_id, username, impacted_id, is_admin=is_admin)
        if impacts:
            st.warning(f"Este fluxo é referenciado por {len(impacts)} card(s) em outros fluxos.")
            st.dataframe(pd.DataFrame([{
                "Fluxo afetado": item["source_flow_name"], "Card": item["source_node_label"],
                "Entrada esperada": item["entry_node_id"] or "—", "Saída esperada": item["exit_node_id"] or "—",
            } for item in impacts]), use_container_width=True, hide_index=True)
        else:
            st.success("Nenhum fluxo pai depende diretamente deste fluxo.")

with main_tabs[5]:
    releases = list_project_releases(selected_project_id, username, is_admin=is_admin)
    with st.form("create_project_release"):
        release_name = st.text_input("Nome da release", value=f"{project['name']} {len(releases) + 1}.0")
        release_notes = st.text_area("Notas", height=90)
        submitted = st.form_submit_button("Criar release consolidada", type="primary", disabled=not manageable)
        if submitted:
            try:
                created_release = create_project_release(selected_project_id, username, name=release_name, notes=release_notes, is_admin=is_admin)
                flash(f"Release {created_release['version']} criada com {len(created_release['flows'])} fluxos.")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))
    if releases:
        st.dataframe(pd.DataFrame([{
            "Release": item["version"], "Nome": item.get("name"), "Fluxos": len(item.get("flows", [])),
            "Qualidade": item.get("quality_score"), "Criado por": item.get("created_by"), "Data": item.get("created_at"),
        } for item in releases]), use_container_width=True, hide_index=True)
        release_version = st.selectbox("Exportar release", [item["version"] for item in releases])
        st.download_button(
            "Baixar pacote imutável da release",
            export_project_bundle(selected_project_id, username, is_admin=is_admin, release_version=int(release_version)),
            file_name=f"{project.get('code') or project['name']}_release_{release_version}.zip".lower().replace(" ", "_"),
            mime="application/zip",
        )

with main_tabs[6]:
    current_bundle = export_project_bundle(selected_project_id, username, is_admin=is_admin)
    st.download_button(
        "Baixar projeto completo",
        current_bundle,
        file_name=f"{project.get('code') or project['name']}_project.zip".lower().replace(" ", "_"),
        mime="application/zip",
        type="primary",
    )
    st.caption("O pacote contém project.json e todos os fluxos em flows/*.json, preservando vínculos pai-filho.")
    st.divider()
    package = st.file_uploader("Importar pacote de projeto", type=["zip"], key="project_zip_upload")
    preserve_ids = st.checkbox("Preservar IDs quando não houver conflito", value=True)
    if package and st.button("Importar pacote"):
        try:
            imported = import_project_bundle(package.getvalue(), username, user_email, preserve_ids=preserve_ids, is_admin=is_admin)
            st.session_state["selected_project_id"] = imported["project"]["id"]
            st.session_state["project_import_warnings"] = imported.get("warnings") or []
            flash(f"Projeto importado com {len(imported['flow_ids'])} fluxos.")
            st.rerun()
        except (ProjectImportError, ValueError, RuntimeError) as exc:
            st.error(str(exc))
    st.divider()
    json_files = st.file_uploader("Importar vários JSONs como um novo projeto", type=["json"], accept_multiple_files=True, key="multi_json_upload")
    import_name = st.text_input("Nome do novo projeto", value="Projeto importado")
    import_description = st.text_area("Descrição do novo projeto", height=80)
    if json_files and st.button("Criar projeto com os JSONs"):
        try:
            documents = [json.loads(item.getvalue().decode("utf-8-sig")) for item in json_files]
            created = import_documents_as_project(documents, import_name, import_description, username, user_email)
            st.session_state["selected_project_id"] = created["id"]
            st.session_state["project_import_warnings"] = created.get("import_warnings") or []
            flash(f"Projeto criado com {len(documents)} fluxos.")
            st.rerun()
        except Exception as exc:
            st.error(str(exc))

with main_tabs[7]:
    with st.form("project_settings"):
        name = st.text_input("Nome", value=project["name"])
        code = st.text_input("Código", value=project.get("code") or "")
        description = st.text_area("Descrição", value=project.get("description") or "", height=110)
        status = st.selectbox("Status", list(PROJECT_STATUS_LABELS), index=list(PROJECT_STATUS_LABELS).index(project.get("status")) if project.get("status") in PROJECT_STATUS_LABELS else 0, format_func=lambda value: PROJECT_STATUS_LABELS[value])
        visibility = st.radio("Visibilidade", ["private", "organization"], index=1 if project.get("visibility") == "organization" else 0, format_func=lambda value: "Organização" if value == "organization" else "Privado", horizontal=True)
        default_flow = st.selectbox("Fluxo inicial", [""] + [item["id"] for item in flows], index=([""] + [item["id"] for item in flows]).index(project.get("default_flow_id")) if project.get("default_flow_id") in [item["id"] for item in flows] else 0, format_func=lambda value: "Não definido" if not value else flow_by_id[value]["name"])
        tags_text = st.text_input("Tags", value=", ".join(project.get("tags") or []))
        if st.form_submit_button("Salvar configurações", type="primary", disabled=not editable):
            try:
                update_project(
                    selected_project_id, username, name=name, description=description, code=code,
                    status=status, visibility=visibility, default_flow_id=default_flow,
                    tags=[item.strip() for item in tags_text.split(",") if item.strip()], is_admin=is_admin,
                )
                flash("Configurações do projeto salvas.")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))

    st.markdown("#### Participantes")
    users = [item for item in db.get_all_users() if item.get("active") is not False and item.get("username") != project["owner_username"]]
    existing_members = {item["username"]: item.get("level", "viewer") for item in project.get("members") or []}
    selected_members = st.multiselect("Usuários", [item.get("username") for item in users], default=list(existing_members), disabled=not manageable)
    member_levels: dict[str, str] = {}
    if selected_members:
        cols = st.columns(min(3, len(selected_members)))
        for index, member in enumerate(selected_members):
            levels = list(FLOW_ACCESS_LABELS)
            member_levels[member] = cols[index % len(cols)].selectbox(
                f"@{member}", levels,
                index=levels.index(existing_members.get(member, "viewer")),
                format_func=lambda value: FLOW_ACCESS_LABELS[value],
                key=f"project_member_{selected_project_id}_{member}", disabled=not manageable,
            )
    if st.button("Salvar participantes", disabled=not manageable):
        try:
            set_project_members(selected_project_id, username, [{"username": member, "level": member_levels[member]} for member in selected_members], is_admin=is_admin)
            flash("Participantes atualizados.")
            st.rerun()
        except Exception as exc:
            st.error(str(exc))

    with st.expander("Excluir projeto", expanded=False):
        st.warning("Você pode apenas desagrupar os fluxos ou excluir também todos os fluxos do projeto.")
        delete_flows = st.checkbox("Excluir também todos os fluxos")
        project_delete_token = str(project.get("code") or project.get("name") or "").strip()
        confirmation = st.text_input(f'Digite exatamente "{project_delete_token}" para confirmar')
        can_delete_project = permission == "owner" or is_admin
        if st.button("Excluir projeto permanentemente", disabled=not can_delete_project or confirmation.strip() != project_delete_token):
            try:
                delete_project(selected_project_id, username, delete_flows=delete_flows, is_admin=is_admin)
                st.session_state.pop("selected_project_id", None)
                flash("Projeto excluído.", "info")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))
