from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pandas as pd
import streamlit as st

from core.auth import render_account_sidebar, require_login
from core.styles import apply_global_styles, page_header
from services.project_repository import list_projects
from services.wbs_repository import (
    WbsPermissionError,
    WbsRevisionConflict,
    create_wbs,
    delete_wbs,
    duplicate_wbs,
    get_wbs,
    initialize_wbs_tables,
    list_wbs,
    save_wbs,
)
from services.wbs_tools import (
    STATUS_LABELS,
    STATUS_OPTIONS,
    delete_node,
    export_import_template,
    export_wbs,
    graphviz_dot,
    import_wbs,
    new_node,
    normalize_nodes,
    outline_text,
    primary_branch_map,
    select_graph_nodes,
    summary_metrics,
    table_rows,
    top_level_rollup,
)

st.set_page_config(page_title="WBS / EAP", page_icon="🌳", layout="wide")
apply_global_styles(full_width=True)
user = require_login()
render_account_sidebar()
username = str(user["username"]).strip().lower()
is_admin = user.get("role") == "admin"
initialize_wbs_tables()

page_header(
    "WBS — Work Breakdown Structure",
    "Crie, edite, visualize e publique a Estrutura Analítica do Projeto (EAP/WBS), com importação e exportação multiformato.",
)

projects = list_projects(username, include_all=is_admin, is_admin=is_admin)
project_by_id = {item["id"]: item for item in projects}
project_options = [""] + [item["id"] for item in projects]


def project_label(project_id: str) -> str:
    if not project_id:
        return "Sem projeto / WBS pessoal"
    item = project_by_id.get(project_id)
    return f"{item.get('code') + ' · ' if item and item.get('code') else ''}{item.get('name') if item else project_id}"


def rerun_select(wbs_id: str | None = None) -> None:
    if wbs_id is not None:
        st.session_state["wbs_selected_id"] = wbs_id
    st.rerun()


def apply_wbs_visual_styles() -> None:
    st.markdown(
        """
<style>
[data-testid="stMetric"] {
    background: linear-gradient(180deg, #0F172A 0%, #1E293B 100%);
    border: 1px solid rgba(148, 163, 184, .22);
    border-radius: 14px;
    padding: 10px 14px;
}
[data-testid="stMetricLabel"],
[data-testid="stMetricValue"] {
    color: #E5E7EB;
}
div[data-testid="stDataFrame"],
div[data-testid="stDataEditor"] {
    border: 1px solid rgba(71, 85, 105, .55);
    border-radius: 12px;
    overflow: hidden;
}
div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
    background: rgba(30, 41, 59, .10);
    border-radius: 10px 10px 0 0;
}
</style>
        """,
        unsafe_allow_html=True,
    )


def visual_table_frame(nodes: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(table_rows(nodes))

    if frame.empty:
        return frame

    normalized = normalize_nodes(nodes)
    branch_ids = primary_branch_map(normalized)

    branch_labels = {
        str(node["id"]): f"{node['code']} · {node['name']}"
        for node in normalized
    }

    frame["Ramo principal"] = frame["ID"].map(
        lambda node_id: branch_labels.get(
            branch_ids.get(str(node_id), ""),
            "",
        )
    )

    def hierarchy_label(row: pd.Series) -> str:
        level = max(1, int(row.get("Nível", 1)))
        indent = " " * (level - 1)
        marker = "↳ " if level > 1 else ""
        return f"{indent}{marker}{row['Código']} · {row['Nome']}"

    frame["Estrutura"] = frame.apply(hierarchy_label, axis=1)
    return frame


def style_readonly_table(frame: pd.DataFrame):
    def row_style(row: pd.Series) -> list[str]:
        level = int(row.get("Nível", 1) or 1)

        if level == 1:
            background = "#172554"
        elif level == 2:
            background = "#1E293B"
        else:
            background = "#0F172A" if row.name % 2 == 0 else "#111827"

        style = (
            f"background-color: {background}; "
            "color: #E5E7EB; "
            "border-bottom: 1px solid #334155;"
        )
        return [style] * len(row)

    return frame.style.apply(row_style, axis=1).set_table_styles(
        [
            {
                "selector": "th",
                "props": [
                    ("background-color", "#020617"),
                    ("color", "#F8FAFC"),
                    ("font-weight", "600"),
                    ("border-bottom", "1px solid #475569"),
                ],
            }
        ]
    )


def render_import_template_downloads(key_prefix: str) -> None:
    st.caption(
        "Baixe um modelo pronto, preencha sua estrutura e envie o mesmo arquivo "
        "no campo de importação. O Excel inclui exemplos, instruções e valores aceitos."
    )
    c1, c2 = st.columns(2)
    for column, label, fmt in (
        (c1, "Baixar modelo Excel", "xlsx"),
        (c2, "Baixar modelo CSV", "csv"),
    ):
        data, mime, filename = export_import_template(fmt)
        column.download_button(
            label,
            data=data,
            file_name=filename,
            mime=mime,
            use_container_width=True,
            key=f"{key_prefix}_{fmt}",
        )


apply_wbs_visual_styles()

with st.expander("➕ Criar nova WBS", expanded=False):
    with st.form("create_wbs_form", clear_on_submit=False):
        c1, c2 = st.columns([2, 1])
        new_name = c1.text_input("Nome da WBS", placeholder="Ex.: Implantação do Produto Tools")
        new_project = c2.selectbox("Projeto relacionado", project_options, format_func=project_label, key="create_wbs_project")
        new_description = st.text_area("Descrição / objetivo", height=90)
        c3, c4 = st.columns(2)
        new_visibility = c3.selectbox("Visibilidade", ["private", "organization"], format_func=lambda v: "Privada" if v == "private" else "Organização")
        create_submitted = c4.form_submit_button("Criar WBS", type="primary", use_container_width=True)
    if create_submitted:
        try:
            created = create_wbs(
                new_name,
                username,
                description=new_description,
                project_id=new_project,
                visibility=new_visibility,
                is_admin=is_admin,
            )
            st.success("WBS criada.")
            rerun_select(created["id"])
        except Exception as exc:
            st.error(str(exc))

wbss = list_wbs(username, is_admin=is_admin)
if not wbss:
    st.info("Nenhuma WBS disponível. Crie uma acima ou importe um arquivo na seção de importação rápida.")
    st.markdown("#### Modelo para importação")
    render_import_template_downloads("wbs_empty_template")
    uploaded_empty = st.file_uploader(
        "Importar WBS para começar",
        type=["json", "xml", "xlsx", "xls", "csv", "tsv", "txt", "md", "pdf"],
        key="wbs_empty_import",
    )
    if uploaded_empty:
        try:
            parsed = import_wbs(uploaded_empty.name, uploaded_empty.getvalue())
            st.write(f"**{parsed['name']}** — {len(parsed['nodes'])} itens identificados")
            if st.button("Criar WBS com este arquivo", type="primary"):
                created = create_wbs(
                    parsed["name"],
                    username,
                    description=parsed.get("description", ""),
                    nodes=parsed["nodes"],
                    is_admin=is_admin,
                )
                rerun_select(created["id"])
        except Exception as exc:
            st.error(f"Falha na importação: {exc}")
    st.stop()

wbs_by_id = {item["id"]: item for item in wbss}
current_default = st.session_state.get("wbs_selected_id")
if current_default not in wbs_by_id:
    current_default = wbss[0]["id"]

selector_col, duplicate_col, delete_col = st.columns([6, 1, 1])
selected_id = selector_col.selectbox(
    "WBS ativa",
    [item["id"] for item in wbss],
    index=[item["id"] for item in wbss].index(current_default),
    format_func=lambda value: f"{wbs_by_id[value]['name']} · rev. {wbs_by_id[value].get('revision', 1)}",
    key="wbs_selector",
)
st.session_state["wbs_selected_id"] = selected_id
record = get_wbs(selected_id, username, is_admin=is_admin)
if not record:
    st.error("A WBS selecionada não está mais disponível.")
    st.stop()

can_edit = bool(record.get("can_edit"))
can_delete = bool(record.get("can_delete"))

if duplicate_col.button("Duplicar", use_container_width=True, disabled=not can_edit):
    try:
        duplicate = duplicate_wbs(selected_id, username, is_admin=is_admin)
        st.success("Cópia criada.")
        rerun_select(duplicate["id"])
    except Exception as exc:
        st.error(str(exc))

if delete_col.button("Excluir", use_container_width=True, disabled=not can_delete):
    st.session_state["wbs_confirm_delete"] = True

if st.session_state.get("wbs_confirm_delete"):
    st.warning(f"Confirme a exclusão definitiva da WBS **{record['name']}**.")
    d1, d2, _ = st.columns([1, 1, 6])
    if d1.button("Confirmar exclusão", type="primary"):
        try:
            delete_wbs(selected_id, username, is_admin=is_admin)
            st.session_state.pop("wbs_confirm_delete", None)
            st.session_state.pop("wbs_selected_id", None)
            st.success("WBS excluída.")
            st.rerun()
        except Exception as exc:
            st.error(str(exc))
    if d2.button("Cancelar"):
        st.session_state.pop("wbs_confirm_delete", None)
        st.rerun()

nodes = normalize_nodes(record.get("nodes") or []) if record.get("nodes") else []
metrics = summary_metrics(nodes) if nodes else {"packages": 0, "work_packages": 0, "levels": 0, "total_cost": 0, "progress_percent": 0, "done": 0, "blocked": 0}

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Itens WBS", metrics["packages"])
m2.metric("Pacotes de trabalho", metrics["work_packages"])
m3.metric("Níveis", metrics["levels"])
m4.metric("Progresso", f"{metrics['progress_percent']:.1f}%")
m5.metric("Custo total", f"R$ {metrics['total_cost']:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

status_label = {"draft": "Rascunho", "in_review": "Em revisão", "published": "Publicado", "archived": "Arquivado"}.get(record.get("status"), record.get("status"))
st.caption(
    f"Projeto: **{project_label(record.get('project_id', ''))}** · Status: **{status_label}** · "
    f"Revisão: **{record.get('revision', 1)}** · Última alteração: **{record.get('last_saved_by', record.get('owner_username', ''))}**"
)

if not can_edit:
    st.info("Você possui acesso somente para visualização desta WBS.")

tabs = st.tabs(["🌳 Gráfico", "📋 Tabela", "✏️ Editor", "📥 Importar / Exportar", "📊 Indicadores", "⚙️ Configurações"])

with tabs[0]:
    if not nodes:
        st.info("Adicione pacotes na aba Editor.")
    else:
        st.markdown("#### Visualização gráfica")

        g1, g2, g3, g4 = st.columns([1.5, 1.2, 1, 1])

        graph_mode = g1.selectbox(
            "Modo de exibição",
            ["Completa", "Até um nível", "Subárvore"],
            index=1 if len(nodes) > 80 else 0,
            key="wbs_graph_mode",
        )

        direction = g2.radio(
            "Orientação",
            ["Vertical", "Horizontal"],
            horizontal=True,
            key="wbs_graph_direction",
        )

        compact_graph = g3.checkbox(
            "Modo compacto",
            value=len(nodes) > 40,
            key="wbs_graph_compact",
        )

        show_owner = g4.checkbox(
            "Responsável",
            value=len(nodes) <= 80,
            key="wbs_graph_owner",
        )

        d1, d2 = st.columns(2)
        show_status = d1.checkbox(
            "Exibir status",
            value=True,
            key="wbs_graph_status",
        )
        show_deliverable = d2.checkbox(
            "Exibir entregável",
            value=False,
            key="wbs_graph_deliverable",
        )

        graph_nodes = nodes

        if graph_mode == "Até um nível":
            default_level = 2 if len(nodes) > 120 else min(3, int(metrics["levels"]))
            max_visible_level = st.slider(
                "Níveis visíveis",
                min_value=1,
                max_value=max(1, int(metrics["levels"])),
                value=max(1, default_level),
                key="wbs_graph_max_level",
            )
            graph_nodes = select_graph_nodes(
                nodes,
                max_level=max_visible_level,
            )

        elif graph_mode == "Subárvore":
            focus_id = st.selectbox(
                "Pacote raiz da subárvore",
                [node["id"] for node in nodes],
                format_func=lambda value: next(
                    f"{node['code']} · {node['name']}"
                    for node in nodes
                    if node["id"] == value
                ),
                key="wbs_graph_focus",
            )

            max_relative_depth = st.slider(
                "Profundidade da subárvore",
                min_value=0,
                max_value=max(0, int(metrics["levels"]) - 1),
                value=min(3, max(0, int(metrics["levels"]) - 1)),
                key="wbs_graph_depth",
            )

            graph_nodes = select_graph_nodes(
                nodes,
                focus_id=focus_id,
                max_relative_depth=max_relative_depth,
            )

        if graph_mode == "Completa" and len(nodes) > 120:
            st.warning(
                "Esta WBS é muito grande para uma leitura confortável em uma única árvore. "
                "Use 'Até um nível' ou 'Subárvore' para navegar."
            )

        st.caption(
            f"Exibindo **{len(graph_nodes)}** de **{len(nodes)}** itens."
        )

        st.graphviz_chart(
            graphviz_dot(
                graph_nodes,
                rankdir="LR" if direction == "Horizontal" else "TB",
                compact=compact_graph,
                include_owner=show_owner,
                include_status=show_status,
                include_deliverable=show_deliverable,
            ),
            use_container_width=True,
        )

        with st.expander("Árvore textual / outline", expanded=False):
            st.code(outline_text(graph_nodes), language=None)

        with st.expander("Tabela dos itens exibidos", expanded=False):
            graph_preview = pd.DataFrame(
                [
                    {
                        "Código": node.get("code", ""),
                        "Nível": len(str(node.get("code", "")).split(".")),
                        "Nome": node.get("name", ""),
                        "Responsável": node.get("owner", ""),
                        "Status": STATUS_LABELS.get(node.get("status"), node.get("status")),
                        "Entregável": node.get("deliverable", ""),
                    }
                    for node in graph_nodes
                ]
            )
            st.dataframe(
                graph_preview,
                use_container_width=True,
                hide_index=True,
                height=420,
            )



with tabs[1]:
    if not nodes:
        st.info("A WBS ainda não possui itens.")
    else:
        st.markdown("#### Visualização da tabela")
        st.caption(
            "Use os filtros e a paginação para navegar em WBS grandes. "
            "A edição ficou recolhida para priorizar a leitura."
        )

        frame = visual_table_frame(nodes)

        f1, f2, f3, f4 = st.columns([2.2, 1.6, 1.1, 1.1])

        query = f1.text_input(
            "Pesquisar",
            placeholder="Código, pacote, responsável, entregável, descrição...",
            key="wbs_table_search",
        )

        branches = [
            item
            for item in sorted(frame["Ramo principal"].dropna().unique().tolist())
            if item
        ]

        branch_filter = f2.multiselect(
            "Ramo principal",
            branches,
            key="wbs_table_branch",
        )

        statuses = sorted(frame["Status"].dropna().unique().tolist())
        status_filter = f3.multiselect(
            "Status",
            statuses,
            key="wbs_table_status",
        )

        levels = sorted(frame["Nível"].dropna().unique().tolist())
        level_filter = f4.multiselect(
            "Nível",
            levels,
            key="wbs_table_level",
        )

        filtered = frame.copy()

        if query:
            mask = filtered.astype(str).apply(
                lambda column: column.str.contains(query, case=False, na=False)
            ).any(axis=1)
            filtered = filtered[mask]

        if branch_filter:
            filtered = filtered[
                filtered["Ramo principal"].isin(branch_filter)
            ]

        if status_filter:
            filtered = filtered[filtered["Status"].isin(status_filter)]

        if level_filter:
            filtered = filtered[filtered["Nível"].isin(level_filter)]

        v1, v2, v3 = st.columns([1.4, 1, 1])

        column_mode = v1.selectbox(
            "Modo de colunas",
            ["Compacta", "Operacional", "Completa"],
            key="wbs_table_columns",
        )

        page_size = v2.selectbox(
            "Itens por página",
            [25, 50, 100, 250],
            index=1,
            key="wbs_table_page_size",
        )

        total_rows = len(filtered)
        total_pages = max(
            1,
            (total_rows + int(page_size) - 1) // int(page_size),
        )

        page_number = v3.number_input(
            "Página",
            min_value=1,
            max_value=total_pages,
            value=1,
            step=1,
            key="wbs_table_page",
        )

        start = (int(page_number) - 1) * int(page_size)
        end = start + int(page_size)
        visible = filtered.iloc[start:end].copy()

        if column_mode == "Compacta":
            display_columns = [
                "Estrutura",
                "Responsável",
                "Status",
                "Progresso (%)",
                "Entregável",
            ]
        elif column_mode == "Operacional":
            display_columns = [
                "Estrutura",
                "Ramo principal",
                "Nível",
                "Responsável",
                "Status",
                "Entregável",
                "Progresso (%)",
                "Custo",
                "Marco",
                "Tags",
            ]
        else:
            display_columns = [
                "Estrutura",
                "Ramo principal",
                "Nível",
                "Responsável",
                "Status",
                "Entregável",
                "Início",
                "Fim",
                "Duração (dias)",
                "Progresso (%)",
                "Custo",
                "Marco",
                "Tags",
                "Descrição",
            ]

        st.caption(
            f"Exibindo **{len(visible)}** de **{total_rows}** itens filtrados · "
            f"Página **{int(page_number)}/{total_pages}**."
        )

        display_frame = visible[display_columns].reset_index(drop=True)

        st.dataframe(
            style_readonly_table(display_frame),
            use_container_width=True,
            hide_index=True,
            height=650,
            column_config={
                "Estrutura": st.column_config.TextColumn(
                    "Estrutura",
                    width="large",
                ),
                "Ramo principal": st.column_config.TextColumn(
                    "Ramo principal",
                    width="medium",
                ),
                "Responsável": st.column_config.TextColumn(
                    "Responsável",
                    width="medium",
                ),
                "Entregável": st.column_config.TextColumn(
                    "Entregável",
                    width="large",
                ),
                "Descrição": st.column_config.TextColumn(
                    "Descrição",
                    width="large",
                ),
                "Progresso (%)": st.column_config.ProgressColumn(
                    "Progresso",
                    min_value=0,
                    max_value=100,
                    format="%.0f%%",
                ),
                "Custo": st.column_config.NumberColumn(
                    "Custo",
                    format="R$ %.2f",
                ),
            },
        )

        with st.expander("Edição tabular rápida", expanded=False):
            st.caption(
                "Edite nome, responsável, status, entregável, datas, progresso, "
                "custo e tags. A hierarquia é preservada."
            )

            editable_rows = []
            by_id = {node["id"]: node for node in nodes}

            for row in table_rows(nodes):
                original = by_id[row["ID"]]
                editable_rows.append(
                    {
                        "ID": row["ID"],
                        "Código": row["Código"],
                        "Código pai": row["Código pai"],
                        "Nome": row["Nome"],
                        "Responsável": row["Responsável"],
                        "Status": original["status"],
                        "Entregável": row["Entregável"],
                        "Início": row["Início"],
                        "Fim": row["Fim"],
                        "Duração (dias)": row["Duração (dias)"],
                        "Progresso (%)": row["Progresso (%)"],
                        "Custo": row["Custo"],
                        "Marco": original["milestone"],
                        "Tags": row["Tags"],
                    }
                )

            edited = st.data_editor(
                pd.DataFrame(editable_rows),
                use_container_width=True,
                hide_index=True,
                disabled=["ID", "Código", "Código pai"],
                column_config={
                    "Status": st.column_config.SelectboxColumn(
                        options=list(STATUS_OPTIONS)
                    ),
                    "Progresso (%)": st.column_config.NumberColumn(
                        min_value=0.0,
                        max_value=100.0,
                        step=5.0,
                    ),
                    "Custo": st.column_config.NumberColumn(
                        min_value=0.0,
                        step=100.0,
                    ),
                    "Marco": st.column_config.CheckboxColumn(),
                },
                key=(
                    f"wbs_table_editor_{selected_id}_"
                    f"{record.get('revision', 1)}"
                ),
            )

            if st.button(
                "Salvar alterações da tabela",
                type="primary",
                disabled=not can_edit,
            ):
                try:
                    updated_by_id = {
                        node["id"]: deepcopy(node)
                        for node in nodes
                    }

                    for item in edited.to_dict("records"):
                        target = updated_by_id.get(str(item.get("ID")))

                        if not target:
                            continue

                        target.update(
                            {
                                "name": str(item.get("Nome") or "").strip(),
                                "owner": str(item.get("Responsável") or "").strip(),
                                "status": str(item.get("Status") or "planned"),
                                "deliverable": str(item.get("Entregável") or "").strip(),
                                "start_date": str(item.get("Início") or "").strip(),
                                "due_date": str(item.get("Fim") or "").strip(),
                                "duration_days": item.get("Duração (dias)") or 0,
                                "progress_percent": item.get("Progresso (%)") or 0,
                                "cost": item.get("Custo") or 0,
                                "milestone": bool(item.get("Marco")),
                                "tags": str(item.get("Tags") or ""),
                            }
                        )

                    saved = save_wbs(
                        selected_id,
                        username,
                        name=record["name"],
                        description=record.get("description", ""),
                        project_id=record.get("project_id", ""),
                        visibility=record.get("visibility", "private"),
                        status=record.get("status", "draft"),
                        nodes=list(updated_by_id.values()),
                        expected_revision=record.get("revision"),
                        is_admin=is_admin,
                    )

                    st.success(
                        f"Tabela salva. Revisão {saved.get('revision')}."
                    )
                    st.rerun()

                except Exception as exc:
                    st.error(str(exc))



with tabs[2]:
    left, right = st.columns([1, 2])
    with left:
        st.markdown("#### Estrutura")
        choices = [node["id"] for node in nodes]
        if st.session_state.get("wbs_node_select", "") not in [""] + choices:
            st.session_state["wbs_node_select"] = ""
        selected_node_id = st.selectbox(
            "Pacote",
            [""] + choices,
            format_func=lambda value: "Novo pacote" if not value else next(f"{node['code']} · {node['name']}" for node in nodes if node["id"] == value),
            key="wbs_node_select",
        )
        add_root = st.button("Adicionar pacote raiz", use_container_width=True, disabled=not can_edit)
        add_child = st.button("Adicionar filho", use_container_width=True, disabled=not can_edit or not selected_node_id)
        if add_root or add_child:
            draft = nodes + [new_node("Novo pacote de trabalho", parent_id=selected_node_id if add_child else "", order=9999)]
            try:
                saved = save_wbs(
                    selected_id, username, name=record["name"], description=record.get("description", ""), project_id=record.get("project_id", ""),
                    visibility=record.get("visibility", "private"), status=record.get("status", "draft"), nodes=draft,
                    expected_revision=record.get("revision"), is_admin=is_admin,
                )
                st.success(f"Pacote criado. Revisão {saved.get('revision')}.")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))

    with right:
        if selected_node_id:
            node = next(node for node in nodes if node["id"] == selected_node_id)
            valid_parents = [item for item in nodes if item["id"] != selected_node_id and not item["code"].startswith(node["code"] + ".")]
            parent_options = [""] + [item["id"] for item in valid_parents]
            parent_index = parent_options.index(node.get("parent_id", "")) if node.get("parent_id", "") in parent_options else 0
            with st.form(f"wbs_node_form_{selected_node_id}"):
                st.markdown(f"#### {node['code']} · {node['name']}")
                c1, c2 = st.columns([2, 1])
                name = c1.text_input("Nome do pacote", value=node["name"])
                parent = c2.selectbox("Pacote pai", parent_options, index=parent_index, format_func=lambda value: "Raiz" if not value else next(f"{item['code']} · {item['name']}" for item in valid_parents if item["id"] == value))
                description = st.text_area("Descrição", value=node.get("description", ""), height=90)
                c3, c4 = st.columns(2)
                owner = c3.text_input("Responsável", value=node.get("owner", ""))
                status = c4.selectbox("Status", list(STATUS_OPTIONS), index=list(STATUS_OPTIONS).index(node.get("status", "planned")), format_func=lambda value: STATUS_LABELS[value])
                deliverable = st.text_input("Entregável", value=node.get("deliverable", ""))
                c5, c6, c7 = st.columns(3)
                start_date = c5.text_input("Data de início", value=node.get("start_date", ""), placeholder="AAAA-MM-DD")
                due_date = c6.text_input("Data final", value=node.get("due_date", ""), placeholder="AAAA-MM-DD")
                duration_days = c7.number_input("Duração (dias)", min_value=0.0, value=float(node.get("duration_days") or 0), step=1.0)
                c8, c9, c10 = st.columns(3)
                progress = c8.number_input("Progresso (%)", min_value=0.0, max_value=100.0, value=float(node.get("progress_percent") or 0), step=5.0)
                cost = c9.number_input("Custo", min_value=0.0, value=float(node.get("cost") or 0), step=100.0)
                order = c10.number_input("Ordem entre irmãos", min_value=1, value=int(node.get("order") or 1), step=1)
                milestone = st.checkbox("É um marco", value=bool(node.get("milestone")))
                tags = st.text_input("Tags", value=", ".join(node.get("tags") or []), placeholder="produto, entrega, crítico")
                save_node = st.form_submit_button("Salvar pacote", type="primary", disabled=not can_edit)
            if save_node:
                draft = deepcopy(nodes)
                target = next(item for item in draft if item["id"] == selected_node_id)
                target.update({"name": name, "parent_id": parent, "description": description, "owner": owner, "status": status, "deliverable": deliverable, "start_date": start_date, "due_date": due_date, "duration_days": duration_days, "progress_percent": progress, "cost": cost, "order": order, "milestone": milestone, "tags": tags})
                try:
                    saved = save_wbs(
                        selected_id, username, name=record["name"], description=record.get("description", ""), project_id=record.get("project_id", ""),
                        visibility=record.get("visibility", "private"), status=record.get("status", "draft"), nodes=draft,
                        expected_revision=record.get("revision"), is_admin=is_admin,
                    )
                    st.success(f"Pacote salvo. Revisão {saved.get('revision')}.")
                    st.rerun()
                except (WbsPermissionError, WbsRevisionConflict, ValueError) as exc:
                    st.error(str(exc))

            st.markdown("##### Excluir pacote")
            cascade = st.checkbox("Excluir também todos os descendentes", value=True, key="wbs_delete_cascade")
            if st.button("Excluir pacote selecionado", disabled=not can_edit):
                try:
                    draft = delete_node(nodes, selected_node_id, cascade=cascade)
                    save_wbs(
                        selected_id, username, name=record["name"], description=record.get("description", ""), project_id=record.get("project_id", ""),
                        visibility=record.get("visibility", "private"), status=record.get("status", "draft"), nodes=draft,
                        expected_revision=record.get("revision"), is_admin=is_admin,
                    )
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))
        else:
            st.info("Selecione um pacote à esquerda ou adicione um novo.")

with tabs[3]:
    st.markdown("#### Modelo para upload")
    render_import_template_downloads(f"wbs_template_{selected_id}")
    st.markdown("#### Importar")
    uploaded = st.file_uploader(
        "Formatos aceitos: JSON, XML, Microsoft Project XML, Excel XLS/XLSX, CSV, TSV, TXT, Markdown e PDF com texto extraível",
        type=["json", "xml", "xlsx", "xls", "csv", "tsv", "txt", "md", "pdf"],
        key=f"wbs_import_{selected_id}",
    )
    if uploaded:
        try:
            parsed = import_wbs(uploaded.name, uploaded.getvalue())
            for warning in parsed.get("warnings") or []:
                st.warning(warning)
            preview = pd.DataFrame(table_rows(parsed["nodes"]))
            st.success(f"{len(parsed['nodes'])} itens identificados em {uploaded.name}.")
            st.dataframe(preview.head(100), use_container_width=True, hide_index=True)
            import_mode = st.radio("Destino", ["Substituir estrutura da WBS atual", "Criar uma nova WBS"], horizontal=True)
            if st.button("Aplicar importação", type="primary", disabled=not can_edit and import_mode.startswith("Substituir")):
                if import_mode.startswith("Substituir"):
                    save_wbs(
                        selected_id, username, name=record["name"], description=record.get("description", ""), project_id=record.get("project_id", ""),
                        visibility=record.get("visibility", "private"), status=record.get("status", "draft"), nodes=parsed["nodes"],
                        expected_revision=record.get("revision"), is_admin=is_admin,
                    )
                    st.success("Estrutura importada para a WBS atual.")
                    st.rerun()
                else:
                    created = create_wbs(
                        parsed.get("name") or Path(uploaded.name).stem,
                        username,
                        description=parsed.get("description", ""),
                        project_id=record.get("project_id", ""),
                        nodes=parsed["nodes"],
                        is_admin=is_admin,
                    )
                    rerun_select(created["id"])
        except Exception as exc:
            st.error(f"Falha ao importar: {exc}")

    st.divider()
    st.markdown("#### Exportar")
    st.caption("Exporte a mesma WBS nos formatos necessários ou baixe o pacote ZIP com todos os formatos de uma vez.")
    export_formats = [
        ("JSON", "json"), ("XML", "xml"), ("Excel", "xlsx"), ("CSV", "csv"), ("TSV", "tsv"),
        ("PDF", "pdf"), ("Markdown", "md"), ("TXT", "txt"), ("Todos (ZIP)", "zip"),
    ]
    columns = st.columns(3)
    for index, (label, fmt) in enumerate(export_formats):
        try:
            data, mime, filename = export_wbs(record, fmt)
            columns[index % 3].download_button(
                f"Baixar {label}", data=data, file_name=filename, mime=mime,
                use_container_width=True, key=f"wbs_download_{selected_id}_{fmt}",
            )
        except Exception as exc:
            columns[index % 3].error(f"{label}: {exc}")

with tabs[4]:
    if not nodes:
        st.info("Sem dados para indicadores.")
    else:
        rollup = pd.DataFrame(top_level_rollup(nodes))
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### Custo por pacote de nível 1")
            if not rollup.empty:
                st.bar_chart(rollup.set_index("Pacote")["Custo"])
        with c2:
            st.markdown("#### Progresso por pacote de nível 1")
            if not rollup.empty:
                st.bar_chart(rollup.set_index("Pacote")["Progresso (%)"])
        st.markdown("#### Status dos itens")
        status_frame = pd.DataFrame([{"Status": STATUS_LABELS.get(node["status"], node["status"]), "Itens": 1} for node in nodes]).groupby("Status", as_index=False)["Itens"].sum()
        st.bar_chart(status_frame.set_index("Status")["Itens"])
        st.markdown("#### Consolidação nível 1")
        st.dataframe(rollup, use_container_width=True, hide_index=True)

with tabs[5]:
    with st.form("wbs_settings"):
        name = st.text_input("Nome", value=record["name"])
        description = st.text_area("Descrição", value=record.get("description", ""), height=110)
        c1, c2, c3 = st.columns(3)
        project_id = c1.selectbox("Projeto", project_options, index=project_options.index(record.get("project_id", "")) if record.get("project_id", "") in project_options else 0, format_func=project_label)
        visibility = c2.selectbox("Visibilidade", ["private", "organization"], index=0 if record.get("visibility") == "private" else 1, format_func=lambda value: "Privada" if value == "private" else "Organização")
        statuses = ["draft", "in_review", "published", "archived"]
        status = c3.selectbox("Status", statuses, index=statuses.index(record.get("status", "draft")))
        save_settings = st.form_submit_button("Salvar configurações", type="primary", disabled=not can_edit)
    if save_settings:
        try:
            saved = save_wbs(
                selected_id, username, name=name, description=description, project_id=project_id, visibility=visibility, status=status,
                nodes=nodes, expected_revision=record.get("revision"), is_admin=is_admin,
            )
            st.success(f"Configurações salvas. Revisão {saved.get('revision')}.")
            st.rerun()
        except Exception as exc:
            st.error(str(exc))

    st.markdown("#### Recursos desta página")
    st.markdown(
        """
- CRUD completo de WBS e pacotes de trabalho.
- Vinculação opcional a projetos do Produto Tools.
- Visualização gráfica hierárquica, outline textual, tabela pesquisável e indicadores.
- Edição individual ou tabular.
- Modelo de importação para download em Excel e CSV, pronto para preenchimento e reupload.
- Importação: JSON, XML nativo, Microsoft Project XML, XLS/XLSX, CSV, TSV, TXT, Markdown e PDF textual.
- Exportação: JSON, XML, Excel, CSV, TSV, PDF, Markdown, TXT e pacote ZIP completo.
- Controle de revisão otimista e trilha de auditoria no MongoDB.
        """
    )
