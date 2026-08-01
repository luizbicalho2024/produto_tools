from __future__ import annotations

import json
from pathlib import Path

from schemas.flowchart_schema import repair_import_document, validate_document

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_ROOT = ROOT / "examples" / "sigyo_modular_project" / "flows"


def load_example(name: str) -> dict:
    return json.loads((EXAMPLE_ROOT / name).read_text(encoding="utf-8"))


def test_single_output_decisions_are_repaired_without_inventing_branches():
    document = load_example("flow_sigyo_aux_assinatura_onboarding.json")
    for node in document["nodes"]:
        if node["id"] in {"s_entity", "s_commit"}:
            node["type"] = "decision"
            node.get("data", {}).pop("importRepair", None)
    repaired, warnings = repair_import_document(document, "tester")
    nodes = {node["id"]: node for node in repaired["nodes"]}

    assert nodes["s_entity"]["type"] == "task"
    assert nodes["s_commit"]["type"] == "task"
    assert nodes["s_entity"]["data"]["importRepair"] == "decision_without_branches_converted_to_task"
    assert len(warnings) >= 2
    assert validate_document(repaired, strict=False) == []


def test_all_sigyo_project_flows_are_importable_after_safe_repair():
    for path in sorted(EXAMPLE_ROOT.glob("*.json")):
        repaired, _warnings = repair_import_document(json.loads(path.read_text(encoding="utf-8")), "tester")
        assert validate_document(repaired, strict=False) == [], path.name


def test_ascii_pages_and_valid_streamlit_navigation_paths_exist():
    pages = {
        "pages/1_Gestao_de_Acesso.py",
        "pages/2_Central_de_Processos.py",
        "pages/3_Gestao_de_Projetos.py",
        "pages/4_Mapa_de_Relacoes.py",
        "pages/5_Editor_de_Fluxos.py",
    }
    for relative in pages:
        assert (ROOT / relative).exists(), relative

    for source_path in [ROOT / "login_app.py", *sorted((ROOT / "pages").glob("*.py")), ROOT / "core" / "auth.py"]:
        source = source_path.read_text(encoding="utf-8")
        for relative in pages | {"login_app.py"}:
            if f'st.switch_page("{relative}")' in source:
                assert (ROOT / relative).exists(), f"{source_path.name}: {relative}"


def test_global_edge_routing_and_obsidian_graph_are_present():
    main_js = (ROOT / "components" / "flow_editor" / "frontend" / "main.js").read_text(encoding="utf-8")
    index_html = (ROOT / "components" / "flow_editor" / "frontend" / "index.html").read_text(encoding="utf-8")
    graph_page = (ROOT / "pages" / "4_Mapa_de_Relacoes.py").read_text(encoding="utf-8")

    assert 'edgeRouting: "smooth"' in main_js
    assert "edge-routing-global" in index_html
    assert "Curvas suaves" in index_html
    assert "Linhas retas" in index_html
    assert "new ResizeObserver" in graph_page
    assert "Explodir" in graph_page
    assert "requestAnimationFrame" in graph_page


def test_login_has_no_page_link_and_theme_preference_is_persisted():
    login = (ROOT / "login_app.py").read_text(encoding="utf-8")
    styles = (ROOT / "core" / "styles.py").read_text(encoding="utf-8")
    database = (ROOT / "database.py").read_text(encoding="utf-8")

    assert "st.page_link" not in login
    assert 'st.switch_page("pages/2_Central_de_Processos.py")' in login
    assert "set_user_ui_theme" in database
    assert "st.query_params" in styles
    assert "ui_theme" in styles
