from pathlib import Path

from services.flow_analytics import analyze_document, issue_detail_rows


ROOT = Path(__file__).resolve().parents[1]


def test_editor_has_direction_indicators_and_download_menu():
    index = (ROOT / "components/flow_editor/frontend/index.html").read_text(encoding="utf-8")
    main = (ROOT / "components/flow_editor/frontend/main.js").read_text(encoding="utf-8")
    styles = (ROOT / "components/flow_editor/frontend/styles.css").read_text(encoding="utf-8")

    assert 'data-role="download-menu"' in index
    assert "node-flow-indicator incoming" in main
    assert "node-flow-indicator outgoing" in main
    assert "endpointGap = 12" in main
    assert "edge-source-terminal" in main
    assert ".node-flow-indicator" in styles


def test_map_has_fullscreen_and_highlight_filters():
    page = (ROOT / "pages/4_Mapa_de_Relacoes.py").read_text(encoding="utf-8")
    assert 'id="fullscreen"' in page
    assert 'id="typeFilter"' in page
    assert 'id="flowFilter"' in page
    assert 'id="isolate"' in page
    assert "requestFullscreen" in page


def test_human_readable_issue_details_include_card_name():
    document = {
        "lanes": [{"id": "lane", "name": "Operação"}],
        "nodes": [
            {
                "id": "start",
                "type": "start",
                "laneId": "lane",
                "data": {"label": "Início", "description": "Começa", "owner": "Equipe", "enabled": True},
            },
            {
                "id": "decision",
                "type": "decision",
                "laneId": "lane",
                "data": {
                    "label": "Cliente aprovou?",
                    "description": "Valida aceite",
                    "owner": "Comercial",
                    "criticality": "high",
                    "enabled": True,
                },
            },
        ],
        "edges": [{"id": "e1", "source": "start", "target": "decision", "enabled": True}],
    }
    analysis = analyze_document(document)
    details = issue_detail_rows(document, analysis)
    assert any(row["Card"] == "Cliente aprovou?" for row in details)
    assert any(row["Problema"] == "Decisão incompleta" for row in details)
    assert all(row["Como corrigir"] for row in details)


def test_quality_pages_show_exact_cards_and_actions():
    projects = (ROOT / "pages/3_Gestao_de_Projetos.py").read_text(encoding="utf-8")
    central = (ROOT / "pages/2_Central_de_Processos.py").read_text(encoding="utf-8")
    editor = (ROOT / "pages/5_Editor_de_Fluxos.py").read_text(encoding="utf-8")
    assert "Cards com problema" in projects
    assert "Como corrigir" in projects
    assert "Cards com problema" in central
    assert "issue_detail_rows" in editor
    assert "st.popover(\"Baixar relatórios\"" in editor
