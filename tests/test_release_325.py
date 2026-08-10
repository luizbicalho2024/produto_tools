from __future__ import annotations

import re
from copy import deepcopy
from pathlib import Path

from core.configuration import APP_VERSION
from schemas.flowchart_schema import demo_flowchart_document
from services.report_export import flow_only_pdf

ROOT = Path(__file__).resolve().parents[1]
MAIN_JS = ROOT / "components" / "flow_editor" / "frontend" / "main.js"
EDITOR_CSS = ROOT / "components" / "flow_editor" / "frontend" / "styles.css"
REPORT = ROOT / "services" / "report_export.py"


def test_release_325_version_and_rerun_resilience():
    assert APP_VERSION == "3.2.5"
    source = MAIN_JS.read_text(encoding="utf-8")

    assert "function protectCurrentDocumentLocally" in source
    assert "const restoredLocalDraft = Boolean(localDocument && localDiffersFromDatabase);" in source
    assert "const protectedDraft = protectCurrentDocumentLocally();" in source
    assert "pendentes no banco" in source
    assert "localSavedAt >= incomingUpdatedAt" not in source


def test_release_325_criticality_badge_is_outside_text_area():
    css = EDITOR_CSS.read_text(encoding="utf-8")
    block_start = css.index(".flow-node.critical::after")
    block = css[block_start : block_start + 520]
    assert "left: 50%" in block
    assert "top: -13px" in block
    assert "translateX(-50%)" in block
    assert "right: 8px" not in block


def test_release_325_flow_pdf_contains_card_messages_and_dense_detail_pages():
    source = REPORT.read_text(encoding="utf-8")
    assert "_append_dense_flow_detail_pages" in source
    assert "description_lines = _wrap_text_lines" in source
    assert "mensagens/descrições dos cards" in source

    document = demo_flowchart_document("tester")
    base_node = document["nodes"][0]
    lane_id = document["lanes"][0]["id"] if document.get("lanes") else None
    document["nodes"] = []
    document["edges"] = []
    for index in range(72):
        node = deepcopy(base_node)
        node["id"] = f"node_{index}"
        node["laneId"] = lane_id
        node["position"] = {"x": 100 + (index % 18) * 220, "y": 80 + (index // 18) * 90}
        node.setdefault("data", {})["label"] = f"Etapa detalhada {index}"
        node["data"]["description"] = f"Mensagem completa do card {index} para validar a exportação legível."
        document["nodes"].append(node)

    pdf = flow_only_pdf(document, {"current_version": 1, "revision": 1})
    assert pdf.startswith(b"%PDF-")
    # Visão geral + páginas adicionais de detalhe para fluxos densos.
    assert len(re.findall(rb"/Type\s*/Page\b", pdf)) >= 2
