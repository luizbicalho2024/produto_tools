from __future__ import annotations

import json
from pathlib import Path

from schemas.flowchart_schema import normalize_document


ROOT = Path(__file__).resolve().parents[1]
SIGYO_PATH = ROOT / "examples" / "fluxo_sigyo_modular_completo_revisado.json"
MAIN_JS = ROOT / "components" / "flow_editor" / "frontend" / "main.js"
EDITOR_CSS = ROOT / "components" / "flow_editor" / "frontend" / "styles.css"
GLOBAL_STYLES = ROOT / "core" / "styles.py"


def test_revised_sigyo_normalization_and_decisions():
    raw = json.loads(SIGYO_PATH.read_text(encoding="utf-8"))
    document = normalize_document(raw, "tester")

    assert len(document["lanes"]) == 12
    assert len(document["nodes"]) == 153
    assert len(document["edges"]) == 203
    assert document["settings"]["layoutPreset"] == "compact"
    assert document["settings"]["edgeRouting"] == "corridor"
    assert document["settings"]["autosaveSeconds"] == 10

    decisions = {node["id"] for node in document["nodes"] if node["type"] == "decision"}
    outgoing = {node_id: 0 for node_id in decisions}
    for edge in document["edges"]:
        if edge.get("enabled", True) and edge.get("source") in outgoing:
            outgoing[edge["source"]] += 1
    assert decisions
    assert min(outgoing.values()) >= 2


def test_autosave_is_local_and_mongo_sync_is_explicit():
    source = MAIN_JS.read_text(encoding="utf-8")
    assert "function writeLocalDraft" in source
    assert "function persistLocalDraft" in source
    assert "function syncDraftToMongo" in source
    assert source.count('setTriggerValue("autosave"') == 1
    assert 'action === "sync-draft"' in source


def test_filter_layout_routing_and_dark_mode_guards_are_present():
    source = MAIN_JS.read_text(encoding="utf-8")
    css = EDITOR_CSS.read_text(encoding="utf-8")
    global_styles = GLOBAL_STYLES.read_text(encoding="utf-8")

    assert "function nodeMatchesView" in source
    assert "function visibleNodeIds" in source
    assert "function layoutDocumentInPlace" in source
    assert "function findVerticalCorridor" in source
    assert 'state.doc.settings.edgeRouting = "corridor-v2"' in source
    assert ".flow-node.view-hidden" in css
    assert '.flow-editor-shell[data-theme="dark"]' in css
    assert 'div[data-baseweb="popover"]' in global_styles
