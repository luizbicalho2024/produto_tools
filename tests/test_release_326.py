from __future__ import annotations

from pathlib import Path

from core.configuration import APP_VERSION

ROOT = Path(__file__).resolve().parents[1]
MAIN_JS = ROOT / "components" / "flow_editor" / "frontend" / "main.js"
INDEX_HTML = ROOT / "components" / "flow_editor" / "frontend" / "index.html"
COMPONENT = ROOT / "components" / "flow_editor" / "component.py"
EDITOR_PAGE = ROOT / "pages" / "5_Editor_de_Fluxos.py"


def test_release_326_version_and_autosave_removed():
    assert tuple(int(part) for part in APP_VERSION.split(".")) >= (3, 2, 6)
    js = MAIN_JS.read_text(encoding="utf-8")
    component = COMPONENT.read_text(encoding="utf-8")
    page = EDITOR_PAGE.read_text(encoding="utf-8")

    assert "scheduleAutosave" not in js
    assert "persistLocalDraft" not in js
    assert "writeLocalDraft" not in js
    assert 'setTriggerValue("autosave"' not in js
    assert "autosave_seconds" not in component
    assert "on_autosave_change" not in component
    assert 'getattr(result, "autosave"' not in page
    assert 'setTriggerValue("draft_save"' in js
    assert 'getattr(result, "draft_save"' in page


def test_release_326_rerun_protection_is_memory_only():
    js = MAIN_JS.read_text(encoding="utf-8")
    assert "FLOW_EDITOR_RUNTIME_CACHE" in js
    assert "rememberWorkingSession" in js
    assert "restoredWorkingSession" in js
    assert "sessionEpoch" in js
    assert "produto_tools_draft:" in js  # apenas limpeza de chaves antigas
    assert "localStorage.setItem(draftStorageKey" not in js


def test_release_326_keyboard_editing_cannot_delete_canvas_items():
    js = MAIN_JS.read_text(encoding="utf-8")
    assert "function isEditingText(event)" in js
    assert 'if (isEditingText(event)) return;' in js
    assert '["INPUT", "TEXTAREA", "SELECT", "OPTION"]' in js
    assert 'candidate.isContentEditable' in js
    assert 'data-editor-control' in js
    assert "protectEditorControl" in js
    assert 'event.key === "Delete" || event.key === "Backspace"' in js


def test_release_326_fields_are_live_without_rebuilding_inspector():
    js = MAIN_JS.read_text(encoding="utf-8")
    assert "isLiveTextControl" in js
    assert 'control.addEventListener("input"' in js
    assert "scheduleCanvasRender" in js
    assert "renderCanvasOnly" in js
    assert 'propertiesBody.querySelector(\'[data-field="lane-name"]\')' in js


def test_release_326_opening_flow_does_not_auto_reorganize_document():
    js = MAIN_JS.read_text(encoding="utf-8")
    tail = js[js.index("  palette();") :]
    assert "initialLayoutProblems" not in tail
    assert "repairedLargeLayout" not in tail
    assert "automaticLaneAdjustment" not in tail
    assert "Mudanças de layout só acontecem após uma ação explícita" in tail


def test_release_326_lane_management_is_less_destructive():
    js = MAIN_JS.read_text(encoding="utf-8")
    assert "resolveMovedNodeOverlaps" in js
    assert "resizeLaneKeepingRelativePositions" in js
    assert 'if (dragState.kind === "node-group") resolveMovedNodeOverlaps(dragState.ids);' in js
    assert "fitLanesToContent({ repack: false, shrink: true })" in js


def test_release_326_ui_labels_manual_draft():
    html = INDEX_HTML.read_text(encoding="utf-8")
    assert 'data-action="save-draft"' in html
    assert "Salvar rascunho" in html
    assert "Backspace/Delete funcionam normalmente" in html
    assert "Sem alterações pendentes" in html
