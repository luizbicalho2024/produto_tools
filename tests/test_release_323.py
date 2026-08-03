from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAIN_JS = ROOT / "components" / "flow_editor" / "frontend" / "main.js"
INDEX_HTML = ROOT / "components" / "flow_editor" / "frontend" / "index.html"
EDITOR_CSS = ROOT / "components" / "flow_editor" / "frontend" / "styles.css"
GLOBAL_STYLES = ROOT / "core" / "styles.py"
MAP_PAGE = ROOT / "pages" / "4_Mapa_de_Relacoes.py"
CONFIG = ROOT / "core" / "configuration.py"


def test_release_version_and_component_features():
    assert 'APP_VERSION = "3.2.3"' in CONFIG.read_text(encoding="utf-8")
    source = MAIN_JS.read_text(encoding="utf-8")
    for token in (
        "autoFitLanes",
        "fitLanesToContent",
        "growLaneForDesiredY",
        "selectedNodeIds",
        "renderMultiSelectionProperties",
        "decisionEdgeSemantic",
        'event.button === 2',
        "navigationClickGuard",
        "syncDraftToMongo({ navigation: true })",
        "hostDocument",
    ):
        assert token in source


def test_editor_markup_and_styles_cover_requested_interactions():
    html = INDEX_HTML.read_text(encoding="utf-8")
    css = EDITOR_CSS.read_text(encoding="utf-8")
    for token in ("Raias automáticas", "navigation-warning", "nav-save"):
        assert token in html
    for token in ("multi-selected", "edge-positive", "edge-negative", "is-panning"):
        assert token in css


def test_sidebar_dark_mode_controls_are_explicitly_styled():
    styles = GLOBAL_STYLES.read_text(encoding="utf-8")
    for token in (
        'stTextInputRootElement',
        'stSidebarUserContent',
        'aria-haspopup="listbox"',
        'input:disabled',
        'var(--pt-input)',
    ):
        assert token in styles


def test_relation_map_keeps_decision_semantics_and_direction():
    source = MAP_PAGE.read_text(encoding="utf-8")
    for token in (
        "decision_edge_semantic",
        '"semantic": decision_edge_semantic',
        "semanticColor",
        "drawArrow",
        "Verde = Sim/positivo",
    ):
        assert token in source
