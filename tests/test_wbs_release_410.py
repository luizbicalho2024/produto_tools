from __future__ import annotations

from pathlib import Path

from core.configuration import APP_VERSION, WBS_COLLECTION

ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / "pages" / "16_WBS.py"
TOOLS = ROOT / "services" / "wbs_tools.py"
REPOSITORY = ROOT / "services" / "wbs_repository.py"


def test_wbs_release_configuration():
    assert tuple(int(part) for part in APP_VERSION.split(".")) >= (4, 1, 0)
    assert WBS_COLLECTION == "produto_tools_wbs"


def test_wbs_page_exposes_crud_views_and_formats():
    source = PAGE.read_text(encoding="utf-8")
    for token in (
        "graphviz_chart",
        "data_editor",
        "create_wbs",
        "delete_wbs",
        "duplicate_wbs",
        "import_wbs",
        "export_wbs",
        '"json", "xml", "xlsx", "xls", "csv", "tsv", "txt", "md", "pdf"',
        "Todos (ZIP)",
    ):
        assert token in source


def test_wbs_services_keep_hierarchy_and_concurrency_controls():
    tools = TOOLS.read_text(encoding="utf-8")
    repository = REPOSITORY.read_text(encoding="utf-8")
    for token in ("recalculate_codes", "validate_wbs", "Microsoft Project", "import_pdf", "export_pdf"):
        assert token in tools
    for token in ("WbsRevisionConflict", '"revision": current_revision + 1', "matched_count == 0"):
        assert token in repository
