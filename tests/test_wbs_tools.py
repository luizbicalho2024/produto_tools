from __future__ import annotations

import pytest

from services.wbs_tools import (
    delete_node,
    export_import_template,
    export_wbs,
    graphviz_dot,
    import_wbs,
    normalize_nodes,
    summary_metrics,
    table_rows,
    validate_wbs,
)


def sample_nodes():
    return [
        {"id": "root", "name": "Projeto", "order": 1, "cost": 1000, "progress_percent": 25},
        {"id": "design", "parent_id": "root", "name": "Design", "order": 1, "cost": 300, "progress_percent": 50},
        {"id": "build", "parent_id": "root", "name": "Construção", "order": 2, "cost": 700, "progress_percent": 10},
        {"id": "api", "parent_id": "build", "name": "API", "order": 1, "cost": 400, "progress_percent": 20},
    ]


def test_normalize_codes_depth_and_metrics():
    nodes = normalize_nodes(sample_nodes())
    assert [node["code"] for node in nodes] == ["1", "1.1", "1.2", "1.2.1"]
    rows = table_rows(nodes)
    assert rows[-1]["Nível"] == 3
    metrics = summary_metrics(nodes)
    assert metrics["packages"] == 4
    assert metrics["levels"] == 3
    assert metrics["total_cost"] == pytest.approx(2400.0)


def test_validate_detects_cycle():
    nodes = [
        {"id": "a", "parent_id": "b", "name": "A"},
        {"id": "b", "parent_id": "a", "name": "B"},
    ]
    errors = validate_wbs(nodes)
    assert any("ciclo" in item.lower() for item in errors)
    with pytest.raises(ValueError):
        normalize_nodes(nodes)


def test_delete_node_cascade_and_reparent():
    nodes = normalize_nodes(sample_nodes())
    cascaded = delete_node(nodes, "build", cascade=True)
    assert {node["id"] for node in cascaded} == {"root", "design"}
    reparented = delete_node(nodes, "build", cascade=False)
    api = next(node for node in reparented if node["id"] == "api")
    assert api["parent_id"] == "root"


def test_json_xml_and_csv_roundtrip():
    record = {"id": "wbs_test", "name": "WBS Teste", "description": "Teste", "nodes": sample_nodes()}
    for fmt in ("json", "xml", "csv", "tsv", "txt", "md"):
        data, mime, filename = export_wbs(record, fmt)
        assert data
        assert mime
        assert filename.endswith("." + fmt)
        imported = import_wbs(filename, data)
        assert imported["nodes"]


def test_excel_roundtrip():
    pytest.importorskip("openpyxl")
    record = {"id": "wbs_test", "name": "WBS Teste", "nodes": sample_nodes()}
    data, _, filename = export_wbs(record, "xlsx")
    imported = import_wbs(filename, data)
    assert len(imported["nodes"]) == 4
    assert imported["nodes"][-1]["code"] == "1.2.1"


def test_pdf_and_zip_exports():
    record = {"id": "wbs_test", "name": "WBS Teste", "nodes": sample_nodes()}
    pdf, mime, filename = export_wbs(record, "pdf")
    assert pdf.startswith(b"%PDF")
    assert mime == "application/pdf"
    assert filename.endswith(".pdf")
    package, package_mime, package_name = export_wbs(record, "zip")
    assert package.startswith(b"PK")
    assert package_mime == "application/zip"
    assert package_name.endswith(".zip")


def test_import_templates_are_reimportable():
    pytest.importorskip("openpyxl")

    xlsx, xlsx_mime, xlsx_name = export_import_template("xlsx")
    assert xlsx.startswith(b"PK")
    assert xlsx_name == "modelo_importacao_wbs.xlsx"
    assert "spreadsheetml" in xlsx_mime

    imported_xlsx = import_wbs(xlsx_name, xlsx)
    assert [node["code"] for node in imported_xlsx["nodes"]] == [
        "1",
        "1.1",
        "1.1.1",
        "1.2",
    ]

    csv_data, csv_mime, csv_name = export_import_template("csv")
    assert csv_name == "modelo_importacao_wbs.csv"
    assert csv_mime == "text/csv"

    imported_csv = import_wbs(csv_name, csv_data)
    assert len(imported_csv["nodes"]) == 4
    assert imported_csv["nodes"][2]["name"] == "Levantamento de requisitos"


def test_graphviz_contains_hierarchy():
    source = graphviz_dot(sample_nodes())
    assert "digraph WBS" in source
    assert '"root" -> "design"' in source
    assert "Construção" in source
