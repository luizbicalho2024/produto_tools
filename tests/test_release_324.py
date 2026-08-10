from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

from core.configuration import APP_VERSION
from schemas.flowchart_schema import demo_flowchart_document
from services.report_export import export_bundle, flow_only_pdf, full_documentation_pdf


ROOT = Path(__file__).resolve().parents[1]


def test_release_324_version_and_download_ui():
    assert tuple(map(int, APP_VERSION.split("."))) >= (3, 2, 4)
    editor = (ROOT / "pages" / "5_Editor_de_Fluxos.py").read_text(encoding="utf-8")
    index = (ROOT / "components" / "flow_editor" / "frontend" / "index.html").read_text(encoding="utf-8")
    service = (ROOT / "services" / "report_export.py").read_text(encoding="utf-8")

    assert "Downloads do fluxo" in editor
    assert "PDF - somente fluxo" in editor
    assert "PDF - documentação completa" in editor
    assert "Baixar pacote completo (.zip)" in editor
    assert "Exportar visual" in index
    assert "def flow_only_pdf" in service
    assert "def full_documentation_pdf" in service
    assert "def export_bundle" in service


def test_release_324_pdf_exports_and_bundle_are_valid():
    document = demo_flowchart_document("teste")
    metadata = {
        "current_version": 3,
        "revision": 7,
        "workflow_status": "draft",
        "owner_username": "teste",
    }

    diagram = flow_only_pdf(document, metadata)
    complete = full_documentation_pdf(document, metadata)
    bundle = export_bundle(document, metadata)

    assert diagram.startswith(b"%PDF-")
    assert complete.startswith(b"%PDF-")
    assert len(diagram) > 1000
    assert len(complete) > 3000

    with zipfile.ZipFile(io.BytesIO(bundle)) as archive:
        names = archive.namelist()
        assert any(name.endswith("_fluxo.pdf") for name in names)
        assert any(name.endswith("_documentacao_completa.pdf") for name in names)
        assert any(name.endswith(".json") for name in names)
        assert any(name.endswith("_relatorio.html") for name in names)
        assert any(name.endswith("_etapas.csv") for name in names)
        assert any(name.endswith("_raci.csv") for name in names)
        assert archive.testzip() is None
