import json
from pathlib import Path

from schemas.flowchart_schema import demo_flowchart_document, normalize_document
from services.flow_analytics import analyze_document, build_raci_rows


def test_demo_analytics():
    document = normalize_document(demo_flowchart_document("tester"), "tester")
    result = analyze_document(document)
    assert result["counts"]["nodes"] == 6
    assert result["counts"]["decisions"] == 1
    assert 0 <= result["quality_score"] <= 100
    assert len(build_raci_rows(document)) == 6


def test_large_sigyo_regression_file():
    path = Path(__file__).resolve().parents[1] / "examples" / "fluxo_sigyo_modular_completo.json"
    document = normalize_document(json.loads(path.read_text(encoding="utf-8")), "tester")
    result = analyze_document(document)
    assert result["counts"]["nodes"] >= 100
    assert result["counts"]["lanes"] >= 10
    assert result["counts"]["decisions"] >= 10
