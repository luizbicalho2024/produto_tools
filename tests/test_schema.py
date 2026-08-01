from schemas.flowchart_schema import new_flowchart_document, normalize_document, validate_document


def test_new_document_is_valid_empty_draft():
    document = new_flowchart_document("Teste", "tester")
    # Fluxos vazios são permitidos como rascunho inicial.
    assert document["flow"]["name"] == "Teste"
    assert document["schemaVersion"] == "2.0.0"


def test_decision_requires_two_named_outputs():
    document = new_flowchart_document("Decisão", "tester")
    document["nodes"] = [
        {"id": "start", "type": "start", "laneId": "lane_process", "position": {"x": 0, "y": 50}, "data": {"label": "Início", "enabled": True}},
        {"id": "decision", "type": "decision", "laneId": "lane_process", "position": {"x": 200, "y": 50}, "data": {"label": "Aprovar?", "enabled": True}},
        {"id": "end", "type": "end", "laneId": "lane_process", "position": {"x": 400, "y": 50}, "data": {"label": "Fim", "enabled": True}},
    ]
    document["edges"] = [
        {"id": "e1", "source": "start", "target": "decision", "enabled": True},
        {"id": "e2", "source": "decision", "target": "end", "enabled": True, "label": "Sim"},
    ]
    document = normalize_document(document, "tester")
    errors = validate_document(document)
    assert any("no mínimo duas" in error for error in errors)


def test_legacy_document_is_upgraded():
    legacy = {"flow": {"id": "f", "name": "Legado", "status": "active"}, "nodes": [], "edges": [], "lanes": []}
    normalized = normalize_document(legacy, "tester")
    assert normalized["schemaVersion"] == "2.0.0"
    assert normalized["flow"]["status"] == "published"
    assert normalized["settings"]["interactivePlayback"] is True
