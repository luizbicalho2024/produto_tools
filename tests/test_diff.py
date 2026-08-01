from copy import deepcopy

from schemas.flowchart_schema import demo_flowchart_document, normalize_document
from services.flow_diff import compare_documents


def test_diff_detects_add_remove_modify():
    before = normalize_document(demo_flowchart_document("tester"), "tester")
    after = deepcopy(before)
    after["nodes"][0]["data"]["label"] = "Início alterado"
    after["nodes"].pop()
    after["nodes"].append({"id": "new", "type": "task", "laneId": "lane_comercial", "position": {"x": 10, "y": 10}, "data": {"label": "Novo", "enabled": True}})
    result = compare_documents(before, after)
    assert result["summary"]["nodes_added"] == 1
    assert result["summary"]["nodes_removed"] == 1
    assert result["summary"]["nodes_modified"] == 1
