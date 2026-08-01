from __future__ import annotations

import json
import zipfile
from pathlib import Path


def test_sigyo_project_package_has_valid_cross_flow_links():
    package = Path(__file__).resolve().parents[1] / "examples" / "sigyo_modular_project.zip"
    assert package.exists()
    with zipfile.ZipFile(package) as archive:
        assert archive.testzip() is None
        manifest = json.loads(archive.read("project.json").decode("utf-8"))
        documents = {}
        for item in manifest["flows"]:
            document = json.loads(archive.read(item["file"]).decode("utf-8"))
            documents[document["flow"]["id"]] = document

    assert len(documents) == 7
    assert manifest["project"]["defaultFlowId"] in documents

    links = []
    broken = []
    for source_id, document in documents.items():
        for node in document.get("nodes", []):
            data = node.get("data") or {}
            target_id = data.get("linkedFlowId")
            if not target_id:
                continue
            links.append((source_id, node["id"], target_id))
            if target_id not in documents:
                broken.append((source_id, node["id"], "missing_flow"))
                continue
            target_nodes = {item["id"] for item in documents[target_id].get("nodes", [])}
            entry = data.get("linkedFlowEntryNodeId")
            exit_node = data.get("linkedFlowExitNodeId")
            if entry and entry not in target_nodes:
                broken.append((source_id, node["id"], "missing_entry"))
            if exit_node and exit_node not in target_nodes:
                broken.append((source_id, node["id"], "missing_exit"))

    assert len(links) == 13
    assert broken == []
    assert sum(len(item.get("nodes", [])) for item in documents.values()) == 297
    assert sum(len(item.get("edges", [])) for item in documents.values()) == 372
