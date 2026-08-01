from __future__ import annotations

from copy import deepcopy

import pytest

mongomock = pytest.importorskip("mongomock")

from schemas.flowchart_schema import demo_flowchart_document
from services import flowchart_repository as flow_repository
from services import project_repository as project_repository


def configure(monkeypatch):
    client = mongomock.MongoClient(tz_aware=True)
    database = client["simulador_db"]
    for module in (flow_repository, project_repository):
        monkeypatch.setattr(module.db, "initialize_database", lambda: True)
        monkeypatch.setattr(module.db, "get_collection", lambda name, db=database: db[name])
        monkeypatch.setattr(module.db, "add_log", lambda *args, **kwargs: True)
    return database


def create_linked_flows():
    parent = demo_flowchart_document("owner")
    parent["flow"]["id"] = "flow_parent"
    parent["flow"]["name"] = "Fluxo principal"
    child = demo_flowchart_document("owner")
    child["flow"]["id"] = "flow_child"
    child["flow"]["name"] = "Fluxo auxiliar"
    subprocess = next(node for node in parent["nodes"] if node["type"] == "subprocess")
    subprocess["data"]["linkedFlowId"] = "flow_child"
    subprocess["data"]["linkedFlowEntryNodeId"] = "node_start"
    subprocess["data"]["linkedFlowExitNodeId"] = "node_end"
    return parent, child


def test_project_links_search_release_and_bundle(monkeypatch):
    database = configure(monkeypatch)
    project = project_repository.create_project("SIGYO", "Projeto modular", "owner", "owner@example.com")
    parent, child = create_linked_flows()
    for order, (doc, role) in enumerate(((parent, "executive"), (child, "subprocess")), start=1):
        doc["flow"].update({
            "projectId": project["id"],
            "projectRole": role,
            "projectGroup": "Teste",
            "projectOrder": order,
        })
        flow_repository.save_flowchart(doc, "owner", actor_username="owner")
    project_repository.update_project(project["id"], "owner", default_flow_id="flow_parent")

    flows = project_repository.list_project_flows(project["id"], "owner")
    assert [item["id"] for item in flows] == ["flow_parent", "flow_child"]

    graph = project_repository.project_links(project["id"], "owner")
    assert len(graph["links"]) == 1
    assert graph["broken"] == []

    analysis = project_repository.analyze_project(project["id"], "owner")
    assert analysis["flow_count"] == 2
    assert analysis["link_count"] == 1
    assert analysis["broken_count"] == 0

    path = project_repository.shortest_project_path(project["id"], "owner", "flow_parent", "flow_child")
    assert path == ["flow_parent", "flow_child"]

    results = project_repository.search_project(project["id"], "owner", "Executar processo")
    assert any(item["kind"] == "node" for item in results)

    release = project_repository.create_project_release(project["id"], "owner", name="SIGYO 1.0")
    assert release["version"] == 1
    assert len(release["flows"]) == 2

    payload = project_repository.export_project_bundle(project["id"], "owner")
    assert payload.startswith(b"PK")

    imported = project_repository.import_project_bundle(payload, "other", "other@example.com", preserve_ids=False)
    assert imported["project"]["id"] != project["id"]
    assert len(imported["flow_ids"]) == 2
    assert database["produto_tools_projects"].count_documents({}) == 2


def test_project_detects_broken_link(monkeypatch):
    configure(monkeypatch)
    project = project_repository.create_project("Projeto", "", "owner")
    parent, _ = create_linked_flows()
    parent["flow"].update({"projectId": project["id"], "projectRole": "executive", "projectOrder": 1})
    flow_repository.save_flowchart(parent, "owner", actor_username="owner")
    analysis = project_repository.analyze_project(project["id"], "owner")
    assert analysis["broken_count"] == 1
    assert "ausente" in analysis["broken_links"][0]["reasons"][0]
