from __future__ import annotations

from copy import deepcopy

import pytest

mongomock = pytest.importorskip("mongomock")

from schemas.flowchart_schema import demo_flowchart_document
from services import flowchart_repository as repository


def configure_repository(monkeypatch):
    client = mongomock.MongoClient(tz_aware=True)
    database = client["simulador_db"]
    monkeypatch.setattr(repository.db, "initialize_database", lambda: True)
    monkeypatch.setattr(repository.db, "get_collection", lambda name: database[name])
    monkeypatch.setattr(repository.db, "add_log", lambda *args, **kwargs: True)
    return database


def test_repository_concurrency_drafts_comments_and_governance(monkeypatch):
    database = configure_repository(monkeypatch)
    document = demo_flowchart_document("owner")

    created = repository.save_flowchart(
        document,
        "owner",
        "owner@example.com",
        actor_username="owner",
        save_reason="test_create",
    )
    assert created["revision"] == 1
    assert created["version"] == 1

    loaded = repository.get_flowchart(created["id"], actor_username="owner")
    assert loaded is not None
    assert loaded["permission"] == "owner"

    edited = deepcopy(loaded["document"])
    edited["flow"]["name"] = "Fluxo alterado"
    saved = repository.save_flowchart(
        edited,
        "owner",
        expected_revision=loaded["revision"],
        actor_username="owner",
        save_reason="test_update",
    )
    assert saved["revision"] == 2
    assert saved["version"] == 2

    stale = deepcopy(edited)
    stale["flow"]["description"] = "Edição concorrente"
    with pytest.raises(repository.RevisionConflictError):
        repository.save_flowchart(
            stale,
            "owner",
            expected_revision=1,
            actor_username="owner",
            save_reason="stale_update",
        )

    repository.save_draft(created["id"], "owner", stale, base_revision=2)
    draft = repository.get_draft(created["id"], "owner")
    assert draft is not None
    assert draft["base_revision"] == 2

    comment_id = repository.add_comment(
        created["id"], "node", "node_analyze", "Validar esta etapa", "owner", ["reviewer"]
    )
    comments = repository.list_comments(created["id"])
    assert comments[0]["_id"] == comment_id
    assert repository.resolve_comment(comment_id, "owner") is True

    repository.set_collaborators(
        created["id"],
        "owner",
        [{"username": "reviewer", "level": "approver"}],
        "private",
    )
    shared = repository.get_flowchart(created["id"], actor_username="reviewer")
    assert shared is not None
    assert shared["permission"] == "approver"

    repository.transition_workflow(created["id"], "owner", "submit_review")
    repository.transition_workflow(created["id"], "reviewer", "approve")
    repository.transition_workflow(created["id"], "reviewer", "publish")
    published = repository.get_flowchart(created["id"], actor_username="owner")
    assert published["workflow_status"] == "published"
    assert published["published_version"] == 2

    repository.touch_presence(created["id"], "reviewer", "Revisor")
    presence = repository.list_presence(created["id"], exclude_username="owner")
    assert presence and presence[0]["username"] == "reviewer"

    assert database["produto_tools_flowchart_versions"].count_documents({"flowchart_id": created["id"]}) == 2
