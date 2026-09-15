from __future__ import annotations

import pytest

mongomock = pytest.importorskip("mongomock")

from services import wbs_repository as repository


def configure(monkeypatch):
    client = mongomock.MongoClient(tz_aware=True)
    database = client["simulador_db"]
    monkeypatch.setattr(repository.db, "initialize_database", lambda: True)
    monkeypatch.setattr(repository.db, "get_collection", lambda name: database[name])
    monkeypatch.setattr(repository.db, "add_log", lambda *args, **kwargs: True)
    monkeypatch.setattr(repository, "list_projects", lambda *args, **kwargs: [])
    monkeypatch.setattr(repository, "get_project", lambda *args, **kwargs: None)
    return database


def test_wbs_crud_and_revision(monkeypatch):
    configure(monkeypatch)
    created = repository.create_wbs("Projeto X", "owner")
    assert created["revision"] == 1
    assert created["nodes"][0]["code"] == "1"

    listed = repository.list_wbs("owner")
    assert [item["id"] for item in listed] == [created["id"]]

    saved = repository.save_wbs(
        created["id"],
        "owner",
        name="Projeto X atualizado",
        description="Descrição",
        project_id="",
        visibility="private",
        status="draft",
        nodes=created["nodes"],
        expected_revision=1,
    )
    assert saved["revision"] == 2
    assert saved["name"] == "Projeto X atualizado"

    with pytest.raises(repository.WbsRevisionConflict):
        repository.save_wbs(
            created["id"],
            "owner",
            name="stale",
            description="",
            project_id="",
            visibility="private",
            status="draft",
            nodes=created["nodes"],
            expected_revision=1,
        )

    duplicate = repository.duplicate_wbs(created["id"], "owner")
    assert duplicate["id"] != created["id"]
    assert duplicate["name"].endswith("Cópia")

    assert repository.delete_wbs(created["id"], "owner") is True
    assert repository.get_wbs(created["id"], "owner") is None
