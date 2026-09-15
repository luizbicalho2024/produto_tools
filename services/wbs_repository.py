from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pymongo import ASCENDING, DESCENDING
from pymongo.errors import PyMongoError

import database as db
from core.configuration import WBS_COLLECTION
from services.project_repository import can_edit_project, can_manage_project, get_project, list_projects
from services.wbs_tools import new_node, normalize_nodes, validate_wbs


class WbsPermissionError(PermissionError):
    pass


class WbsRevisionConflict(RuntimeError):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _collection():
    collection = db.get_collection(WBS_COLLECTION)
    if collection is None:
        raise RuntimeError("MongoDB indisponível para o módulo WBS.")
    return collection


def initialize_wbs_tables() -> None:
    if not db.initialize_database():
        raise RuntimeError("Não foi possível inicializar o MongoDB para o módulo WBS.")
    collection = _collection()
    collection.create_index([("owner_username", ASCENDING), ("updated_at", DESCENDING)], name="ix_pt_wbs_owner_updated")
    collection.create_index([("project_id", ASCENDING), ("updated_at", DESCENDING)], name="ix_pt_wbs_project_updated")
    collection.create_index([("status", ASCENDING), ("updated_at", DESCENDING)], name="ix_pt_wbs_status_updated")


def _serialize(record: dict[str, Any]) -> dict[str, Any]:
    result = {key: deepcopy(value) for key, value in record.items() if key != "_id"}
    result["id"] = str(record.get("_id") or record.get("id") or "")
    result["name"] = str(record.get("name") or "WBS sem nome")
    result["description"] = str(record.get("description") or "")
    result["project_id"] = str(record.get("project_id") or "")
    result["owner_username"] = str(record.get("owner_username") or "")
    result["visibility"] = str(record.get("visibility") or "private")
    result["status"] = str(record.get("status") or "draft")
    result["revision"] = int(record.get("revision") or 1)
    result["nodes"] = normalize_nodes(deepcopy(record.get("nodes") or [])) if record.get("nodes") else []
    return result


def _project_access(project_id: str, actor: str, *, is_admin: bool = False) -> tuple[bool, bool]:
    if not project_id:
        return False, False
    project = get_project(project_id, actor, is_admin=is_admin)
    if not project:
        return False, False
    permission = project.get("permission")
    return True, can_edit_project(permission)


def _can_view(record: dict[str, Any], actor: str, *, is_admin: bool = False) -> bool:
    if is_admin:
        return True
    normalized = actor.strip().lower()
    if str(record.get("owner_username") or "").strip().lower() == normalized:
        return True
    project_id = str(record.get("project_id") or "")
    can_view_project, _ = _project_access(project_id, normalized, is_admin=False)
    return can_view_project or record.get("visibility") == "organization"


def _can_edit(record: dict[str, Any], actor: str, *, is_admin: bool = False) -> bool:
    if is_admin:
        return True
    normalized = actor.strip().lower()
    if str(record.get("owner_username") or "").strip().lower() == normalized:
        return True
    project_id = str(record.get("project_id") or "")
    _, can_edit = _project_access(project_id, normalized, is_admin=False)
    return can_edit


def _can_delete(record: dict[str, Any], actor: str, *, is_admin: bool = False) -> bool:
    if is_admin:
        return True
    normalized = actor.strip().lower()
    if str(record.get("owner_username") or "").strip().lower() == normalized:
        return True
    project_id = str(record.get("project_id") or "")
    if not project_id:
        return False
    project = get_project(project_id, normalized, is_admin=False)
    return bool(project and can_manage_project(project.get("permission")))


def list_wbs(actor: str, *, is_admin: bool = False) -> list[dict[str, Any]]:
    initialize_wbs_tables()
    normalized = actor.strip().lower()
    if is_admin:
        query: dict[str, Any] = {}
    else:
        projects = list_projects(normalized, include_all=False, is_admin=False)
        project_ids = [item["id"] for item in projects]
        clauses: list[dict[str, Any]] = [
            {"owner_username": normalized},
            {"visibility": "organization"},
        ]
        if project_ids:
            clauses.append({"project_id": {"$in": project_ids}})
        query = {"$or": clauses}
    try:
        records = _collection().find(query).sort("updated_at", DESCENDING)
        result = []
        for record in records:
            if not _can_view(record, normalized, is_admin=is_admin):
                continue
            item = _serialize(record)
            item["can_edit"] = _can_edit(record, normalized, is_admin=is_admin)
            item["can_delete"] = _can_delete(record, normalized, is_admin=is_admin)
            result.append(item)
        return result
    except PyMongoError as exc:
        raise RuntimeError("Falha ao listar WBS no MongoDB.") from exc


def get_wbs(wbs_id: str, actor: str, *, is_admin: bool = False) -> dict[str, Any] | None:
    initialize_wbs_tables()
    try:
        record = _collection().find_one({"_id": str(wbs_id)})
    except PyMongoError as exc:
        raise RuntimeError("Falha ao carregar WBS.") from exc
    if not record or not _can_view(record, actor, is_admin=is_admin):
        return None
    item = _serialize(record)
    item["can_edit"] = _can_edit(record, actor, is_admin=is_admin)
    item["can_delete"] = _can_delete(record, actor, is_admin=is_admin)
    return item


def create_wbs(
    name: str,
    actor: str,
    *,
    description: str = "",
    project_id: str = "",
    visibility: str = "private",
    nodes: list[dict[str, Any]] | None = None,
    is_admin: bool = False,
) -> dict[str, Any]:
    initialize_wbs_tables()
    clean_name = str(name or "").strip()
    if not clean_name:
        raise ValueError("O nome da WBS é obrigatório.")
    actor = actor.strip().lower()
    if project_id:
        project = get_project(project_id, actor, is_admin=is_admin)
        if not project or not can_edit_project(project.get("permission")):
            raise WbsPermissionError("Você não possui permissão de edição no projeto selecionado.")
    content = normalize_nodes(nodes or [new_node(clean_name)])
    errors = validate_wbs(content)
    if errors:
        raise ValueError("WBS inválida: " + " | ".join(errors[:10]))
    now = utc_now()
    wbs_id = f"wbs_{uuid4().hex[:14]}"
    record = {
        "_id": wbs_id,
        "id": wbs_id,
        "name": clean_name,
        "description": str(description or "").strip(),
        "project_id": str(project_id or ""),
        "owner_username": actor,
        "visibility": visibility if visibility in {"private", "organization"} else "private",
        "status": "draft",
        "revision": 1,
        "nodes": content,
        "created_at": now,
        "updated_at": now,
        "last_saved_by": actor,
    }
    try:
        _collection().insert_one(record)
        db.add_log(actor, "wbs:create", {"wbs_id": wbs_id, "project_id": project_id, "name": clean_name})
    except PyMongoError as exc:
        raise RuntimeError("Falha ao criar WBS.") from exc
    return _serialize(record)


def save_wbs(
    wbs_id: str,
    actor: str,
    *,
    name: str,
    description: str,
    project_id: str,
    visibility: str,
    status: str,
    nodes: list[dict[str, Any]],
    expected_revision: int | None = None,
    is_admin: bool = False,
) -> dict[str, Any]:
    initialize_wbs_tables()
    actor = actor.strip().lower()
    current = _collection().find_one({"_id": str(wbs_id)})
    if not current:
        raise KeyError(wbs_id)
    if not _can_edit(current, actor, is_admin=is_admin):
        raise WbsPermissionError("Você não possui permissão para editar esta WBS.")
    if project_id:
        project = get_project(project_id, actor, is_admin=is_admin)
        if not project or not can_edit_project(project.get("permission")):
            raise WbsPermissionError("Você não possui permissão de edição no projeto de destino.")
    current_revision = int(current.get("revision") or 1)
    if expected_revision is not None and current_revision != int(expected_revision):
        raise WbsRevisionConflict("A WBS foi alterada por outro usuário. Recarregue antes de salvar.")
    normalized = normalize_nodes(nodes)
    errors = validate_wbs(normalized)
    if errors:
        raise ValueError("WBS inválida: " + " | ".join(errors[:10]))
    clean_name = str(name or "").strip()
    if not clean_name:
        raise ValueError("O nome da WBS é obrigatório.")
    update = {
        "name": clean_name,
        "description": str(description or "").strip(),
        "project_id": str(project_id or ""),
        "visibility": visibility if visibility in {"private", "organization"} else "private",
        "status": status if status in {"draft", "in_review", "published", "archived"} else "draft",
        "nodes": normalized,
        "revision": current_revision + 1,
        "updated_at": utc_now(),
        "last_saved_by": actor,
    }
    result = _collection().update_one({"_id": str(wbs_id), "revision": current_revision}, {"$set": update})
    if result.matched_count == 0:
        raise WbsRevisionConflict("A WBS foi alterada durante o salvamento. Recarregue antes de tentar novamente.")
    db.add_log(actor, "wbs:save", {"wbs_id": wbs_id, "revision": update["revision"], "project_id": project_id})
    return get_wbs(wbs_id, actor, is_admin=is_admin) or {**update, "id": wbs_id}


def duplicate_wbs(wbs_id: str, actor: str, *, is_admin: bool = False) -> dict[str, Any]:
    source = get_wbs(wbs_id, actor, is_admin=is_admin)
    if not source:
        raise KeyError(wbs_id)
    return create_wbs(
        f"{source['name']} - Cópia",
        actor,
        description=source.get("description", ""),
        project_id=source.get("project_id", ""),
        visibility="private",
        nodes=source.get("nodes") or [],
        is_admin=is_admin,
    )


def delete_wbs(wbs_id: str, actor: str, *, is_admin: bool = False) -> bool:
    record = _collection().find_one({"_id": str(wbs_id)})
    if not record:
        return False
    if not _can_delete(record, actor, is_admin=is_admin):
        raise WbsPermissionError("Você não possui permissão para excluir esta WBS.")
    result = _collection().delete_one({"_id": str(wbs_id)})
    if result.deleted_count:
        db.add_log(actor, "wbs:delete", {"wbs_id": wbs_id, "name": record.get("name")})
        return True
    return False
