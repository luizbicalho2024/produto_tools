from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from pymongo import ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

import database as db
from core.configuration import (
    FLOW_ACCESS_LEVELS,
    FLOWCHART_APPROVALS_COLLECTION,
    FLOWCHART_COMMENTS_COLLECTION,
    FLOWCHART_DRAFTS_COLLECTION,
    FLOWCHART_PRESENCE_COLLECTION,
    FLOWCHART_TEMPLATES_COLLECTION,
    FLOWCHART_VERSIONS_COLLECTION,
    FLOWCHARTS_COLLECTION,
    PROJECTS_COLLECTION,
    WORKFLOW_STATUSES,
)
from schemas.flowchart_schema import normalize_document, validate_document
from services.flow_diff import compare_documents


class RevisionConflictError(RuntimeError):
    def __init__(self, current_revision: int, current_record: dict[str, Any] | None = None):
        super().__init__("O fluxo foi alterado por outro usuário.")
        self.current_revision = int(current_revision)
        self.current_record = current_record


class FlowPermissionError(PermissionError):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def document_hash(document: dict[str, Any]) -> str:
    payload = json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def initialize_flowchart_tables() -> None:
    if not db.initialize_database():
        raise RuntimeError("Não foi possível inicializar o armazenamento de fluxos no MongoDB.")


def _collection(name: str):
    collection = db.get_collection(name)
    if collection is None:
        raise RuntimeError(f"Coleção indisponível no MongoDB: {name}")
    return collection


def _flow_collection():
    return _collection(FLOWCHARTS_COLLECTION)


def _version_collection():
    return _collection(FLOWCHART_VERSIONS_COLLECTION)


def _serialize_record(record: dict[str, Any], *, include_document: bool = False) -> dict[str, Any]:
    raw_status = str(record.get("workflow_status") or record.get("status") or "draft")
    normalized_status = "published" if raw_status == "active" else raw_status
    result = {
        "id": str(record.get("_id") or record.get("id") or ""),
        "name": str(record.get("name") or "Processo sem nome"),
        "description": str(record.get("description") or ""),
        "status": normalized_status,
        "workflow_status": normalized_status,
        "owner_username": str(record.get("owner_username") or ""),
        "owner_email": str(record.get("owner_email") or ""),
        "current_version": int(record.get("current_version") or 1),
        "published_version": record.get("published_version"),
        "revision": int(record.get("revision") or 1),
        "visibility": str(record.get("visibility") or "private"),
        "collaborators": deepcopy(record.get("collaborators") or []),
        "tags": deepcopy(record.get("tags") or []),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "last_saved_by": str(record.get("last_saved_by") or record.get("owner_username") or ""),
        "document_hash": str(record.get("document_hash") or ""),
        "project_id": str(record.get("project_id") or ""),
        "project_role": str(record.get("project_role") or ""),
        "project_group": str(record.get("project_group") or ""),
        "project_order": int(record.get("project_order") or 0),
    }
    if include_document:
        result["document"] = deepcopy(record.get("document") or {})
    return result


def _collaborator_level(record: dict[str, Any], username: str) -> str | None:
    normalized = username.strip().lower()
    for item in record.get("collaborators") or []:
        if str(item.get("username") or "").strip().lower() == normalized:
            level = str(item.get("level") or "viewer")
            return level if level in FLOW_ACCESS_LEVELS else "viewer"
    return None


def permission_for(record: dict[str, Any], username: str, *, is_admin: bool = False) -> str | None:
    normalized = username.strip().lower()
    if is_admin or str(record.get("owner_username") or "").strip().lower() == normalized:
        return "owner"
    level = _collaborator_level(record, normalized)
    if level:
        return level
    if record.get("visibility") == "organization":
        return "viewer"
    return None


def can_edit(permission: str | None) -> bool:
    return permission in {"owner", "editor", "reviewer", "approver"}


def can_review(permission: str | None) -> bool:
    return permission in {"owner", "reviewer", "approver"}


def can_approve(permission: str | None) -> bool:
    return permission in {"owner", "approver"}


def list_flowcharts(owner_username: str, include_all: bool = False, project_id: str | None = None) -> list[dict]:
    initialize_flowchart_tables()
    normalized = owner_username.strip().lower()
    query: dict[str, Any]
    if include_all:
        query = {}
    else:
        query = {"$or": [
            {"owner_username": normalized},
            {"collaborators.username": normalized},
            {"visibility": "organization"},
        ]}
    if project_id:
        project_filter = {"project_id": str(project_id)}
        query = project_filter if not query else {"$and": [query, project_filter]}
    projection = {"document": 0}
    try:
        records = _flow_collection().find(query, projection).sort("updated_at", DESCENDING)
        return [_serialize_record(record) for record in records]
    except PyMongoError as exc:
        raise RuntimeError("Falha ao listar fluxos no MongoDB.") from exc


def get_flowchart(
    flowchart_id: str,
    owner_username: str | None = None,
    *,
    actor_username: str | None = None,
    is_admin: bool = False,
) -> dict | None:
    initialize_flowchart_tables()
    try:
        record = _flow_collection().find_one({"_id": str(flowchart_id)})
    except PyMongoError as exc:
        raise RuntimeError("Falha ao carregar o fluxo no MongoDB.") from exc
    if not record:
        return None
    legacy_owner = owner_username.strip().lower() if owner_username else None
    actor = (actor_username or legacy_owner or "").strip().lower()
    if actor and permission_for(record, actor, is_admin=is_admin) is None:
        return None
    result = _serialize_record(record, include_document=True)
    result["permission"] = permission_for(record, actor, is_admin=is_admin) if actor else None
    return result


def _version_payload(
    flow_id: str,
    version: int,
    document: dict[str, Any],
    actor: str,
    reason: str,
    before_document: dict[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    diff = compare_documents(before_document or {}, document) if before_document else None
    return {
        "flowchart_id": flow_id,
        "version": version,
        "document": deepcopy(document),
        "document_hash": document_hash(document),
        "created_by": actor,
        "created_at": now,
        "reason": reason,
        "diff_summary": deepcopy(diff.get("summary")) if diff else {},
    }


def save_flowchart(
    document: dict,
    owner_username: str,
    owner_email: str = "",
    *,
    create_version: bool = True,
    expected_revision: int | None = None,
    actor_username: str | None = None,
    is_admin: bool = False,
    force: bool = False,
    save_reason: str = "manual",
) -> dict:
    initialize_flowchart_tables()
    normalized_owner = owner_username.strip().lower()
    actor = (actor_username or normalized_owner).strip().lower()
    doc = normalize_document(document, normalized_owner)
    errors = validate_document(doc)
    if errors:
        raise ValueError("Documento inválido: " + " | ".join(errors[:12]))

    flow = doc["flow"]
    flow_id = str(flow["id"])
    now = utc_now()
    flow["updatedAt"] = now.isoformat()
    collection, versions = _flow_collection(), _version_collection()

    try:
        existing = collection.find_one({"_id": flow_id})
        if existing:
            permission = permission_for(existing, actor, is_admin=is_admin)
            if not can_edit(permission):
                raise FlowPermissionError("Seu perfil não possui permissão para editar este fluxo.")
            current_revision = int(existing.get("revision") or 1)
            if expected_revision is not None and current_revision != int(expected_revision) and not force:
                raise RevisionConflictError(current_revision, _serialize_record(existing, include_document=True))
            next_revision = current_revision + 1
            next_version = int(existing.get("current_version") or 1) + (1 if create_version else 0)
            workflow_status = str(existing.get("workflow_status") or existing.get("status") or "draft")
            if workflow_status == "active":
                workflow_status = "published"
            # Alterar uma versão publicada cria novamente um rascunho, preservando a publicação anterior.
            if workflow_status == "published" and save_reason not in {"publish", "archive"}:
                workflow_status = "draft"
            replacement = {
                **{key: deepcopy(value) for key, value in existing.items() if key not in {"document", "revision", "updated_at", "last_saved_by", "document_hash", "name", "description", "status", "workflow_status", "tags", "current_version"}},
                "_id": flow_id,
                "id": flow_id,
                "name": str(flow.get("name") or "Processo sem nome").strip(),
                "description": str(flow.get("description") or ""),
                "status": workflow_status,
                "workflow_status": workflow_status,
                "tags": deepcopy(flow.get("tags") or existing.get("tags") or []),
                "owner_username": str(existing.get("owner_username") or normalized_owner),
                "owner_email": str(existing.get("owner_email") or owner_email).strip().lower(),
                "current_version": next_version,
                "revision": next_revision,
                "document": deepcopy(doc),
                "document_hash": document_hash(doc),
                "created_at": existing.get("created_at") or now,
                "updated_at": now,
                "last_saved_by": actor,
                "visibility": existing.get("visibility") or "private",
                "collaborators": deepcopy(existing.get("collaborators") or []),
                "published_version": existing.get("published_version"),
                "project_id": str(flow.get("projectId") or existing.get("project_id") or ""),
                "project_role": str(flow.get("projectRole") or existing.get("project_role") or ""),
                "project_group": str(flow.get("projectGroup") or existing.get("project_group") or ""),
                "project_order": int(flow.get("projectOrder") or existing.get("project_order") or 0),
            }
            query: dict[str, Any] = {"_id": flow_id}
            if not force:
                query["revision"] = current_revision if "revision" in existing else {"$in": [None, current_revision]}
            result = collection.replace_one(query, replacement)
            if result.matched_count == 0:
                latest = collection.find_one({"_id": flow_id})
                raise RevisionConflictError(int((latest or {}).get("revision") or current_revision), _serialize_record(latest, include_document=True) if latest else None)
            if create_version:
                versions.replace_one(
                    {"flowchart_id": flow_id, "version": next_version},
                    _version_payload(flow_id, next_version, doc, actor, save_reason, existing.get("document"), now),
                    upsert=True,
                )
        else:
            next_revision, next_version = 1, 1
            replacement = {
                "_id": flow_id, "id": flow_id,
                "name": str(flow.get("name") or "Processo sem nome").strip(),
                "description": str(flow.get("description") or ""),
                "status": str(flow.get("status") or "draft"),
                "workflow_status": str(flow.get("status") or "draft"),
                "tags": deepcopy(flow.get("tags") or []),
                "owner_username": normalized_owner,
                "owner_email": owner_email.strip().lower(),
                "current_version": next_version,
                "published_version": None,
                "revision": next_revision,
                "visibility": "private",
                "collaborators": [],
                "document": deepcopy(doc),
                "document_hash": document_hash(doc),
                "created_at": now, "updated_at": now, "last_saved_by": actor,
                "project_id": str(flow.get("projectId") or ""),
                "project_role": str(flow.get("projectRole") or ""),
                "project_group": str(flow.get("projectGroup") or ""),
                "project_order": int(flow.get("projectOrder") or 0),
            }
            try:
                collection.insert_one(replacement)
            except DuplicateKeyError:
                latest = collection.find_one({"_id": flow_id})
                raise RevisionConflictError(int((latest or {}).get("revision") or 1), _serialize_record(latest, include_document=True) if latest else None)
            if create_version:
                versions.replace_one(
                    {"flowchart_id": flow_id, "version": 1},
                    _version_payload(flow_id, 1, doc, actor, save_reason, None, now),
                    upsert=True,
                )
        project_id = str(doc.get("flow", {}).get("projectId") or "")
        if project_id:
            project = _collection(PROJECTS_COLLECTION).find_one({"_id": project_id}, {"members": 1, "visibility": 1})
            if project:
                collection.update_one(
                    {"_id": flow_id},
                    {"$set": {
                        "collaborators": deepcopy(project.get("members") or []),
                        "visibility": str(project.get("visibility") or "private"),
                    }},
                )
        _collection(FLOWCHART_DRAFTS_COLLECTION).delete_one({"flowchart_id": flow_id, "username": actor})
        db.add_log(actor, "Salvou fluxo no Produto Tools", {"flowchart_id": flow_id, "version": next_version, "revision": next_revision, "reason": save_reason, "project_id": project_id})
        return {"id": flow_id, "version": next_version, "revision": next_revision, "document": doc}
    except (RevisionConflictError, FlowPermissionError, ValueError):
        raise
    except PyMongoError as exc:
        raise RuntimeError("Falha ao salvar o fluxo no MongoDB.") from exc


def save_draft(flowchart_id: str, username: str, document: dict, base_revision: int) -> dict[str, Any]:
    initialize_flowchart_tables()
    now = utc_now()
    normalized = username.strip().lower()
    normalized_doc = normalize_document(document, normalized)
    try:
        _collection(FLOWCHART_DRAFTS_COLLECTION).replace_one(
            {"flowchart_id": str(flowchart_id), "username": normalized},
            {"flowchart_id": str(flowchart_id), "project_id": str(normalized_doc.get("flow", {}).get("projectId") or ""), "username": normalized, "base_revision": int(base_revision), "document": normalized_doc, "updated_at": now},
            upsert=True,
        )
        return {"updated_at": now, "base_revision": int(base_revision)}
    except PyMongoError as exc:
        raise RuntimeError("Falha ao salvar o rascunho automático.") from exc


def get_draft(flowchart_id: str, username: str) -> dict[str, Any] | None:
    try:
        record = _collection(FLOWCHART_DRAFTS_COLLECTION).find_one({"flowchart_id": str(flowchart_id), "username": username.strip().lower()})
        if not record:
            return None
        return {"document": deepcopy(record.get("document") or {}), "base_revision": int(record.get("base_revision") or 0), "updated_at": record.get("updated_at")}
    except PyMongoError as exc:
        raise RuntimeError("Falha ao carregar o rascunho automático.") from exc


def discard_draft(flowchart_id: str, username: str) -> bool:
    return _collection(FLOWCHART_DRAFTS_COLLECTION).delete_one({"flowchart_id": str(flowchart_id), "username": username.strip().lower()}).deleted_count > 0


def delete_flowchart(flowchart_id: str, owner_username: str, *, is_admin: bool = False) -> bool:
    initialize_flowchart_tables()
    actor = owner_username.strip().lower()
    record = _flow_collection().find_one({"_id": str(flowchart_id)})
    if not record or permission_for(record, actor, is_admin=is_admin) != "owner":
        return False
    try:
        deleted = _flow_collection().delete_one({"_id": str(flowchart_id)}).deleted_count > 0
        if deleted:
            for name in (FLOWCHART_VERSIONS_COLLECTION, FLOWCHART_DRAFTS_COLLECTION, FLOWCHART_COMMENTS_COLLECTION, FLOWCHART_APPROVALS_COLLECTION, FLOWCHART_PRESENCE_COLLECTION):
                _collection(name).delete_many({"flowchart_id": str(flowchart_id)})
            db.add_log(actor, "Excluiu fluxo no Produto Tools", {"flowchart_id": str(flowchart_id)})
        return deleted
    except PyMongoError as exc:
        raise RuntimeError("Falha ao excluir o fluxo no MongoDB.") from exc


def duplicate_flowchart(flowchart_id: str, owner_username: str, owner_email: str = "", *, is_admin: bool = False) -> dict:
    current = get_flowchart(flowchart_id, actor_username=owner_username, is_admin=is_admin)
    if not current:
        raise ValueError("Fluxo não encontrado")
    doc = deepcopy(current["document"])
    new_id, now = f"flow_{uuid4().hex[:12]}", utc_now_iso()
    doc["flow"].update({"id": new_id, "name": f"Cópia de {doc['flow'].get('name', 'Processo')}", "status": "draft", "createdBy": owner_username.strip().lower(), "createdAt": now, "updatedAt": now})
    return save_flowchart(doc, owner_username, owner_email, actor_username=owner_username, save_reason="duplicate")


def list_versions(flowchart_id: str) -> list[dict]:
    initialize_flowchart_tables()
    try:
        return list(_version_collection().find(
            {"flowchart_id": str(flowchart_id)},
            {"_id": 0, "version": 1, "created_by": 1, "created_at": 1, "reason": 1, "diff_summary": 1, "document_hash": 1},
        ).sort("version", DESCENDING))
    except PyMongoError as exc:
        raise RuntimeError("Falha ao listar versões no MongoDB.") from exc


def get_version(flowchart_id: str, version: int) -> dict | None:
    try:
        record = _version_collection().find_one({"flowchart_id": str(flowchart_id), "version": int(version)}, {"document": 1})
        return deepcopy(record.get("document")) if record else None
    except PyMongoError as exc:
        raise RuntimeError("Falha ao carregar a versão no MongoDB.") from exc


def compare_versions(flowchart_id: str, left_version: int, right_version: int) -> dict[str, Any]:
    left, right = get_version(flowchart_id, left_version), get_version(flowchart_id, right_version)
    if left is None or right is None:
        raise ValueError("Uma das versões selecionadas não foi encontrada.")
    return compare_documents(left, right)


def set_collaborators(flowchart_id: str, actor: str, collaborators: list[dict[str, str]], visibility: str = "private", *, is_admin: bool = False) -> bool:
    record = _flow_collection().find_one({"_id": str(flowchart_id)})
    if not record or permission_for(record, actor, is_admin=is_admin) != "owner":
        raise FlowPermissionError("Somente o proprietário pode alterar o compartilhamento.")
    clean, seen = [], set()
    for item in collaborators:
        username = str(item.get("username") or "").strip().lower()
        level = str(item.get("level") or "viewer")
        if username and username not in seen and level in FLOW_ACCESS_LEVELS and username != record.get("owner_username"):
            clean.append({"username": username, "level": level})
            seen.add(username)
    result = _flow_collection().update_one({"_id": str(flowchart_id)}, {"$set": {"collaborators": clean, "visibility": visibility if visibility in {"private", "organization"} else "private", "updated_at": utc_now()}})
    db.add_log(actor, "Alterou compartilhamento de fluxo", {"flowchart_id": flowchart_id, "collaborators": clean, "visibility": visibility})
    return result.matched_count > 0


def transition_workflow(flowchart_id: str, actor: str, action: str, *, comment: str = "", is_admin: bool = False) -> dict[str, Any]:
    record = _flow_collection().find_one({"_id": str(flowchart_id)})
    if not record:
        raise ValueError("Fluxo não encontrado.")
    permission = permission_for(record, actor, is_admin=is_admin)
    current = str(record.get("workflow_status") or record.get("status") or "draft")
    if current == "active":
        current = "published"
    transitions = {
        "submit_review": ({"draft"}, "in_review", can_edit),
        "request_changes": ({"in_review", "approved"}, "draft", can_review),
        "approve": ({"in_review"}, "approved", can_approve),
        "publish": ({"approved"}, "published", can_approve),
        "archive": ({"published", "approved", "draft"}, "archived", can_approve),
        "reopen": ({"archived"}, "draft", can_approve),
    }
    if action not in transitions:
        raise ValueError("Ação de governança inválida.")
    allowed_from, target, permission_fn = transitions[action]
    if current not in allowed_from:
        raise ValueError(f"A transição {action} não é permitida a partir de {current}.")
    if not permission_fn(permission):
        raise FlowPermissionError("Seu perfil não possui permissão para esta transição.")
    update: dict[str, Any] = {"workflow_status": target, "status": target, "updated_at": utc_now(), "last_saved_by": actor}
    if target == "published":
        update["published_version"] = int(record.get("current_version") or 1)
        update["published_at"] = utc_now()
        update["published_by"] = actor
    _flow_collection().update_one({"_id": str(flowchart_id)}, {"$set": update})
    _collection(FLOWCHART_APPROVALS_COLLECTION).insert_one({
        "flowchart_id": str(flowchart_id), "from_status": current, "to_status": target,
        "action": action, "comment": comment.strip(), "created_by": actor.strip().lower(), "created_at": utc_now(),
    })
    db.add_log(actor, "Alterou status de governança", {"flowchart_id": flowchart_id, "from": current, "to": target, "action": action})
    return {"from": current, "to": target}


def list_approval_history(flowchart_id: str) -> list[dict[str, Any]]:
    return list(_collection(FLOWCHART_APPROVALS_COLLECTION).find({"flowchart_id": str(flowchart_id)}, {"_id": 0}).sort("created_at", DESCENDING))


def add_comment(flowchart_id: str, target_kind: str, target_id: str, content: str, author: str, mentions: list[str] | None = None) -> str:
    clean = content.strip()
    if not clean:
        raise ValueError("O comentário não pode ficar vazio.")
    comment_id = f"comment_{uuid4().hex[:12]}"
    _collection(FLOWCHART_COMMENTS_COLLECTION).insert_one({
        "_id": comment_id, "flowchart_id": str(flowchart_id), "target_kind": target_kind,
        "target_id": str(target_id), "content": clean, "author": author.strip().lower(),
        "mentions": sorted({item.strip().lower() for item in (mentions or []) if item.strip()}),
        "resolved": False, "created_at": utc_now(), "updated_at": utc_now(),
    })
    return comment_id


def list_comments(flowchart_id: str, *, include_resolved: bool = True, target_kind: str | None = None, target_id: str | None = None) -> list[dict[str, Any]]:
    query: dict[str, Any] = {"flowchart_id": str(flowchart_id)}
    if not include_resolved:
        query["resolved"] = False
    if target_kind:
        query["target_kind"] = target_kind
    if target_id:
        query["target_id"] = str(target_id)
    return list(_collection(FLOWCHART_COMMENTS_COLLECTION).find(query).sort("created_at", DESCENDING))


def resolve_comment(comment_id: str, actor: str, resolved: bool = True) -> bool:
    result = _collection(FLOWCHART_COMMENTS_COLLECTION).update_one({"_id": str(comment_id)}, {"$set": {"resolved": bool(resolved), "resolved_by": actor.strip().lower(), "resolved_at": utc_now(), "updated_at": utc_now()}})
    return result.matched_count > 0


def create_template(name: str, description: str, category: str, document: dict, owner_username: str, *, organization: bool = False) -> str:
    template_id = f"template_{uuid4().hex[:12]}"
    _collection(FLOWCHART_TEMPLATES_COLLECTION).insert_one({
        "_id": template_id, "name": name.strip() or "Template", "description": description.strip(),
        "category": category.strip() or "Geral", "owner_username": owner_username.strip().lower(),
        "organization": bool(organization), "document": normalize_document(document, owner_username),
        "created_at": utc_now(), "updated_at": utc_now(),
    })
    return template_id


def list_custom_templates(username: str, *, include_all: bool = False) -> list[dict[str, Any]]:
    query = {} if include_all else {"$or": [{"owner_username": username.strip().lower()}, {"organization": True}]}
    return list(_collection(FLOWCHART_TEMPLATES_COLLECTION).find(query).sort([("category", ASCENDING), ("name", ASCENDING)]))


def delete_template(template_id: str, username: str, *, is_admin: bool = False) -> bool:
    query: dict[str, Any] = {"_id": str(template_id)}
    if not is_admin:
        query["owner_username"] = username.strip().lower()
    return _collection(FLOWCHART_TEMPLATES_COLLECTION).delete_one(query).deleted_count > 0


def touch_presence(flowchart_id: str, username: str, display_name: str) -> None:
    now = utc_now()
    _collection(FLOWCHART_PRESENCE_COLLECTION).replace_one(
        {"flowchart_id": str(flowchart_id), "username": username.strip().lower()},
        {"flowchart_id": str(flowchart_id), "username": username.strip().lower(), "display_name": display_name, "last_seen": now, "expires_at": now + timedelta(minutes=3)},
        upsert=True,
    )


def list_presence(flowchart_id: str, *, exclude_username: str | None = None) -> list[dict[str, Any]]:
    query: dict[str, Any] = {"flowchart_id": str(flowchart_id), "expires_at": {"$gt": utc_now()}}
    if exclude_username:
        query["username"] = {"$ne": exclude_username.strip().lower()}
    return list(_collection(FLOWCHART_PRESENCE_COLLECTION).find(query, {"_id": 0}).sort("last_seen", DESCENDING))
