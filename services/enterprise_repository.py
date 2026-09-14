from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from bson.binary import Binary
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import PyMongoError

import database as db

COLLECTIONS = {
    "sipoc": "produto_tools_sipoc",
    "action_plan": "produto_tools_action_plans",
    "risk": "produto_tools_risks",
    "root_cause": "produto_tools_root_causes",
    "vsm": "produto_tools_vsm",
    "scenario": "produto_tools_scenarios",
    "dmn": "produto_tools_dmn",
    "change_request": "produto_tools_change_requests",
    "compliance": "produto_tools_compliance",
    "system": "produto_tools_system_catalog",
    "capability": "produto_tools_capabilities",
    "journey": "produto_tools_customer_journeys",
    "evidence": "produto_tools_evidence",
    "webhook": "produto_tools_webhooks",
    "notification": "produto_tools_notifications",
    "template_meta": "produto_tools_template_meta",
}

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def _collection(kind: str):
    name = COLLECTIONS.get(kind)
    if not name:
        raise ValueError(f"Tipo de registro desconhecido: {kind}")
    collection = db.get_collection(name)
    if collection is None:
        raise RuntimeError("MongoDB indisponível.")
    return collection

def initialize_enterprise_tables() -> bool:
    try:
        for kind, name in COLLECTIONS.items():
            collection = db.get_collection(name)
            if collection is None:
                return False
            collection.create_index([("id", ASCENDING)], unique=True, name=f"uq_{kind}_id")
            collection.create_index([("project_id", ASCENDING), ("updated_at", DESCENDING)], name=f"ix_{kind}_project_updated")
            collection.create_index([("flow_id", ASCENDING), ("updated_at", DESCENDING)], name=f"ix_{kind}_flow_updated")
            collection.create_index([("owner_username", ASCENDING), ("updated_at", DESCENDING)], name=f"ix_{kind}_owner_updated")
        notifications = _collection("notification")
        notifications.create_index([("username", ASCENDING), ("read", ASCENDING), ("created_at", DESCENDING)], name="ix_notifications_user_read")
        return True
    except PyMongoError:
        return False

def save_record(
    kind: str,
    payload: dict[str, Any],
    actor_username: str,
    *,
    record_id: str | None = None,
    project_id: str = "",
    flow_id: str = "",
) -> dict[str, Any]:
    collection = _collection(kind)
    now = utc_now()
    identifier = str(record_id or payload.get("id") or f"{kind}_{uuid4().hex[:14]}")
    current = collection.find_one({"id": identifier})
    owner = str((current or {}).get("owner_username") or actor_username).strip().lower()
    document = {
        **payload,
        "id": identifier,
        "kind": kind,
        "project_id": str(project_id or payload.get("project_id") or ""),
        "flow_id": str(flow_id or payload.get("flow_id") or ""),
        "owner_username": owner,
        "updated_by": actor_username,
        "updated_at": now,
    }
    if current:
        collection.update_one({"id": identifier}, {"$set": document})
    else:
        document["created_by"] = actor_username
        document["created_at"] = now
        collection.insert_one(document)
    db.add_log(actor_username, f"enterprise:{kind}:save", {"id": identifier, "flow_id": document["flow_id"], "project_id": document["project_id"]})
    document.pop("_id", None)
    return document

def list_records(
    kind: str,
    actor_username: str,
    *,
    project_id: str | None = None,
    flow_id: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    collection = _collection(kind)
    query: dict[str, Any] = {}
    if project_id is not None:
        query["project_id"] = project_id
    if flow_id is not None:
        query["flow_id"] = flow_id
    cursor = collection.find(query, {"_id": 0, "content": 0}).sort("updated_at", DESCENDING).limit(max(1, min(int(limit), 2000)))
    return list(cursor)

def delete_record(kind: str, record_id: str, actor_username: str) -> bool:
    collection = _collection(kind)
    result = collection.delete_one({"id": record_id})
    if result.deleted_count:
        db.add_log(actor_username, f"enterprise:{kind}:delete", {"id": record_id})
        return True
    return False

def save_evidence(
    actor_username: str,
    filename: str,
    mime_type: str,
    content: bytes,
    *,
    project_id: str = "",
    flow_id: str = "",
    node_id: str = "",
    description: str = "",
) -> dict[str, Any]:
    if len(content) > 5 * 1024 * 1024:
        raise ValueError("Cada evidência deve ter no máximo 5 MB.")
    payload = {
        "filename": filename,
        "mime_type": mime_type or "application/octet-stream",
        "size": len(content),
        "node_id": node_id,
        "description": description,
        "content": Binary(content),
    }
    return save_record("evidence", payload, actor_username, project_id=project_id, flow_id=flow_id)

def load_evidence(record_id: str) -> tuple[bytes, dict[str, Any]] | None:
    record = _collection("evidence").find_one({"id": record_id})
    if not record:
        return None
    content = bytes(record.get("content") or b"")
    metadata = {key: value for key, value in record.items() if key not in {"_id", "content"}}
    return content, metadata

def add_notification(username: str, title: str, message: str, *, category: str = "info", link: str = "") -> dict[str, Any]:
    return save_record(
        "notification",
        {
            "username": username.strip().lower(),
            "title": title,
            "message": message,
            "category": category,
            "link": link,
            "read": False,
        },
        "sistema",
    )

def list_notifications(username: str, *, unread_only: bool = False, limit: int = 100) -> list[dict[str, Any]]:
    query: dict[str, Any] = {"username": username.strip().lower()}
    if unread_only:
        query["read"] = False
    return list(_collection("notification").find(query, {"_id": 0}).sort("created_at", DESCENDING).limit(limit))

def mark_notification_read(record_id: str, username: str) -> bool:
    result = _collection("notification").update_one(
        {"id": record_id, "username": username.strip().lower()},
        {"$set": {"read": True, "updated_at": utc_now()}},
    )
    return result.matched_count > 0

def export_record_json(kind: str, record_id: str) -> bytes:
    record = _collection(kind).find_one({"id": record_id}, {"_id": 0, "content": 0})
    if not record:
        raise KeyError(record_id)
    return json.dumps(record, ensure_ascii=False, indent=2, default=str).encode("utf-8")