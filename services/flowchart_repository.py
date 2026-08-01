from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pymongo import DESCENDING
from pymongo.errors import PyMongoError

import database as db
from core.configuration import FLOWCHARTS_COLLECTION, FLOWCHART_VERSIONS_COLLECTION
from schemas.flowchart_schema import normalize_document, validate_document


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def initialize_flowchart_tables() -> None:
    """Mantém o nome legado da função; o armazenamento agora é MongoDB."""
    if not db.initialize_database():
        raise RuntimeError("Não foi possível inicializar o armazenamento de fluxos no MongoDB.")


def _flow_collection():
    collection = db.get_collection(FLOWCHARTS_COLLECTION)
    if collection is None:
        raise RuntimeError("Coleção de fluxos indisponível no MongoDB.")
    return collection


def _version_collection():
    collection = db.get_collection(FLOWCHART_VERSIONS_COLLECTION)
    if collection is None:
        raise RuntimeError("Coleção de versões indisponível no MongoDB.")
    return collection


def _serialize_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(record.get("_id") or record.get("id") or ""),
        "name": str(record.get("name") or "Processo sem nome"),
        "description": str(record.get("description") or ""),
        "status": str(record.get("status") or "draft"),
        "owner_username": str(record.get("owner_username") or ""),
        "owner_email": str(record.get("owner_email") or ""),
        "current_version": int(record.get("current_version") or 1),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
    }


def list_flowcharts(owner_username: str, include_all: bool = False) -> list[dict]:
    initialize_flowchart_tables()
    query = {} if include_all else {"owner_username": owner_username.strip().lower()}
    projection = {"document": 0}
    try:
        records = _flow_collection().find(query, projection).sort("updated_at", DESCENDING)
        return [_serialize_record(record) for record in records]
    except PyMongoError as exc:
        raise RuntimeError("Falha ao listar fluxos no MongoDB.") from exc


def get_flowchart(flowchart_id: str, owner_username: str | None = None) -> dict | None:
    initialize_flowchart_tables()
    query: dict[str, Any] = {"_id": str(flowchart_id)}
    if owner_username:
        query["owner_username"] = owner_username.strip().lower()
    try:
        record = _flow_collection().find_one(query)
    except PyMongoError as exc:
        raise RuntimeError("Falha ao carregar o fluxo no MongoDB.") from exc
    if not record:
        return None
    result = _serialize_record(record)
    result["document"] = deepcopy(record.get("document") or {})
    return result


def save_flowchart(
    document: dict,
    owner_username: str,
    owner_email: str = "",
    *,
    create_version: bool = True,
) -> dict:
    initialize_flowchart_tables()
    normalized_owner = owner_username.strip().lower()
    doc = normalize_document(document, normalized_owner)
    errors = validate_document(doc)
    if errors:
        raise ValueError("Documento inválido: " + " | ".join(errors[:10]))

    flow = doc["flow"]
    flow_id = str(flow["id"])
    name = str(flow["name"]).strip() or "Processo sem nome"
    description = str(flow.get("description", ""))
    status = str(flow.get("status", "draft"))
    now = utc_now()
    flow["updatedAt"] = now.isoformat()

    collection = _flow_collection()
    versions = _version_collection()
    try:
        existing = collection.find_one({"_id": flow_id}, {"current_version": 1, "created_at": 1})
        next_version = (
            int(existing.get("current_version") or 1) + (1 if create_version else 0)
            if existing
            else 1
        )
        created_at = existing.get("created_at") if existing else now

        collection.replace_one(
            {"_id": flow_id},
            {
                "_id": flow_id,
                "id": flow_id,
                "name": name,
                "description": description,
                "status": status,
                "owner_username": normalized_owner,
                "owner_email": owner_email.strip().lower(),
                "current_version": next_version,
                "document": deepcopy(doc),
                "created_at": created_at,
                "updated_at": now,
            },
            upsert=True,
        )

        if create_version:
            versions.replace_one(
                {"flowchart_id": flow_id, "version": next_version},
                {
                    "flowchart_id": flow_id,
                    "version": next_version,
                    "document": deepcopy(doc),
                    "created_by": normalized_owner,
                    "created_at": now,
                },
                upsert=True,
            )

        db.add_log(
            normalized_owner,
            "Salvou fluxo no Produto Tools",
            {"flowchart_id": flow_id, "version": next_version, "name": name},
        )
        return {"id": flow_id, "version": next_version, "document": doc}
    except PyMongoError as exc:
        raise RuntimeError("Falha ao salvar o fluxo no MongoDB.") from exc


def delete_flowchart(
    flowchart_id: str,
    owner_username: str,
    *,
    is_admin: bool = False,
) -> bool:
    initialize_flowchart_tables()
    query: dict[str, Any] = {"_id": str(flowchart_id)}
    if not is_admin:
        query["owner_username"] = owner_username.strip().lower()
    try:
        deleted = _flow_collection().delete_one(query).deleted_count > 0
        if deleted:
            _version_collection().delete_many({"flowchart_id": str(flowchart_id)})
            db.add_log(
                owner_username,
                "Excluiu fluxo no Produto Tools",
                {"flowchart_id": str(flowchart_id)},
            )
        return deleted
    except PyMongoError as exc:
        raise RuntimeError("Falha ao excluir o fluxo no MongoDB.") from exc


def duplicate_flowchart(
    flowchart_id: str,
    owner_username: str,
    owner_email: str = "",
    *,
    is_admin: bool = False,
) -> dict:
    current = get_flowchart(
        flowchart_id, None if is_admin else owner_username
    )
    if not current:
        raise ValueError("Fluxo não encontrado")
    doc = deepcopy(current["document"])
    new_id = f"flow_{uuid4().hex[:12]}"
    now = utc_now_iso()
    doc["flow"]["id"] = new_id
    doc["flow"]["name"] = f"Cópia de {doc['flow'].get('name', 'Processo')}"
    doc["flow"]["createdBy"] = owner_username.strip().lower()
    doc["flow"]["createdAt"] = now
    doc["flow"]["updatedAt"] = now
    return save_flowchart(doc, owner_username, owner_email)


def list_versions(flowchart_id: str) -> list[dict]:
    initialize_flowchart_tables()
    try:
        cursor = _version_collection().find(
            {"flowchart_id": str(flowchart_id)},
            {"_id": 0, "version": 1, "created_by": 1, "created_at": 1},
        ).sort("version", DESCENDING)
        return list(cursor)
    except PyMongoError as exc:
        raise RuntimeError("Falha ao listar versões no MongoDB.") from exc


def get_version(flowchart_id: str, version: int) -> dict | None:
    initialize_flowchart_tables()
    try:
        record = _version_collection().find_one(
            {"flowchart_id": str(flowchart_id), "version": int(version)},
            {"document": 1},
        )
    except PyMongoError as exc:
        raise RuntimeError("Falha ao carregar a versão no MongoDB.") from exc
    return deepcopy(record.get("document")) if record else None
