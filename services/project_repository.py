from __future__ import annotations

import io
import json
import re
import zipfile
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import uuid4

from pymongo import ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError, PyMongoError

import database as db
from core.configuration import (
    FLOW_ACCESS_LEVELS,
    FLOWCHARTS_COLLECTION,
    PROJECT_MEMBERS_COLLECTION,
    PROJECT_RELEASES_COLLECTION,
    PROJECT_RELEASE_FLOWS_COLLECTION,
    PROJECT_ROLES,
    PROJECT_STATUSES,
    PROJECTS_COLLECTION,
)
from schemas.flowchart_schema import normalize_document, repair_import_document, validate_document
from services.flow_analytics import analyze_document
from services.flowchart_repository import (
    can_edit,
    document_hash,
    get_flowchart,
    get_version,
    list_flowcharts,
    permission_for as flow_permission_for,
    save_flowchart,
)


class ProjectPermissionError(PermissionError):
    pass


class ProjectImportError(ValueError):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def _collection(name: str):
    collection = db.get_collection(name)
    if collection is None:
        raise RuntimeError(f"Coleção indisponível no MongoDB: {name}")
    return collection


def initialize_project_tables() -> None:
    if not db.initialize_database():
        raise RuntimeError("Não foi possível inicializar o armazenamento de projetos no MongoDB.")


def _slug(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "-", str(value or "").strip()).strip("-").lower()
    return text or "projeto"


def _serialize_project(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(record.get("_id") or record.get("id") or ""),
        "name": str(record.get("name") or "Projeto sem nome"),
        "code": str(record.get("code") or ""),
        "description": str(record.get("description") or ""),
        "status": str(record.get("status") or "draft"),
        "owner_username": str(record.get("owner_username") or ""),
        "owner_email": str(record.get("owner_email") or ""),
        "visibility": str(record.get("visibility") or "private"),
        "members": deepcopy(record.get("members") or []),
        "default_flow_id": str(record.get("default_flow_id") or ""),
        "tags": deepcopy(record.get("tags") or []),
        "settings": deepcopy(record.get("settings") or {}),
        "current_release": int(record.get("current_release") or 0),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "last_saved_by": str(record.get("last_saved_by") or record.get("owner_username") or ""),
    }


def _member_level(record: dict[str, Any], username: str) -> str | None:
    normalized = str(username or "").strip().lower()
    for member in record.get("members") or []:
        if str(member.get("username") or "").strip().lower() == normalized:
            level = str(member.get("level") or "viewer")
            return level if level in FLOW_ACCESS_LEVELS else "viewer"
    return None


def project_permission(record: dict[str, Any], username: str, *, is_admin: bool = False) -> str | None:
    normalized = str(username or "").strip().lower()
    if is_admin or str(record.get("owner_username") or "").strip().lower() == normalized:
        return "owner"
    member_level = _member_level(record, normalized)
    if member_level:
        return member_level
    if record.get("visibility") == "organization":
        return "viewer"
    return None


def can_manage_project(permission: str | None) -> bool:
    return permission in {"owner", "approver"}


def can_edit_project(permission: str | None) -> bool:
    return permission in {"owner", "editor", "reviewer", "approver"}


def _sync_project_access_to_flows(project_id: str, project_record: dict[str, Any]) -> None:
    """Aplica aos fluxos a visibilidade e os participantes herdados do projeto."""
    members = deepcopy(project_record.get("members") or [])
    visibility = str(project_record.get("visibility") or "private")
    _collection(FLOWCHARTS_COLLECTION).update_many(
        {"project_id": str(project_id)},
        {"$set": {"collaborators": members, "visibility": visibility}},
    )


def list_projects(username: str, *, include_all: bool = False, is_admin: bool = False) -> list[dict[str, Any]]:
    initialize_project_tables()
    normalized = str(username or "").strip().lower()
    if include_all and is_admin:
        query: dict[str, Any] = {}
    else:
        query = {"$or": [
            {"owner_username": normalized},
            {"members.username": normalized},
            {"visibility": "organization"},
        ]}
    try:
        records = _collection(PROJECTS_COLLECTION).find(query).sort("updated_at", DESCENDING)
        result = []
        for record in records:
            permission = project_permission(record, normalized, is_admin=is_admin)
            if permission:
                item = _serialize_project(record)
                item["permission"] = permission
                item["flow_count"] = _collection(FLOWCHARTS_COLLECTION).count_documents({"project_id": item["id"]})
                result.append(item)
        return result
    except PyMongoError as exc:
        raise RuntimeError("Falha ao listar projetos no MongoDB.") from exc


def get_project(project_id: str, username: str, *, is_admin: bool = False) -> dict[str, Any] | None:
    initialize_project_tables()
    try:
        record = _collection(PROJECTS_COLLECTION).find_one({"_id": str(project_id)})
    except PyMongoError as exc:
        raise RuntimeError("Falha ao carregar o projeto no MongoDB.") from exc
    if not record:
        return None
    permission = project_permission(record, username, is_admin=is_admin)
    if not permission:
        return None
    result = _serialize_project(record)
    result["permission"] = permission
    return result


def create_project(
    name: str,
    description: str,
    owner_username: str,
    owner_email: str = "",
    *,
    code: str = "",
    project_id: str | None = None,
    tags: list[str] | None = None,
    default_flow_id: str = "",
) -> dict[str, Any]:
    initialize_project_tables()
    clean_name = str(name or "").strip()
    if not clean_name:
        raise ValueError("O nome do projeto é obrigatório.")
    owner = str(owner_username or "").strip().lower()
    now = utc_now()
    final_id = str(project_id or f"project_{uuid4().hex[:12]}")
    record = {
        "_id": final_id,
        "id": final_id,
        "name": clean_name,
        "code": str(code or _slug(clean_name).upper().replace("-", "_")[:40]),
        "description": str(description or "").strip(),
        "status": "draft",
        "owner_username": owner,
        "owner_email": str(owner_email or "").strip().lower(),
        "visibility": "private",
        "members": [],
        "default_flow_id": str(default_flow_id or ""),
        "tags": sorted({str(item).strip() for item in (tags or []) if str(item).strip()}),
        "settings": {
            "openLinkedFlowInTab": True,
            "projectPlayback": True,
            "globalSearch": True,
            "requireReleaseForPublish": True,
        },
        "current_release": 0,
        "created_at": now,
        "updated_at": now,
        "last_saved_by": owner,
    }
    try:
        _collection(PROJECTS_COLLECTION).insert_one(record)
        db.add_log(owner, "Criou projeto no Produto Tools", {"project_id": final_id, "name": clean_name})
        result = _serialize_project(record)
        result["permission"] = "owner"
        return result
    except DuplicateKeyError as exc:
        raise ValueError("Já existe um projeto com este identificador.") from exc
    except PyMongoError as exc:
        raise RuntimeError("Falha ao criar o projeto no MongoDB.") from exc


def update_project(
    project_id: str,
    actor: str,
    *,
    name: str | None = None,
    description: str | None = None,
    code: str | None = None,
    status: str | None = None,
    visibility: str | None = None,
    default_flow_id: str | None = None,
    tags: list[str] | None = None,
    settings: dict[str, Any] | None = None,
    is_admin: bool = False,
) -> dict[str, Any]:
    record = _collection(PROJECTS_COLLECTION).find_one({"_id": str(project_id)})
    if not record:
        raise ValueError("Projeto não encontrado.")
    permission = project_permission(record, actor, is_admin=is_admin)
    if not can_edit_project(permission):
        raise ProjectPermissionError("Seu perfil não possui permissão para alterar este projeto.")
    update: dict[str, Any] = {"updated_at": utc_now(), "last_saved_by": actor.strip().lower()}
    if name is not None:
        if not str(name).strip():
            raise ValueError("O nome do projeto é obrigatório.")
        update["name"] = str(name).strip()
    if description is not None:
        update["description"] = str(description).strip()
    if code is not None:
        update["code"] = str(code).strip()
    if status is not None:
        if status not in PROJECT_STATUSES:
            raise ValueError("Status do projeto inválido.")
        update["status"] = status
    if visibility is not None:
        if visibility not in {"private", "organization"}:
            raise ValueError("Visibilidade inválida.")
        if not can_manage_project(permission):
            raise ProjectPermissionError("Somente o proprietário ou aprovador pode alterar a visibilidade.")
        update["visibility"] = visibility
    if default_flow_id is not None:
        update["default_flow_id"] = str(default_flow_id)
    if tags is not None:
        update["tags"] = sorted({str(item).strip() for item in tags if str(item).strip()})
    if settings is not None:
        update["settings"] = {**deepcopy(record.get("settings") or {}), **deepcopy(settings)}
    _collection(PROJECTS_COLLECTION).update_one({"_id": str(project_id)}, {"$set": update})
    if "visibility" in update:
        refreshed = _collection(PROJECTS_COLLECTION).find_one({"_id": str(project_id)}) or record
        _sync_project_access_to_flows(project_id, refreshed)
    db.add_log(actor, "Atualizou projeto no Produto Tools", {"project_id": project_id, "fields": sorted(update)})
    return get_project(project_id, actor, is_admin=is_admin) or {}


def set_project_members(
    project_id: str,
    actor: str,
    members: list[dict[str, str]],
    *,
    is_admin: bool = False,
) -> list[dict[str, str]]:
    record = _collection(PROJECTS_COLLECTION).find_one({"_id": str(project_id)})
    if not record or not can_manage_project(project_permission(record, actor, is_admin=is_admin)):
        raise ProjectPermissionError("Somente o proprietário ou aprovador pode alterar os participantes.")
    clean: list[dict[str, str]] = []
    seen: set[str] = set()
    owner = str(record.get("owner_username") or "").strip().lower()
    for item in members:
        username = str(item.get("username") or "").strip().lower()
        level = str(item.get("level") or "viewer")
        if username and username != owner and username not in seen and level in FLOW_ACCESS_LEVELS:
            clean.append({"username": username, "level": level})
            seen.add(username)
    now = utc_now()
    _collection(PROJECTS_COLLECTION).update_one(
        {"_id": str(project_id)},
        {"$set": {"members": clean, "updated_at": now, "last_saved_by": actor.strip().lower()}},
    )
    refreshed = {**record, "members": clean}
    _sync_project_access_to_flows(project_id, refreshed)
    members_collection = _collection(PROJECT_MEMBERS_COLLECTION)
    members_collection.delete_many({"project_id": str(project_id)})
    if clean:
        members_collection.insert_many([
            {"project_id": str(project_id), "username": item["username"], "level": item["level"], "updated_at": now}
            for item in clean
        ])
    db.add_log(actor, "Alterou participantes do projeto", {"project_id": project_id, "members": clean})
    return clean


def list_project_flows(project_id: str, username: str, *, is_admin: bool = False, include_documents: bool = False) -> list[dict[str, Any]]:
    project = get_project(project_id, username, is_admin=is_admin)
    if not project:
        return []
    flows = list_flowcharts(username, include_all=is_admin, project_id=project_id)
    flows.sort(key=lambda item: (int(item.get("project_order") or 0), item.get("name", "")))
    if include_documents:
        detailed: list[dict[str, Any]] = []
        for item in flows:
            record = get_flowchart(item["id"], actor_username=username, is_admin=is_admin)
            if record:
                detailed.append(record)
        return detailed
    return flows


def assign_flow_to_project(
    project_id: str,
    flow_id: str,
    actor: str,
    *,
    role: str = "subprocess",
    group: str = "",
    order: int = 0,
    is_admin: bool = False,
) -> dict[str, Any]:
    project = get_project(project_id, actor, is_admin=is_admin)
    if not project or not can_edit_project(project.get("permission")):
        raise ProjectPermissionError("Seu perfil não possui permissão para adicionar fluxos ao projeto.")
    flow = get_flowchart(flow_id, actor_username=actor, is_admin=is_admin)
    if not flow or not can_edit(flow.get("permission")):
        raise ProjectPermissionError("Você não possui permissão para alterar o fluxo selecionado.")
    final_role = role if role in PROJECT_ROLES else "subprocess"
    document = deepcopy(flow["document"])
    document.setdefault("flow", {}).update({
        "projectId": str(project_id),
        "projectRole": final_role,
        "projectGroup": str(group or ""),
        "projectOrder": int(order or 0),
    })
    saved = save_flowchart(
        document,
        flow["owner_username"],
        flow.get("owner_email", ""),
        expected_revision=flow["revision"],
        actor_username=actor,
        is_admin=is_admin,
        save_reason="project_assignment",
    )
    _sync_project_access_to_flows(project_id, project)
    if not project.get("default_flow_id") or final_role == "executive":
        update_project(project_id, actor, default_flow_id=flow_id, is_admin=is_admin)
    return saved


def detach_flow_from_project(project_id: str, flow_id: str, actor: str, *, is_admin: bool = False) -> bool:
    project = get_project(project_id, actor, is_admin=is_admin)
    if not project or not can_edit_project(project.get("permission")):
        raise ProjectPermissionError("Seu perfil não possui permissão para remover fluxos do projeto.")
    flow = get_flowchart(flow_id, actor_username=actor, is_admin=is_admin)
    if not flow or str(flow.get("project_id") or "") != str(project_id):
        return False
    document = deepcopy(flow["document"])
    for key in ("projectId", "projectRole", "projectGroup", "projectOrder"):
        document.setdefault("flow", {}).pop(key, None)
    save_flowchart(
        document,
        flow["owner_username"],
        flow.get("owner_email", ""),
        expected_revision=flow["revision"],
        actor_username=actor,
        is_admin=is_admin,
        save_reason="project_detach",
    )
    if project.get("default_flow_id") == flow_id:
        remaining = [item for item in list_project_flows(project_id, actor, is_admin=is_admin) if item["id"] != flow_id]
        update_project(project_id, actor, default_flow_id=remaining[0]["id"] if remaining else "", is_admin=is_admin)
    return True


def _node_ids(document: dict[str, Any]) -> set[str]:
    return {str(node.get("id") or "") for node in document.get("nodes", [])}


def project_links(project_id: str, username: str, *, is_admin: bool = False) -> dict[str, Any]:
    records = list_project_flows(project_id, username, is_admin=is_admin, include_documents=True)
    by_id = {item["id"]: item for item in records}
    links: list[dict[str, Any]] = []
    broken: list[dict[str, Any]] = []
    for record in records:
        source_id = record["id"]
        for node in record.get("document", {}).get("nodes", []):
            data = node.get("data") or {}
            target_id = str(data.get("linkedFlowId") or "").strip()
            if not target_id:
                continue
            link = {
                "source_flow_id": source_id,
                "source_flow_name": record["name"],
                "source_node_id": str(node.get("id") or ""),
                "source_node_label": str(data.get("label") or "Subprocesso"),
                "target_flow_id": target_id,
                "entry_node_id": str(data.get("linkedFlowEntryNodeId") or ""),
                "exit_node_id": str(data.get("linkedFlowExitNodeId") or ""),
            }
            target = by_id.get(target_id)
            reasons: list[str] = []
            if not target:
                reasons.append("fluxo vinculado ausente no projeto")
            else:
                ids = _node_ids(target.get("document") or {})
                if link["entry_node_id"] and link["entry_node_id"] not in ids:
                    reasons.append("nó de entrada inexistente")
                if link["exit_node_id"] and link["exit_node_id"] not in ids:
                    reasons.append("nó de saída inexistente")
            if target_id == source_id:
                reasons.append("referência ao próprio fluxo")
            if reasons:
                broken.append({**link, "reasons": reasons})
            links.append(link)
    return {"flows": records, "by_id": by_id, "links": links, "broken": broken}


def _project_cycles(flow_ids: set[str], links: list[dict[str, Any]]) -> list[list[str]]:
    adjacency: dict[str, list[str]] = {flow_id: [] for flow_id in flow_ids}
    for link in links:
        source, target = link["source_flow_id"], link["target_flow_id"]
        if source in flow_ids and target in flow_ids:
            adjacency[source].append(target)
    cycles: list[list[str]] = []
    visiting: set[str] = set()
    visited: set[str] = set()
    stack: list[str] = []

    def visit(node: str) -> None:
        if node in visiting:
            if node in stack:
                index = stack.index(node)
                cycle = stack[index:] + [node]
                if cycle not in cycles:
                    cycles.append(cycle)
            return
        if node in visited:
            return
        visiting.add(node)
        stack.append(node)
        for neighbor in adjacency.get(node, []):
            visit(neighbor)
        stack.pop()
        visiting.remove(node)
        visited.add(node)

    for flow_id in sorted(flow_ids):
        visit(flow_id)
    return cycles


def analyze_project(project_id: str, username: str, *, is_admin: bool = False) -> dict[str, Any]:
    graph = project_links(project_id, username, is_admin=is_admin)
    records = graph["flows"]
    flow_ids = set(graph["by_id"])
    incoming = {flow_id: 0 for flow_id in flow_ids}
    outgoing = {flow_id: 0 for flow_id in flow_ids}
    for link in graph["links"]:
        source, target = link["source_flow_id"], link["target_flow_id"]
        if source in outgoing:
            outgoing[source] += 1
        if target in incoming:
            incoming[target] += 1
    orphans = [flow_id for flow_id in flow_ids if incoming[flow_id] == 0 and outgoing[flow_id] == 0]
    roots = [flow_id for flow_id in flow_ids if incoming[flow_id] == 0]
    cycles = _project_cycles(flow_ids, graph["links"])
    quality_rows: list[dict[str, Any]] = []
    total_nodes = total_edges = total_issues = 0
    for record in records:
        analysis = analyze_document(record["document"])
        total_nodes += len(record["document"].get("nodes", []))
        total_edges += len(record["document"].get("edges", []))
        issue_count = sum(len(value) for value in analysis.get("issues", {}).values() if isinstance(value, list))
        total_issues += issue_count
        quality_rows.append({
            "flow_id": record["id"],
            "name": record["name"],
            "quality_score": analysis.get("quality_score", 0),
            "issues": issue_count,
            "nodes": len(record["document"].get("nodes", [])),
            "edges": len(record["document"].get("edges", [])),
        })
    average_quality = round(sum(item["quality_score"] for item in quality_rows) / len(quality_rows)) if quality_rows else 0
    score = max(0, min(100, average_quality - len(graph["broken"]) * 8 - len(cycles) * 10 - len(orphans) * 2))
    return {
        "flow_count": len(records),
        "node_count": total_nodes,
        "edge_count": total_edges,
        "link_count": len(graph["links"]),
        "broken_links": graph["broken"],
        "broken_count": len(graph["broken"]),
        "cycles": cycles,
        "cycle_count": len(cycles),
        "orphans": orphans,
        "roots": roots,
        "quality_rows": quality_rows,
        "average_quality": average_quality,
        "quality_score": score,
        "issue_count": total_issues,
    }


def search_project(project_id: str, username: str, query: str, *, is_admin: bool = False, limit: int = 100) -> list[dict[str, Any]]:
    needle = str(query or "").strip().lower()
    if not needle:
        return []
    results: list[dict[str, Any]] = []
    for record in list_project_flows(project_id, username, is_admin=is_admin, include_documents=True):
        document = record["document"]
        flow_text = " ".join([
            record["name"], record.get("description", ""), " ".join(record.get("tags") or [])
        ]).lower()
        if needle in flow_text:
            results.append({"kind": "flow", "flow_id": record["id"], "flow_name": record["name"], "target_id": record["id"], "label": record["name"], "context": record.get("description", "")})
        lane_names = {str(item.get("id")): str(item.get("name") or "") for item in document.get("lanes", [])}
        for node in document.get("nodes", []):
            data = node.get("data") or {}
            text = " ".join([
                str(data.get("label") or ""), str(data.get("description") or ""),
                str(data.get("owner") or ""), " ".join(map(str, data.get("tags") or [])),
                str(node.get("id") or ""), lane_names.get(str(node.get("laneId") or ""), ""),
            ]).lower()
            if needle in text:
                results.append({
                    "kind": "node", "flow_id": record["id"], "flow_name": record["name"],
                    "target_id": str(node.get("id") or ""), "label": str(data.get("label") or node.get("id")),
                    "context": f"{lane_names.get(str(node.get('laneId') or ''), 'Sem raia')} · {data.get('owner') or 'Sem responsável'}",
                })
            if len(results) >= limit:
                return results
    return results[:limit]


def shortest_project_path(project_id: str, username: str, source_flow_id: str, target_flow_id: str, *, is_admin: bool = False) -> list[str]:
    graph = project_links(project_id, username, is_admin=is_admin)
    flow_ids = set(graph["by_id"])
    source, target = str(source_flow_id), str(target_flow_id)
    if source not in flow_ids or target not in flow_ids:
        return []
    adjacency: dict[str, list[str]] = {item: [] for item in flow_ids}
    for link in graph["links"]:
        if link["source_flow_id"] in adjacency and link["target_flow_id"] in flow_ids:
            adjacency[link["source_flow_id"]].append(link["target_flow_id"])
    queue: list[tuple[str, list[str]]] = [(source, [source])]
    visited: set[str] = set()
    while queue:
        current, path = queue.pop(0)
        if current == target:
            return path
        if current in visited:
            continue
        visited.add(current)
        for neighbor in adjacency.get(current, []):
            if neighbor not in visited:
                queue.append((neighbor, [*path, neighbor]))
    return []


def project_impact(project_id: str, username: str, changed_flow_id: str, *, is_admin: bool = False) -> list[dict[str, Any]]:
    graph = project_links(project_id, username, is_admin=is_admin)
    return [
        link for link in graph["links"]
        if link["target_flow_id"] == str(changed_flow_id)
    ]


def remove_project_flow_references(
    project_id: str,
    target_flow_id: str,
    actor: str,
    *,
    is_admin: bool = False,
) -> int:
    """Remove referências de subprocessos para um fluxo que será excluído."""
    changed = 0
    for record in list_project_flows(project_id, actor, is_admin=is_admin, include_documents=True):
        if record["id"] == str(target_flow_id):
            continue
        document = deepcopy(record.get("document") or {})
        touched = False
        for node in document.get("nodes", []):
            data = node.get("data") or {}
            if str(data.get("linkedFlowId") or "") != str(target_flow_id):
                continue
            data["linkedFlowId"] = None
            data["linkedFlowEntryNodeId"] = None
            data["linkedFlowExitNodeId"] = None
            touched = True
        if not touched:
            continue
        save_flowchart(
            document,
            record["owner_username"],
            record.get("owner_email", ""),
            expected_revision=record["revision"],
            actor_username=actor,
            is_admin=is_admin,
            save_reason="remove_deleted_flow_reference",
        )
        changed += 1
    return changed


def create_project_release(project_id: str, actor: str, *, name: str = "", notes: str = "", is_admin: bool = False) -> dict[str, Any]:
    project = get_project(project_id, actor, is_admin=is_admin)
    if not project or not can_manage_project(project.get("permission")):
        raise ProjectPermissionError("Somente o proprietário ou aprovador pode criar releases.")
    flows = list_project_flows(project_id, actor, is_admin=is_admin)
    if not flows:
        raise ValueError("O projeto não possui fluxos para publicar.")
    analysis = analyze_project(project_id, actor, is_admin=is_admin)
    if analysis["broken_count"]:
        raise ValueError("Corrija os vínculos quebrados antes de criar a release.")
    releases = _collection(PROJECT_RELEASES_COLLECTION)
    latest = releases.find_one({"project_id": str(project_id)}, sort=[("version", DESCENDING)])
    version = int((latest or {}).get("version") or 0) + 1
    now = utc_now()
    snapshot = [{
        "flow_id": item["id"],
        "name": item["name"],
        "revision": int(item.get("revision") or 1),
        "version": int(item.get("current_version") or 1),
        "document_hash": item.get("document_hash", ""),
        "role": item.get("project_role", ""),
        "group": item.get("project_group", ""),
        "order": int(item.get("project_order") or 0),
    } for item in flows]
    record = {
        "project_id": str(project_id), "version": version,
        "name": str(name or f"{project['name']} {version}.0"), "notes": str(notes or ""),
        "flows": snapshot, "quality_score": analysis["quality_score"],
        "created_by": actor.strip().lower(), "created_at": now,
    }
    releases.insert_one(record)
    release_flow_documents = []
    for item in flows:
        flow_record = get_flowchart(item["id"], actor_username=actor, is_admin=is_admin)
        if flow_record:
            release_flow_documents.append({
                "project_id": str(project_id),
                "release_version": version,
                "flow_id": item["id"],
                "flow_version": int(item.get("current_version") or 1),
                "flow_revision": int(item.get("revision") or 1),
                "document_hash": item.get("document_hash", ""),
                "document": deepcopy(flow_record.get("document") or {}),
                "created_at": now,
            })
    if release_flow_documents:
        _collection(PROJECT_RELEASE_FLOWS_COLLECTION).insert_many(release_flow_documents)
    _collection(PROJECTS_COLLECTION).update_one(
        {"_id": str(project_id)},
        {"$set": {"current_release": version, "status": "published", "updated_at": now, "last_saved_by": actor.strip().lower()}},
    )
    db.add_log(actor, "Criou release do projeto", {"project_id": project_id, "release": version, "flows": len(snapshot)})
    record.pop("_id", None)
    return record


def list_project_releases(project_id: str, username: str, *, is_admin: bool = False) -> list[dict[str, Any]]:
    if not get_project(project_id, username, is_admin=is_admin):
        return []
    return list(_collection(PROJECT_RELEASES_COLLECTION).find({"project_id": str(project_id)}, {"_id": 0}).sort("version", DESCENDING))


def _project_manifest(project: dict[str, Any], flows: list[dict[str, Any]], release: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "schemaVersion": "1.0.0",
        "project": {
            "id": project["id"], "name": project["name"], "code": project.get("code", ""),
            "description": project.get("description", ""), "status": project.get("status", "draft"),
            "owner": project.get("owner_username", ""), "defaultFlowId": project.get("default_flow_id", ""),
            "tags": project.get("tags", []), "exportedAt": utc_now_iso(),
        },
        "release": deepcopy(release) if release else None,
        "flows": [{
            "flowId": item["id"], "name": item["name"], "role": item.get("project_role", "subprocess"),
            "group": item.get("project_group", ""), "order": int(item.get("project_order") or 0),
            "revision": int(item.get("revision") or 1), "version": int(item.get("current_version") or 1),
            "file": f"flows/{item['id']}.json",
        } for item in flows],
    }


def export_project_bundle(project_id: str, username: str, *, is_admin: bool = False, release_version: int | None = None) -> bytes:
    project = get_project(project_id, username, is_admin=is_admin)
    if not project:
        raise ValueError("Projeto não encontrado.")
    current_flows = list_project_flows(project_id, username, is_admin=is_admin)
    release = None
    export_flows = current_flows
    release_documents: dict[str, dict[str, Any]] = {}
    if release_version is not None:
        release = _collection(PROJECT_RELEASES_COLLECTION).find_one(
            {"project_id": str(project_id), "version": int(release_version)}, {"_id": 0}
        )
        if not release:
            raise ValueError("Release do projeto não encontrada.")
        release_snapshots = list(_collection(PROJECT_RELEASE_FLOWS_COLLECTION).find(
            {"project_id": str(project_id), "release_version": int(release_version)}, {"_id": 0}
        ))
        release_documents = {str(item.get("flow_id")): deepcopy(item.get("document") or {}) for item in release_snapshots}
        export_flows = [{
            "id": item["flow_id"], "name": item.get("name") or item["flow_id"],
            "project_role": item.get("role", ""), "project_group": item.get("group", ""),
            "project_order": int(item.get("order") or 0), "revision": int(item.get("revision") or 1),
            "current_version": int(item.get("version") or 1), "workflow_status": "published",
        } for item in release.get("flows", [])]
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        manifest = _project_manifest(project, export_flows, release)
        archive.writestr("project.json", json.dumps(manifest, ensure_ascii=False, indent=2, default=str))
        for flow in export_flows:
            if release is not None:
                document = release_documents.get(flow["id"])
                if document is None:
                    document = get_version(flow["id"], int(flow.get("current_version") or 1))
            else:
                record = get_flowchart(flow["id"], actor_username=username, is_admin=is_admin)
                document = record.get("document") if record else None
            if document:
                archive.writestr(f"flows/{flow['id']}.json", json.dumps(document, ensure_ascii=False, indent=2))
        archive.writestr("README.txt", (
            f"Projeto: {project['name']}\n"
            f"Fluxos: {len(export_flows)}\n"
            "Importe este arquivo ZIP pela tela Gestão de Projetos do Produto Tools.\n"
        ))
    return buffer.getvalue()


def _load_bundle(payload: bytes) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    try:
        with zipfile.ZipFile(io.BytesIO(payload), "r") as archive:
            names = set(archive.namelist())
            if "project.json" not in names:
                raise ProjectImportError("O pacote não contém project.json.")
            manifest = json.loads(archive.read("project.json").decode("utf-8-sig"))
            documents = []
            for item in manifest.get("flows", []):
                filename = str(item.get("file") or f"flows/{item.get('flowId')}.json")
                if filename not in names:
                    raise ProjectImportError(f"Arquivo de fluxo ausente no pacote: {filename}")
                documents.append(json.loads(archive.read(filename).decode("utf-8-sig")))
            if not documents:
                for filename in sorted(name for name in names if name.startswith("flows/") and name.endswith(".json")):
                    documents.append(json.loads(archive.read(filename).decode("utf-8-sig")))
            return manifest, documents
    except zipfile.BadZipFile as exc:
        raise ProjectImportError("O arquivo informado não é um ZIP de projeto válido.") from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProjectImportError("O pacote contém JSON inválido.") from exc


def import_project_bundle(
    payload: bytes,
    owner_username: str,
    owner_email: str = "",
    *,
    preserve_ids: bool = True,
    is_admin: bool = False,
) -> dict[str, Any]:
    manifest, raw_documents = _load_bundle(payload)
    project_data = manifest.get("project") or {}
    base_project_id = str(project_data.get("id") or f"project_{uuid4().hex[:12]}")
    existing_project = _collection(PROJECTS_COLLECTION).find_one({"_id": base_project_id}, {"_id": 1})
    project_id = base_project_id if preserve_ids and not existing_project else f"project_{uuid4().hex[:12]}"
    project = create_project(
        str(project_data.get("name") or "Projeto importado"),
        str(project_data.get("description") or ""),
        owner_username,
        owner_email,
        code=str(project_data.get("code") or ""),
        project_id=project_id,
        tags=list(project_data.get("tags") or []),
    )
    manifest_flows = {str(item.get("flowId") or ""): item for item in manifest.get("flows", [])}
    normalized_docs: list[dict[str, Any]] = []
    import_warnings: list[str] = []
    for raw_document in raw_documents:
        repaired, warnings = repair_import_document(raw_document, owner_username)
        errors = validate_document(repaired, strict=False)
        if errors:
            raise ProjectImportError("Documento inválido: " + " | ".join(errors[:20]))
        normalized_docs.append(repaired)
        import_warnings.extend(warnings)
    id_map: dict[str, str] = {}
    for document in normalized_docs:
        old_id = str(document["flow"]["id"])
        existing = _collection(FLOWCHARTS_COLLECTION).find_one({"_id": old_id}, {"_id": 1})
        id_map[old_id] = old_id if preserve_ids and not existing else f"flow_{uuid4().hex[:12]}"
    for document in normalized_docs:
        old_id = str(document["flow"]["id"])
        document["flow"]["id"] = id_map[old_id]
        for node in document.get("nodes", []):
            linked = str((node.get("data") or {}).get("linkedFlowId") or "")
            if linked in id_map:
                node["data"]["linkedFlowId"] = id_map[linked]
        meta = manifest_flows.get(old_id, {})
        document["flow"].update({
            "projectId": project_id,
            "projectRole": str(meta.get("role") or "subprocess"),
            "projectGroup": str(meta.get("group") or ""),
            "projectOrder": int(meta.get("order") or 0),
        })
        save_flowchart(document, owner_username, owner_email, actor_username=owner_username, is_admin=is_admin, save_reason="project_import")
    default_old = str(project_data.get("defaultFlowId") or "")
    default_new = id_map.get(default_old, "")
    if not default_new:
        executive = next((doc["flow"]["id"] for doc in normalized_docs if doc["flow"].get("projectRole") == "executive"), "")
        default_new = executive or (normalized_docs[0]["flow"]["id"] if normalized_docs else "")
    update_project(project_id, owner_username, default_flow_id=default_new, is_admin=is_admin)
    return {
        "project": get_project(project_id, owner_username, is_admin=is_admin),
        "flow_ids": list(id_map.values()),
        "id_map": id_map,
        "warnings": import_warnings,
    }


def import_documents_as_project(
    documents: Iterable[dict[str, Any]],
    project_name: str,
    project_description: str,
    owner_username: str,
    owner_email: str = "",
) -> dict[str, Any]:
    docs: list[dict[str, Any]] = []
    import_warnings: list[str] = []
    for raw_document in documents:
        repaired, warnings = repair_import_document(raw_document, owner_username)
        errors = validate_document(repaired, strict=False)
        if errors:
            raise ProjectImportError("Documento inválido: " + " | ".join(errors[:20]))
        docs.append(repaired)
        import_warnings.extend(warnings)
    if not docs:
        raise ValueError("Nenhum fluxo foi informado para importação.")
    project = create_project(project_name, project_description, owner_username, owner_email)
    project_id = project["id"]
    id_map: dict[str, str] = {}
    for document in docs:
        old_id = str(document["flow"]["id"])
        exists = _collection(FLOWCHARTS_COLLECTION).find_one({"_id": old_id}, {"_id": 1})
        id_map[old_id] = f"flow_{uuid4().hex[:12]}" if exists or old_id in id_map.values() else old_id
    for document in docs:
        old_id = str(document["flow"]["id"])
        document["flow"]["id"] = id_map[old_id]
        for node in document.get("nodes", []):
            linked = str((node.get("data") or {}).get("linkedFlowId") or "")
            if linked in id_map:
                node["data"]["linkedFlowId"] = id_map[linked]
    for index, document in enumerate(docs, start=1):
        document["flow"].update({
            "projectId": project_id,
            "projectRole": "executive" if index == 1 else ("operational" if index == 2 else "subprocess"),
            "projectGroup": "",
            "projectOrder": index,
        })
        save_flowchart(document, owner_username, owner_email, actor_username=owner_username, save_reason="project_multi_import")
    update_project(project_id, owner_username, default_flow_id=docs[0]["flow"]["id"])
    result = get_project(project_id, owner_username) or project
    result["import_warnings"] = import_warnings
    return result


def delete_project(project_id: str, actor: str, *, delete_flows: bool = False, is_admin: bool = False) -> bool:
    record = _collection(PROJECTS_COLLECTION).find_one({"_id": str(project_id)})
    if not record or project_permission(record, actor, is_admin=is_admin) != "owner":
        raise ProjectPermissionError("Somente o proprietário pode excluir o projeto.")
    flows = list_project_flows(project_id, actor, is_admin=is_admin, include_documents=True)
    if delete_flows:
        from services.flowchart_repository import delete_flowchart
        for flow in flows:
            delete_flowchart(flow["id"], actor, is_admin=is_admin)
    else:
        for flow in flows:
            document = deepcopy(flow["document"])
            for key in ("projectId", "projectRole", "projectGroup", "projectOrder"):
                document.setdefault("flow", {}).pop(key, None)
            save_flowchart(
                document, flow["owner_username"], flow.get("owner_email", ""),
                expected_revision=flow["revision"], actor_username=actor,
                is_admin=is_admin, save_reason="project_deleted_detach",
            )
    _collection(PROJECT_RELEASES_COLLECTION).delete_many({"project_id": str(project_id)})
    _collection(PROJECT_RELEASE_FLOWS_COLLECTION).delete_many({"project_id": str(project_id)})
    _collection(PROJECT_MEMBERS_COLLECTION).delete_many({"project_id": str(project_id)})
    deleted = _collection(PROJECTS_COLLECTION).delete_one({"_id": str(project_id)}).deleted_count > 0
    if deleted:
        db.add_log(actor, "Excluiu projeto no Produto Tools", {"project_id": project_id, "delete_flows": delete_flows})
    return deleted
