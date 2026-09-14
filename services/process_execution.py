from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import uuid4

from pymongo import ASCENDING, DESCENDING

import database as db
from database import utc_now
from services.flowchart_repository import get_flowchart
from services.event_bus import emit_event

FORMS = "produto_tools_forms"
INSTANCES = "produto_tools_instances"
TASKS = "produto_tools_tasks"

def _collection(name: str):
    collection = db.get_collection(name)
    if collection is None:
        raise RuntimeError("MongoDB indisponível.")
    return collection

def initialize_execution_tables() -> None:
    _collection(FORMS).create_index([("id", ASCENDING)], unique=True, name="uq_pt_form_id")
    _collection(FORMS).create_index([("flow_id", ASCENDING), ("updated_at", DESCENDING)], name="ix_pt_form_flow")
    _collection(INSTANCES).create_index([("id", ASCENDING)], unique=True, name="uq_pt_instance_id")
    _collection(INSTANCES).create_index([("flow_id", ASCENDING), ("status", ASCENDING), ("updated_at", DESCENDING)], name="ix_pt_instance_flow_status")
    _collection(TASKS).create_index([("id", ASCENDING)], unique=True, name="uq_pt_task_id")
    _collection(TASKS).create_index([("assignee", ASCENDING), ("status", ASCENDING), ("due_at", ASCENDING)], name="ix_pt_task_assignee_status")

def save_form(flow_id: str, name: str, fields: list[dict[str, Any]], actor: str, form_id: str = "") -> dict[str, Any]:
    now = utc_now()
    identifier = form_id or f"form_{uuid4().hex[:12]}"
    record = {
        "id": identifier, "flow_id": flow_id, "name": name.strip() or "Formulário",
        "fields": fields, "updated_by": actor, "updated_at": now,
    }
    current = _collection(FORMS).find_one({"id": identifier})
    if current:
        _collection(FORMS).update_one({"id": identifier}, {"$set": record})
    else:
        record.update({"created_by": actor, "created_at": now})
        _collection(FORMS).insert_one(record)
    record.pop("_id", None)
    return record

def list_forms(flow_id: str = "") -> list[dict[str, Any]]:
    query = {"flow_id": flow_id} if flow_id else {}
    return list(_collection(FORMS).find(query, {"_id": 0}).sort("updated_at", DESCENDING))

def _graph(document: dict[str, Any]):
    nodes = [n for n in document.get("nodes", []) if (n.get("data") or {}).get("enabled", True)]
    node_map = {str(n["id"]): n for n in nodes}
    outgoing: dict[str, list[str]] = {}
    for edge in document.get("edges", []):
        if not edge.get("enabled", True):
            continue
        outgoing.setdefault(str(edge.get("source")), []).append(str(edge.get("target")))
    starts = [str(n["id"]) for n in nodes if n.get("type") == "start"]
    return node_map, outgoing, starts

def _first_work_node(node_map: dict[str, dict], outgoing: dict[str, list[str]], start: str) -> str:
    current = start
    for _ in range(len(node_map) + 3):
        node = node_map.get(current)
        if not node:
            return current
        if node.get("type") not in {"start", "note"}:
            return current
        next_ids = outgoing.get(current) or []
        if not next_ids:
            return current
        current = next_ids[0]
    return current

def _create_task(instance: dict[str, Any], node: dict[str, Any], actor: str) -> dict[str, Any]:
    data = node.get("data") or {}
    sla = float(data.get("slaMinutes") or 0)
    now = utc_now()
    due = now + timedelta(minutes=sla) if sla > 0 else None
    task = {
        "id": f"task_{uuid4().hex[:12]}",
        "instance_id": instance["id"],
        "flow_id": instance["flow_id"],
        "node_id": str(node.get("id")),
        "label": str(data.get("label") or node.get("id")),
        "assignee": str(data.get("owner") or actor),
        "status": "open",
        "due_at": due,
        "created_at": now,
        "updated_at": now,
    }
    _collection(TASKS).insert_one(task)
    task.pop("_id", None)
    emit_event("task.created", {"task": task, "instance_id": instance["id"], "flow_id": instance["flow_id"]})
    return task

def start_instance(flow_id: str, actor: str, *, form_data: dict[str, Any] | None = None, is_admin: bool = False) -> dict[str, Any]:
    record = get_flowchart(flow_id, actor_username=actor, is_admin=is_admin)
    if not record:
        raise ValueError("Fluxo não encontrado ou sem acesso.")
    node_map, outgoing, starts = _graph(record["document"])
    if not node_map:
        raise ValueError("Fluxo sem etapas.")
    start = starts[0] if starts else next(iter(node_map))
    first = _first_work_node(node_map, outgoing, start)
    now = utc_now()
    instance = {
        "id": f"inst_{uuid4().hex[:12]}",
        "flow_id": flow_id,
        "flow_name": record["name"],
        "status": "running",
        "started_by": actor,
        "started_at": now,
        "updated_at": now,
        "current_node_id": first,
        "form_data": form_data or {},
        "history": [{"node_id": start, "at": now, "actor": actor, "event": "started"}],
    }
    _collection(INSTANCES).insert_one(instance)
    emit_event("process.started", {"instance_id": instance["id"], "flow_id": flow_id, "started_by": actor})
    node = node_map.get(first)
    if node and node.get("type") != "end":
        _create_task(instance, node, actor)
    elif node and node.get("type") == "end":
        _collection(INSTANCES).update_one({"id": instance["id"]}, {"$set": {"status": "completed", "completed_at": now}})
        instance["status"] = "completed"
    instance.pop("_id", None)
    return instance

def list_tasks(username: str, *, include_all: bool = False, status: str = "open") -> list[dict[str, Any]]:
    query: dict[str, Any] = {"status": status} if status else {}
    if not include_all:
        query["assignee"] = username
    return list(_collection(TASKS).find(query, {"_id": 0}).sort([("due_at", ASCENDING), ("created_at", ASCENDING)]))

def complete_task(task_id: str, actor: str, *, next_node_id: str = "", is_admin: bool = False) -> dict[str, Any]:
    task = _collection(TASKS).find_one({"id": task_id, "status": "open"})
    if not task:
        raise ValueError("Tarefa não encontrada ou já concluída.")
    instance = _collection(INSTANCES).find_one({"id": task["instance_id"]})
    if not instance:
        raise ValueError("Instância não encontrada.")
    record = get_flowchart(instance["flow_id"], actor_username=actor, is_admin=is_admin)
    if not record:
        raise ValueError("Fluxo não encontrado.")
    node_map, outgoing, _ = _graph(record["document"])
    candidates = outgoing.get(str(task["node_id"])) or []
    target = next_node_id if next_node_id in candidates else (candidates[0] if len(candidates) == 1 else "")
    if candidates and not target:
        return {"needs_choice": True, "candidates": [{"id": item, "label": str((node_map.get(item, {}).get("data") or {}).get("label") or item)} for item in candidates]}
    now = utc_now()
    _collection(TASKS).update_one({"id": task_id}, {"$set": {"status": "completed", "completed_by": actor, "completed_at": now, "updated_at": now}})
    history_item = {"node_id": str(task["node_id"]), "at": now, "actor": actor, "event": "completed"}
    if not target:
        _collection(INSTANCES).update_one({"id": instance["id"]}, {"$set": {"status": "completed", "completed_at": now, "updated_at": now}, "$push": {"history": history_item}})
        emit_event("process.completed", {"instance_id": instance["id"], "flow_id": instance["flow_id"], "completed_by": actor})
        return {"needs_choice": False, "completed": True}
    target_node = node_map.get(target)
    while target_node and target_node.get("type") in {"note", "start"}:
        nexts = outgoing.get(target) or []
        if not nexts:
            break
        target = nexts[0]
        target_node = node_map.get(target)
    if target_node and target_node.get("type") == "end":
        _collection(INSTANCES).update_one(
            {"id": instance["id"]},
            {"$set": {"status": "completed", "completed_at": now, "updated_at": now, "current_node_id": target}, "$push": {"history": history_item}},
        )
        emit_event("task.completed", {"task_id": task_id, "instance_id": instance["id"], "flow_id": instance["flow_id"], "completed_by": actor})
        emit_event("process.completed", {"instance_id": instance["id"], "flow_id": instance["flow_id"], "completed_by": actor})
        return {"needs_choice": False, "completed": True}
    emit_event("task.completed", {"task_id": task_id, "instance_id": instance["id"], "flow_id": instance["flow_id"], "completed_by": actor})
    _collection(INSTANCES).update_one(
        {"id": instance["id"]},
        {"$set": {"updated_at": now, "current_node_id": target}, "$push": {"history": history_item}},
    )
    instance["current_node_id"] = target
    if target_node:
        _create_task(instance, target_node, actor)
    return {"needs_choice": False, "completed": False, "next_node_id": target}

def list_instances(flow_id: str = "", *, status: str = "") -> list[dict[str, Any]]:
    query: dict[str, Any] = {}
    if flow_id:
        query["flow_id"] = flow_id
    if status:
        query["status"] = status
    return list(_collection(INSTANCES).find(query, {"_id": 0}).sort("started_at", DESCENDING).limit(1000))

def cockpit_metrics() -> dict[str, Any]:
    now = utc_now()
    open_tasks = list(_collection(TASKS).find({"status": "open"}, {"_id": 0, "due_at": 1}))
    overdue = sum(1 for task in open_tasks if task.get("due_at") and task["due_at"] < now)
    running = _collection(INSTANCES).count_documents({"status": "running"})
    completed = _collection(INSTANCES).count_documents({"status": "completed"})
    return {"open_tasks": len(open_tasks), "overdue_tasks": overdue, "running_instances": running, "completed_instances": completed}