from __future__ import annotations

from collections import Counter, defaultdict, deque
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from jsonschema import Draft202012Validator

SCHEMA_VERSION = "2.0.0"
NODE_TYPES = {
    "start", "end", "task", "decision", "subprocess", "event", "wait",
    "document", "api", "note",
}
FLOW_STATUSES = {"draft", "in_review", "approved", "published", "archived", "active"}

FLOWCHART_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["schemaVersion", "flow", "settings", "viewport", "lanes", "nodes", "edges"],
    "properties": {
        "schemaVersion": {"type": "string"},
        "flow": {
            "type": "object",
            "required": ["id", "name", "status"],
            "properties": {
                "id": {"type": "string", "minLength": 1},
                "name": {"type": "string", "minLength": 1},
                "description": {"type": "string"},
                "status": {"type": "string"},
                "orientation": {"enum": ["LR", "TB", "RL", "BT"]},
                "tags": {"type": "array", "items": {"type": "string"}},
            },
            "additionalProperties": True,
        },
        "settings": {"type": "object"},
        "viewport": {"type": "object"},
        "lanes": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "name"],
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string", "minLength": 1},
                    "owner": {"type": "string"},
                    "enabled": {"type": "boolean"},
                },
                "additionalProperties": True,
            },
        },
        "nodes": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "type", "position", "data"],
                "properties": {
                    "id": {"type": "string"},
                    "type": {"type": "string"},
                    "laneId": {"type": ["string", "null"]},
                    "position": {
                        "type": "object",
                        "required": ["x", "y"],
                        "properties": {"x": {"type": "number"}, "y": {"type": "number"}},
                    },
                    "data": {"type": "object", "required": ["label", "enabled"]},
                },
                "additionalProperties": True,
            },
        },
        "edges": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "source", "target", "enabled"],
                "properties": {
                    "id": {"type": "string"},
                    "source": {"type": "string"},
                    "target": {"type": "string"},
                    "sourceHandle": {"type": "string"},
                    "targetHandle": {"type": "string"},
                    "enabled": {"type": "boolean"},
                },
                "additionalProperties": True,
            },
        },
    },
    "additionalProperties": True,
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_node_data(label: str) -> dict[str, Any]:
    return {
        "label": label,
        "description": "",
        "owner": "",
        "enabled": True,
        "locked": False,
        "slaMinutes": None,
        "tags": [],
        "level": "operational",
        "category": "process",
        "criticality": "medium",
        "linkedFlowId": None,
        "linkedFlowEntryNodeId": None,
        "linkedFlowExitNodeId": None,
        "preferredEdgeId": None,
        "documentationUrl": "",
        "raci": {"responsible": "", "accountable": "", "consulted": [], "informed": []},
    }


def new_flowchart_document(name: str = "Novo processo", owner_email: str = "") -> dict[str, Any]:
    flow_id = f"flow_{uuid4().hex[:12]}"
    created = now_iso()
    return {
        "schemaVersion": SCHEMA_VERSION,
        "flow": {
            "id": flow_id,
            "name": name,
            "description": "",
            "status": "draft",
            "orientation": "LR",
            "createdAt": created,
            "updatedAt": created,
            "createdBy": owner_email,
            "tags": [],
        },
        "settings": {
            "snapToGrid": True,
            "gridSize": 20,
            "autoLayout": False,
            "showMiniMap": True,
            "showGrid": True,
            "layoutPreset": "readable",
            "edgeRouting": "smooth",
            "autosaveSeconds": 10,
            "interactivePlayback": True,
        },
        "viewport": {"x": 0, "y": 0, "zoom": 1},
        "lanes": [{
            "id": "lane_process", "name": "Processo", "owner": "",
            "orientation": "horizontal", "order": 1, "color": "#E8F5F0",
            "collapsed": False, "enabled": True, "height": 260,
        }],
        "nodes": [],
        "edges": [],
    }


def demo_flowchart_document(owner_email: str = "") -> dict[str, Any]:
    doc = new_flowchart_document("Fluxo de exemplo", owner_email)
    doc["lanes"] = [
        {"id": "lane_comercial", "name": "Comercial", "owner": "Comercial", "orientation": "horizontal", "order": 1, "color": "#E8F5F0", "collapsed": False, "enabled": True, "height": 240},
        {"id": "lane_operacao", "name": "Operação", "owner": "Operação", "orientation": "horizontal", "order": 2, "color": "#EAF4FF", "collapsed": False, "enabled": True, "height": 240},
    ]
    nodes = [
        ("node_start", "start", "lane_comercial", 80, 78, "Solicitação recebida", "Comercial", "executive"),
        ("node_analyze", "task", "lane_comercial", 330, 70, "Analisar solicitação", "Comercial", "operational"),
        ("node_decision", "decision", "lane_comercial", 610, 65, "Dados completos?", "Comercial", "operational"),
        ("node_reject", "end", "lane_comercial", 890, 145, "Solicitação devolvida", "Comercial", "operational"),
        ("node_execute", "subprocess", "lane_operacao", 890, 315, "Executar processo", "Operação", "executive"),
        ("node_end", "end", "lane_operacao", 1180, 325, "Processo concluído", "Operação", "executive"),
    ]
    doc["nodes"] = []
    for node_id, node_type, lane_id, x, y, label, owner, level in nodes:
        data = _default_node_data(label)
        data.update({"owner": owner, "level": level, "slaMinutes": 60 if node_id == "node_analyze" else None})
        doc["nodes"].append({"id": node_id, "type": node_type, "laneId": lane_id, "position": {"x": x, "y": y}, "data": data})
    doc["edges"] = [
        {"id": "edge_1", "source": "node_start", "target": "node_analyze", "sourceHandle": "output", "targetHandle": "input", "type": "step", "label": "", "enabled": True, "condition": ""},
        {"id": "edge_2", "source": "node_analyze", "target": "node_decision", "sourceHandle": "output", "targetHandle": "input", "type": "step", "label": "", "enabled": True, "condition": ""},
        {"id": "edge_3", "source": "node_decision", "target": "node_execute", "sourceHandle": "branch-0", "targetHandle": "input", "type": "step", "label": "Sim", "enabled": True, "condition": "Dados completos"},
        {"id": "edge_4", "source": "node_decision", "target": "node_reject", "sourceHandle": "branch-1", "targetHandle": "input", "type": "step", "label": "Não", "enabled": True, "condition": "Dados incompletos"},
        {"id": "edge_5", "source": "node_execute", "target": "node_end", "sourceHandle": "output", "targetHandle": "input", "type": "step", "label": "", "enabled": True, "condition": ""},
    ]
    return doc


def normalize_document(document: dict[str, Any], owner_email: str = "") -> dict[str, Any]:
    doc = deepcopy(document or {})
    doc["schemaVersion"] = SCHEMA_VERSION
    flow = doc.setdefault("flow", {})
    flow.setdefault("id", f"flow_{uuid4().hex[:12]}")
    flow.setdefault("name", "Processo importado")
    flow.setdefault("description", "")
    status = str(flow.get("status") or "draft")
    flow["status"] = "published" if status == "active" else (status if status in FLOW_STATUSES else "draft")
    flow.setdefault("orientation", "LR")
    if flow.get("orientation") not in {"LR", "RL"}:
        flow["orientation"] = "LR"
    flow.setdefault("createdAt", now_iso())
    flow["updatedAt"] = now_iso()
    flow.setdefault("createdBy", owner_email)
    flow["tags"] = [str(item).strip() for item in flow.get("tags", []) if str(item).strip()][:30]

    settings = doc.setdefault("settings", {})
    defaults = {
        "snapToGrid": True, "gridSize": 20, "autoLayout": False,
        "showMiniMap": True, "showGrid": True, "layoutPreset": "readable",
        "edgeRouting": "smooth", "autosaveSeconds": 10,
        "interactivePlayback": True,
    }
    for key, value in defaults.items():
        settings.setdefault(key, value)
    preset_aliases = {
        "compact-readable-v2": "compact",
        "compact-readable": "compact",
        "balanced": "readable",
        "legible": "readable",
    }
    settings["layoutPreset"] = preset_aliases.get(str(settings.get("layoutPreset") or ""), str(settings.get("layoutPreset") or "readable"))
    if settings["layoutPreset"] not in {"compact", "readable", "preserve"}:
        settings["layoutPreset"] = "readable"
    routing_aliases = {
        "step": "orthogonal",
        "smoothstep": "smooth",
        "bezier": "smooth",
        "curve": "smooth",
    }
    settings["edgeRouting"] = routing_aliases.get(
        str(settings.get("edgeRouting") or "").lower(),
        str(settings.get("edgeRouting") or "smooth").lower(),
    )
    if settings["edgeRouting"] not in {"corridor", "corridor-v2", "orthogonal", "smooth", "straight"}:
        settings["edgeRouting"] = "smooth"
    settings["autosaveSeconds"] = max(5, min(300, int(settings.get("autosaveSeconds") or 10)))
    doc.setdefault("viewport", {"x": 0, "y": 0, "zoom": 1})
    doc.setdefault("lanes", [])
    doc.setdefault("nodes", [])
    doc.setdefault("edges", [])

    for index, lane in enumerate(doc["lanes"]):
        lane.setdefault("id", f"lane_{uuid4().hex[:8]}")
        lane.setdefault("name", f"Raia {index + 1}")
        lane.setdefault("owner", "")
        lane.setdefault("orientation", "horizontal")
        lane.setdefault("order", index + 1)
        lane.setdefault("color", "#E8F5F0")
        lane.setdefault("collapsed", False)
        lane.setdefault("enabled", True)
        lane["height"] = max(110, min(1600, int(lane.get("height") or 240)))

    for node in doc["nodes"]:
        node.setdefault("id", f"node_{uuid4().hex[:10]}")
        if node.get("type") not in NODE_TYPES:
            node["type"] = "task"
        node.setdefault("laneId", None)
        node.setdefault("position", {"x": 0, "y": 0})
        node["position"]["x"] = float(node["position"].get("x") or 0)
        node["position"]["y"] = float(node["position"].get("y") or 0)
        data = node.setdefault("data", {})
        defaults_data = _default_node_data(str(data.get("label") or node["type"].title()))
        for key, value in defaults_data.items():
            data.setdefault(key, deepcopy(value))
        data["enabled"] = data.get("enabled") is not False
        data["locked"] = data.get("locked") is True
        data["tags"] = [str(tag).strip() for tag in data.get("tags", []) if str(tag).strip()][:30]
        if data.get("level") not in {"executive", "operational", "technical"}:
            data["level"] = "technical" if node["type"] == "api" else "operational"
        if data.get("criticality") not in {"low", "medium", "high", "critical"}:
            data["criticality"] = "medium"
        if not isinstance(data.get("raci"), dict):
            data["raci"] = deepcopy(defaults_data["raci"])

    outgoing: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for edge in doc["edges"]:
        edge.setdefault("id", f"edge_{uuid4().hex[:10]}")
        edge.setdefault("type", "step")
        edge.setdefault("label", "")
        edge.setdefault("condition", "")
        edge.setdefault("enabled", True)
        edge.setdefault("targetHandle", "input")
        outgoing[str(edge.get("source") or "")].append(edge)
    node_by_id = {node["id"]: node for node in doc["nodes"]}
    for source_id, edges in outgoing.items():
        decision = node_by_id.get(source_id, {}).get("type") == "decision"
        for index, edge in enumerate(edges):
            edge["sourceHandle"] = edge.get("sourceHandle") or (f"branch-{index}" if decision else "output")
    return doc


def repair_import_document(
    document: dict[str, Any],
    owner_email: str = "",
) -> tuple[dict[str, Any], list[str]]:
    """Normaliza um arquivo importado e corrige inconsistências estruturais seguras.

    Uma decisão com zero ou uma saída não representa uma ramificação real. Em vez de
    bloquear toda a importação, o elemento é convertido em tarefa e a conexão única
    passa a usar a saída padrão. Nenhuma condição de negócio é inventada.
    """
    doc = normalize_document(document, owner_email)
    warnings: list[str] = []
    active_node_ids = {
        str(node.get("id") or "")
        for node in doc.get("nodes", [])
        if (node.get("data") or {}).get("enabled", True)
    }
    outgoing: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for edge in doc.get("edges", []):
        if not edge.get("enabled", True):
            continue
        if str(edge.get("target") or "") not in active_node_ids:
            continue
        outgoing[str(edge.get("source") or "")].append(edge)

    for node in doc.get("nodes", []):
        if node.get("type") != "decision" or not (node.get("data") or {}).get("enabled", True):
            continue
        node_id = str(node.get("id") or "")
        node_out = outgoing.get(node_id, [])
        if len(node_out) >= 2:
            continue
        label = str((node.get("data") or {}).get("label") or node_id)
        node["type"] = "task"
        data = node.setdefault("data", {})
        data["importRepair"] = "decision_without_branches_converted_to_task"
        tags = [str(item).strip() for item in data.get("tags", []) if str(item).strip()]
        if "Importação corrigida" not in tags:
            tags.append("Importação corrigida")
        data["tags"] = tags[:30]
        for edge in node_out:
            edge["sourceHandle"] = "output"
        if len(node_out) == 1:
            warnings.append(
                f"O elemento {node_id} ({label}) foi convertido de decisão para tarefa porque possuía somente uma saída."
            )
        else:
            warnings.append(
                f"O elemento {node_id} ({label}) foi convertido de decisão para tarefa porque não possuía saídas ativas."
            )
    return doc, warnings


def _reachable(start_ids: list[str], edges: list[dict[str, Any]]) -> set[str]:
    outgoing: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        outgoing[edge["source"]].append(edge["target"])
    visited: set[str] = set()
    queue = deque(start_ids)
    while queue:
        node_id = queue.popleft()
        if node_id in visited:
            continue
        visited.add(node_id)
        queue.extend(outgoing.get(node_id, []))
    return visited


def validate_document(document: dict[str, Any], *, strict: bool = False) -> list[str]:
    errors: list[str] = []
    validator = Draft202012Validator(FLOWCHART_SCHEMA)
    for error in sorted(validator.iter_errors(document), key=lambda item: list(item.path)):
        path = ".".join(str(part) for part in error.path) or "documento"
        errors.append(f"{path}: {error.message}")
    if errors:
        return errors

    nodes = document.get("nodes", [])
    lanes = document.get("lanes", [])
    edges = document.get("edges", [])
    node_ids = [node["id"] for node in nodes]
    lane_ids = [lane["id"] for lane in lanes]
    edge_ids = [edge["id"] for edge in edges]
    for label, values in (("nó", node_ids), ("raia", lane_ids), ("conexão", edge_ids)):
        duplicates = sorted(value for value, count in Counter(values).items() if count > 1)
        if duplicates:
            errors.append(f"IDs duplicados de {label}: {', '.join(duplicates)}")

    node_set, lane_set = set(node_ids), set(lane_ids)
    active_nodes = {node["id"]: node for node in nodes if node.get("data", {}).get("enabled", True)}
    active_edges = [edge for edge in edges if edge.get("enabled", True)]
    incoming: dict[str, list[dict[str, Any]]] = defaultdict(list)
    outgoing: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for edge in edges:
        if edge.get("source") not in node_set:
            errors.append(f"Conexão {edge['id']} aponta para origem inexistente: {edge.get('source')}")
        if edge.get("target") not in node_set:
            errors.append(f"Conexão {edge['id']} aponta para destino inexistente: {edge.get('target')}")
        if edge.get("source") == edge.get("target"):
            errors.append(f"Conexão {edge['id']} liga um nó a ele mesmo")
        if edge.get("enabled", True):
            outgoing[str(edge.get("source"))].append(edge)
            incoming[str(edge.get("target"))].append(edge)

    starts, ends = [], []
    for node in nodes:
        node_id = node["id"]
        data = node.get("data", {})
        if node.get("laneId") and node.get("laneId") not in lane_set:
            errors.append(f"Nó {node_id} referencia raia inexistente: {node.get('laneId')}")
        if not str(data.get("label", "")).strip():
            errors.append(f"Nó {node_id} está sem nome")
        if not data.get("enabled", True):
            continue
        if node.get("type") == "start":
            starts.append(node_id)
            if strict and incoming.get(node_id):
                errors.append(f"Início {node_id} não pode possuir conexão de entrada")
        if node.get("type") == "end":
            ends.append(node_id)
            if strict and outgoing.get(node_id):
                errors.append(f"Fim {node_id} não pode possuir conexão de saída")
        if node.get("type") == "decision":
            node_out = [edge for edge in outgoing.get(node_id, []) if edge.get("target") in active_nodes]
            if len(node_out) < 2:
                errors.append(f"Decisão {node_id} deve possuir no mínimo duas conexões de saída")
            conditions = [str(edge.get("label") or edge.get("condition") or "").strip().lower() for edge in node_out]
            if strict and any(not condition for condition in conditions):
                errors.append(f"Decisão {node_id} possui saída sem rótulo ou condição")
            duplicates = [value for value, count in Counter(conditions).items() if value and count > 1]
            if strict and duplicates:
                errors.append(f"Decisão {node_id} possui condições repetidas: {', '.join(duplicates)}")
        if node.get("type") == "subprocess" and data.get("linkedFlowId") == document.get("flow", {}).get("id"):
            errors.append(f"Subprocesso {node_id} não pode apontar para o próprio fluxo")

    if strict and active_nodes and not starts:
        errors.append("O fluxo ativo não possui elemento de início")
    if strict and active_nodes and not ends:
        errors.append("O fluxo ativo não possui elemento de fim")
    reachable = _reachable(starts, [edge for edge in active_edges if edge.get("source") in active_nodes and edge.get("target") in active_nodes])
    if strict:
        for node_id, node in active_nodes.items():
            if starts and node_id not in reachable:
                errors.append(f"Nó {node_id} está inacessível a partir dos elementos de início")
    return errors
