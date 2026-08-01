from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from jsonschema import Draft202012Validator

SCHEMA_VERSION = "1.0.0"

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
                "status": {"enum": ["draft", "active", "archived"]},
                "orientation": {"enum": ["LR", "TB", "RL", "BT"]},
            },
            "additionalProperties": True,
        },
        "settings": {"type": "object"},
        "viewport": {"type": "object"},
        "lanes": {"type": "array", "items": {"type": "object", "required": ["id", "name"]}},
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
            },
        },
    },
    "additionalProperties": True,
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
        },
        "settings": {
            "snapToGrid": True,
            "gridSize": 20,
            "autoLayout": False,
            "showMiniMap": True,
            "showGrid": True,
        },
        "viewport": {"x": 0, "y": 0, "zoom": 1},
        "lanes": [
            {
                "id": "lane_process",
                "name": "Processo",
                "orientation": "horizontal",
                "order": 1,
                "color": "#EEF2FF",
                "collapsed": False,
                "enabled": True,
                "height": 260,
            }
        ],
        "nodes": [],
        "edges": [],
    }


def demo_flowchart_document(owner_email: str = "") -> dict[str, Any]:
    doc = new_flowchart_document("Fluxo de exemplo", owner_email)
    doc["lanes"] = [
        {"id": "lane_comercial", "name": "Comercial", "orientation": "horizontal", "order": 1, "color": "#EEF2FF", "collapsed": False, "enabled": True, "height": 240},
        {"id": "lane_operacao", "name": "Operação", "orientation": "horizontal", "order": 2, "color": "#ECFDF5", "collapsed": False, "enabled": True, "height": 240},
    ]
    doc["nodes"] = [
        {"id": "node_start", "type": "start", "laneId": "lane_comercial", "position": {"x": 80, "y": 78}, "data": {"label": "Solicitação recebida", "description": "", "owner": "Comercial", "enabled": True, "locked": False, "slaMinutes": None, "tags": []}},
        {"id": "node_analyze", "type": "task", "laneId": "lane_comercial", "position": {"x": 330, "y": 70}, "data": {"label": "Analisar solicitação", "description": "Validar dados e escopo", "owner": "Comercial", "enabled": True, "locked": False, "slaMinutes": 60, "tags": ["análise"]}},
        {"id": "node_decision", "type": "decision", "laneId": "lane_comercial", "position": {"x": 610, "y": 65}, "data": {"label": "Dados completos?", "description": "", "owner": "Comercial", "enabled": True, "locked": False, "slaMinutes": None, "tags": []}},
        {"id": "node_reject", "type": "end", "laneId": "lane_comercial", "position": {"x": 890, "y": 145}, "data": {"label": "Solicitação devolvida", "description": "Solicitar complementação dos dados", "owner": "Comercial", "enabled": True, "locked": False, "slaMinutes": None, "tags": ["pendência"]}},
        {"id": "node_execute", "type": "subprocess", "laneId": "lane_operacao", "position": {"x": 890, "y": 315}, "data": {"label": "Executar processo", "description": "", "owner": "Operação", "enabled": True, "locked": False, "slaMinutes": 240, "tags": []}},
        {"id": "node_end", "type": "end", "laneId": "lane_operacao", "position": {"x": 1180, "y": 325}, "data": {"label": "Processo concluído", "description": "", "owner": "Operação", "enabled": True, "locked": False, "slaMinutes": None, "tags": []}},
    ]
    doc["edges"] = [
        {"id": "edge_1", "source": "node_start", "target": "node_analyze", "sourceHandle": "output", "targetHandle": "input", "type": "smoothstep", "label": "", "enabled": True, "condition": None},
        {"id": "edge_2", "source": "node_analyze", "target": "node_decision", "sourceHandle": "output", "targetHandle": "input", "type": "smoothstep", "label": "", "enabled": True, "condition": None},
        {"id": "edge_3", "source": "node_decision", "target": "node_execute", "sourceHandle": "branch-0", "targetHandle": "input", "type": "smoothstep", "label": "Sim", "enabled": True, "condition": "Dados completos"},
        {"id": "edge_4", "source": "node_decision", "target": "node_reject", "sourceHandle": "branch-1", "targetHandle": "input", "type": "smoothstep", "label": "Não", "enabled": True, "condition": "Dados incompletos"},
        {"id": "edge_5", "source": "node_execute", "target": "node_end", "sourceHandle": "output", "targetHandle": "input", "type": "smoothstep", "label": "", "enabled": True, "condition": None},
    ]
    return doc


def normalize_document(document: dict[str, Any], owner_email: str = "") -> dict[str, Any]:
    doc = deepcopy(document)
    doc.setdefault("schemaVersion", SCHEMA_VERSION)
    doc.setdefault("flow", {})
    doc["flow"].setdefault("id", f"flow_{uuid4().hex[:12]}")
    doc["flow"].setdefault("name", "Processo importado")
    doc["flow"].setdefault("description", "")
    doc["flow"].setdefault("status", "draft")
    doc["flow"].setdefault("orientation", "LR")
    doc["flow"].setdefault("createdAt", now_iso())
    doc["flow"]["updatedAt"] = now_iso()
    doc["flow"].setdefault("createdBy", owner_email)
    doc.setdefault("settings", {"snapToGrid": True, "gridSize": 20, "showMiniMap": True, "showGrid": True})
    doc.setdefault("viewport", {"x": 0, "y": 0, "zoom": 1})
    doc.setdefault("lanes", [])
    doc.setdefault("nodes", [])
    doc.setdefault("edges", [])
    return doc


def validate_document(document: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    validator = Draft202012Validator(FLOWCHART_SCHEMA)
    for error in sorted(validator.iter_errors(document), key=lambda item: list(item.path)):
        path = ".".join(str(part) for part in error.path) or "documento"
        errors.append(f"{path}: {error.message}")

    if errors:
        return errors

    node_ids = [node["id"] for node in document.get("nodes", [])]
    lane_ids = [lane["id"] for lane in document.get("lanes", [])]
    edge_ids = [edge["id"] for edge in document.get("edges", [])]

    for label, values in (("nó", node_ids), ("raia", lane_ids), ("conexão", edge_ids)):
        duplicates = sorted({value for value in values if values.count(value) > 1})
        if duplicates:
            errors.append(f"IDs duplicados de {label}: {', '.join(duplicates)}")

    node_set = set(node_ids)
    lane_set = set(lane_ids)
    for edge in document.get("edges", []):
        if edge["source"] not in node_set:
            errors.append(f"Conexão {edge['id']} aponta para origem inexistente: {edge['source']}")
        if edge["target"] not in node_set:
            errors.append(f"Conexão {edge['id']} aponta para destino inexistente: {edge['target']}")
        if edge["source"] == edge["target"]:
            errors.append(f"Conexão {edge['id']} liga um nó a ele mesmo")

    active_edges = [edge for edge in document.get("edges", []) if edge.get("enabled", True)]
    for node in document.get("nodes", []):
        lane_id = node.get("laneId")
        if lane_id and lane_id not in lane_set:
            errors.append(f"Nó {node['id']} referencia raia inexistente: {lane_id}")
        if not str(node.get("data", {}).get("label", "")).strip():
            errors.append(f"Nó {node['id']} está sem nome")
        if node.get("type") == "decision" and node.get("data", {}).get("enabled", True):
            outgoing = [edge for edge in active_edges if edge.get("source") == node["id"]]
            if len(outgoing) < 2:
                errors.append(
                    f"Decisão {node['id']} deve possuir no mínimo duas conexões de saída"
                )

    return errors
