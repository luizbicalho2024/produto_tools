from __future__ import annotations

from collections import Counter, defaultdict, deque
from typing import Any

EXCEPTION_WORDS = (
    "erro", "falha", "recus", "cancel", "bloque", "expir", "indispon",
    "pendente", "corrigir", "reprocess", "exceção", "excecao",
)


def _active_graph(document: dict[str, Any]):
    nodes = [node for node in document.get("nodes", []) if node.get("data", {}).get("enabled", True)]
    node_map = {node["id"]: node for node in nodes}
    edges = [
        edge for edge in document.get("edges", [])
        if edge.get("enabled", True) and edge.get("source") in node_map and edge.get("target") in node_map
    ]
    outgoing: dict[str, list[dict]] = defaultdict(list)
    incoming: dict[str, list[dict]] = defaultdict(list)
    for edge in edges:
        outgoing[edge["source"]].append(edge)
        incoming[edge["target"]].append(edge)
    return nodes, node_map, edges, outgoing, incoming


def _reachable(starts: list[str], outgoing: dict[str, list[dict]]) -> set[str]:
    seen: set[str] = set()
    queue = deque(starts)
    while queue:
        current = queue.popleft()
        if current in seen:
            continue
        seen.add(current)
        queue.extend(edge["target"] for edge in outgoing.get(current, []))
    return seen


def _cycle_nodes(nodes: list[dict], outgoing: dict[str, list[dict]]) -> set[str]:
    color: dict[str, int] = {node["id"]: 0 for node in nodes}
    stack: list[str] = []
    cycles: set[str] = set()

    def visit(node_id: str) -> None:
        color[node_id] = 1
        stack.append(node_id)
        for edge in outgoing.get(node_id, []):
            target = edge["target"]
            if color.get(target, 0) == 0:
                visit(target)
            elif color.get(target) == 1:
                try:
                    index = stack.index(target)
                    cycles.update(stack[index:])
                except ValueError:
                    cycles.add(target)
        stack.pop()
        color[node_id] = 2

    for node in nodes:
        if color[node["id"]] == 0:
            visit(node["id"])
    return cycles


def _longest_acyclic_path(starts: list[str], outgoing: dict[str, list[dict]], limit: int = 5000) -> list[str]:
    best: list[str] = []
    explored = 0

    def walk(node_id: str, path: list[str], visited: set[str]) -> None:
        nonlocal best, explored
        explored += 1
        if explored > limit:
            return
        if len(path) > len(best):
            best = list(path)
        for edge in outgoing.get(node_id, []):
            target = edge["target"]
            if target in visited:
                continue
            walk(target, [*path, target], {*visited, target})

    for start in starts:
        walk(start, [start], {start})
    return best


def _is_exception(node: dict) -> bool:
    data = node.get("data", {})
    text = " ".join([
        str(data.get("label") or ""), str(data.get("description") or ""),
        " ".join(str(tag) for tag in data.get("tags", [])),
    ]).lower()
    return any(word in text for word in EXCEPTION_WORDS)


def analyze_document(document: dict[str, Any]) -> dict[str, Any]:
    nodes, node_map, edges, outgoing, incoming = _active_graph(document)
    lanes = document.get("lanes", [])
    starts = [node["id"] for node in nodes if node.get("type") == "start"]
    ends = [node["id"] for node in nodes if node.get("type") == "end"]
    reachable = _reachable(starts, outgoing) if starts else set()
    inaccessible = [node["id"] for node in nodes if starts and node["id"] not in reachable]
    cycle_nodes = sorted(_cycle_nodes(nodes, outgoing))
    decisions = [node for node in nodes if node.get("type") == "decision"]
    subprocesses = [node for node in nodes if node.get("type") == "subprocess"]
    integrations = [node for node in nodes if node.get("type") == "api"]
    exceptions = [node for node in nodes if _is_exception(node)]
    lane_transitions = sum(
        1 for edge in edges
        if node_map[edge["source"]].get("laneId") != node_map[edge["target"]].get("laneId")
    )
    longest = _longest_acyclic_path(starts or ([nodes[0]["id"]] if nodes else []), outgoing)
    total_sla = sum(float(node.get("data", {}).get("slaMinutes") or 0) for node in nodes)

    missing_description = [node["id"] for node in nodes if not str(node.get("data", {}).get("description") or "").strip()]
    missing_owner = [node["id"] for node in nodes if not str(node.get("data", {}).get("owner") or "").strip()]
    missing_sla_critical = [
        node["id"] for node in nodes
        if node.get("data", {}).get("criticality") in {"high", "critical"}
        and not node.get("data", {}).get("slaMinutes")
    ]
    decisions_invalid = [
        node["id"] for node in decisions
        if len(outgoing.get(node["id"], [])) < 2
        or any(not str(edge.get("label") or edge.get("condition") or "").strip() for edge in outgoing.get(node["id"], []))
    ]
    subprocess_unlinked = [
        node["id"] for node in subprocesses
        if not node.get("data", {}).get("linkedFlowId")
    ]

    structure_score = 100
    structure_score -= min(35, len(inaccessible) * 4)
    structure_score -= min(20, len(decisions_invalid) * 5)
    structure_score -= 10 if not starts and nodes else 0
    structure_score -= 10 if not ends and nodes else 0
    structure_score -= min(15, len(cycle_nodes) * 2)

    documentation_score = 100 if not nodes else round(100 * (1 - len(missing_description) / len(nodes)))
    responsibility_score = 100 if not nodes else round(100 * (1 - len(missing_owner) / len(nodes)))
    sla_relevant = [node for node in nodes if node.get("data", {}).get("criticality") in {"high", "critical"}]
    sla_score = 100 if not sla_relevant else round(100 * (1 - len(missing_sla_critical) / len(sla_relevant)))
    subprocess_score = 100 if not subprocesses else round(100 * (1 - len(subprocess_unlinked) / len(subprocesses)))
    quality_score = max(0, round(
        max(0, structure_score) * .38
        + documentation_score * .20
        + responsibility_score * .17
        + sla_score * .12
        + subprocess_score * .13
    ))

    lane_counts = Counter(node.get("laneId") or "Sem raia" for node in nodes)
    type_counts = Counter(node.get("type") or "task" for node in nodes)
    level_counts = Counter(node.get("data", {}).get("level") or "operational" for node in nodes)
    criticality_counts = Counter(node.get("data", {}).get("criticality") or "medium" for node in nodes)

    return {
        "quality_score": quality_score,
        "scores": {
            "structure": max(0, structure_score),
            "documentation": documentation_score,
            "responsibility": responsibility_score,
            "sla": sla_score,
            "subprocesses": subprocess_score,
        },
        "counts": {
            "nodes": len(nodes), "edges": len(edges), "lanes": len(lanes),
            "decisions": len(decisions), "subprocesses": len(subprocesses),
            "integrations": len(integrations), "exceptions": len(exceptions),
            "lane_transitions": lane_transitions, "cycles": len(cycle_nodes),
        },
        "longest_path_node_ids": longest,
        "longest_path_length": len(longest),
        "total_sla_minutes": total_sla,
        "issues": {
            "inaccessible": inaccessible,
            "cycles": cycle_nodes,
            "missing_description": missing_description,
            "missing_owner": missing_owner,
            "missing_sla_critical": missing_sla_critical,
            "decisions_invalid": decisions_invalid,
            "subprocess_unlinked": subprocess_unlinked,
        },
        "distribution": {
            "lanes": dict(lane_counts),
            "types": dict(type_counts),
            "levels": dict(level_counts),
            "criticality": dict(criticality_counts),
        },
    }


def build_raci_rows(document: dict[str, Any]) -> list[dict[str, str]]:
    lane_by_id = {lane.get("id"): lane.get("name", "") for lane in document.get("lanes", [])}
    rows: list[dict[str, str]] = []
    for node in document.get("nodes", []):
        data = node.get("data", {})
        raci = data.get("raci") if isinstance(data.get("raci"), dict) else {}
        rows.append({
            "Etapa": str(data.get("label") or node.get("id")),
            "Raia": str(lane_by_id.get(node.get("laneId"), "Sem raia")),
            "Responsável": str(raci.get("responsible") or data.get("owner") or ""),
            "Aprovador": str(raci.get("accountable") or ""),
            "Consultados": ", ".join(str(item) for item in raci.get("consulted", []) if item),
            "Informados": ", ".join(str(item) for item in raci.get("informed", []) if item),
            "Criticidade": str(data.get("criticality") or "medium"),
            "SLA (min)": str(data.get("slaMinutes") or ""),
        })
    return rows
