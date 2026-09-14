from __future__ import annotations

import csv
import io
import json
import math
import random
import statistics
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

import pandas as pd

from schemas.flowchart_schema import new_flowchart_document, normalize_document
from services.flow_analytics import analyze_document, build_raci_rows

BPMN_NS = "http://www.omg.org/spec/BPMN/20100524/MODEL"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
ET.register_namespace("bpmn", BPMN_NS)
ET.register_namespace("xsi", XSI_NS)

def _active_graph(document: dict[str, Any]):
    nodes = [n for n in document.get("nodes", []) if (n.get("data") or {}).get("enabled", True)]
    node_map = {str(n.get("id")): n for n in nodes}
    edges = [
        e for e in document.get("edges", [])
        if e.get("enabled", True) and str(e.get("source")) in node_map and str(e.get("target")) in node_map
    ]
    outgoing: dict[str, list[dict[str, Any]]] = defaultdict(list)
    incoming: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for edge in edges:
        outgoing[str(edge["source"])].append(edge)
        incoming[str(edge["target"])].append(edge)
    return nodes, node_map, edges, outgoing, incoming

def sipoc_from_document(document: dict[str, Any]) -> dict[str, list[str]]:
    nodes, node_map, edges, outgoing, incoming = _active_graph(document)
    lane_map = {str(l.get("id")): str(l.get("name") or "") for l in document.get("lanes", [])}
    suppliers = sorted({lane_map.get(str(n.get("laneId")), "") for n in nodes if not incoming.get(str(n.get("id"))) and lane_map.get(str(n.get("laneId")), "")})
    customers = sorted({lane_map.get(str(n.get("laneId")), "") for n in nodes if not outgoing.get(str(n.get("id"))) and lane_map.get(str(n.get("laneId")), "")})
    process = [str((n.get("data") or {}).get("label") or n.get("id")) for n in nodes if n.get("type") not in {"note", "document"}][:12]
    inputs = sorted({
        str((n.get("data") or {}).get("description") or "").strip()
        for n in nodes if n.get("type") == "document" and str((n.get("data") or {}).get("description") or "").strip()
    })
    outputs = [str((n.get("data") or {}).get("label") or n.get("id")) for n in nodes if n.get("type") == "end"]
    return {
        "Suppliers": suppliers or ["Definir fornecedores"],
        "Inputs": inputs or ["Definir entradas"],
        "Process": process or ["Definir etapas principais"],
        "Outputs": outputs or ["Definir saídas"],
        "Customers": customers or ["Definir clientes"],
    }

def five_whys(problem: str, answers: list[str]) -> list[dict[str, str]]:
    rows = []
    current = problem.strip()
    for index in range(5):
        answer = str(answers[index] if index < len(answers) else "").strip()
        rows.append({"Nível": str(index + 1), "Pergunta": f"Por que {current}?" if current else f"Por quê? ({index + 1})", "Resposta": answer})
        if answer:
            current = answer
    return rows

def pareto_rows(labels: list[str]) -> list[dict[str, Any]]:
    counts = Counter(str(item).strip() for item in labels if str(item).strip())
    total = sum(counts.values()) or 1
    cumulative = 0.0
    rows = []
    for label, count in counts.most_common():
        percentage = count * 100.0 / total
        cumulative += percentage
        rows.append({"Causa": label, "Ocorrências": count, "%": round(percentage, 2), "% acumulado": round(cumulative, 2)})
    return rows

def vsm_metrics(rows: list[dict[str, Any]]) -> dict[str, float]:
    va = sum(float(row.get("va_minutes") or 0) for row in rows)
    wait = sum(float(row.get("wait_minutes") or 0) for row in rows)
    total = va + wait
    return {
        "value_added_minutes": round(va, 2),
        "wait_minutes": round(wait, 2),
        "lead_time_minutes": round(total, 2),
        "flow_efficiency_percent": round((va / total * 100.0) if total else 0.0, 2),
    }

def simulate_process(
    document: dict[str, Any],
    *,
    iterations: int = 1000,
    default_task_minutes: float = 15.0,
    hourly_cost: float = 50.0,
    variability_percent: float = 20.0,
) -> dict[str, Any]:
    nodes, node_map, edges, outgoing, incoming = _active_graph(document)
    starts = [str(n["id"]) for n in nodes if n.get("type") == "start"] or ([str(nodes[0]["id"])] if nodes else [])
    if not starts:
        return {"iterations": 0, "mean_minutes": 0, "p50_minutes": 0, "p95_minutes": 0, "mean_cost": 0, "paths": []}
    durations: list[float] = []
    costs: list[float] = []
    paths: Counter[str] = Counter()
    for _ in range(max(1, min(int(iterations), 20000))):
        current = starts[0]
        visited: Counter[str] = Counter()
        duration = 0.0
        path: list[str] = []
        for _step in range(max(10, len(nodes) * 4 + 10)):
            if current not in node_map:
                break
            node = node_map[current]
            visited[current] += 1
            if visited[current] > 3:
                break
            label = str((node.get("data") or {}).get("label") or current)
            path.append(label)
            base = float((node.get("data") or {}).get("slaMinutes") or default_task_minutes)
            variation = max(0.0, variability_percent) / 100.0
            sampled = max(0.0, random.uniform(base * (1 - variation), base * (1 + variation)))
            if node.get("type") not in {"start", "end", "note"}:
                duration += sampled
            options = outgoing.get(current, [])
            if not options:
                break
            weights = []
            for edge in options:
                raw = edge.get("probability")
                if raw is None:
                    raw = (edge.get("data") or {}).get("probability")
                try:
                    weights.append(max(0.0, float(raw)))
                except (TypeError, ValueError):
                    weights.append(1.0)
            if sum(weights) <= 0:
                weights = [1.0] * len(options)
            selected = random.choices(options, weights=weights, k=1)[0]
            current = str(selected["target"])
        durations.append(duration)
        costs.append(duration / 60.0 * float(hourly_cost))
        paths[" → ".join(path)] += 1
    sorted_durations = sorted(durations)
    def percentile(p: float) -> float:
        if not sorted_durations:
            return 0.0
        idx = min(len(sorted_durations) - 1, max(0, math.ceil(p * len(sorted_durations)) - 1))
        return sorted_durations[idx]
    return {
        "iterations": len(durations),
        "mean_minutes": round(statistics.fmean(durations), 2),
        "p50_minutes": round(percentile(0.50), 2),
        "p95_minutes": round(percentile(0.95), 2),
        "mean_cost": round(statistics.fmean(costs), 2),
        "paths": [{"path": path, "count": count, "percent": round(count * 100 / len(durations), 2)} for path, count in paths.most_common(10)],
    }

def process_mining(
    frame: pd.DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
) -> dict[str, Any]:
    data = frame[[case_col, activity_col, timestamp_col]].copy()
    data[case_col] = data[case_col].astype(str)
    data[activity_col] = data[activity_col].astype(str)
    data[timestamp_col] = pd.to_datetime(data[timestamp_col], errors="coerce", utc=True)
    data = data.dropna(subset=[timestamp_col]).sort_values([case_col, timestamp_col])
    dfg: Counter[tuple[str, str]] = Counter()
    variants: Counter[str] = Counter()
    durations: list[float] = []
    for _, group in data.groupby(case_col):
        activities = group[activity_col].tolist()
        variants[" → ".join(activities)] += 1
        for source, target in zip(activities, activities[1:]):
            dfg[(source, target)] += 1
        if len(group) >= 2:
            delta = group[timestamp_col].iloc[-1] - group[timestamp_col].iloc[0]
            durations.append(delta.total_seconds() / 60.0)
    return {
        "case_count": int(data[case_col].nunique()),
        "event_count": int(len(data)),
        "activity_count": int(data[activity_col].nunique()),
        "mean_case_minutes": round(statistics.fmean(durations), 2) if durations else 0.0,
        "dfg": [{"source": s, "target": t, "count": c} for (s, t), c in dfg.most_common()],
        "variants": [{"variant": v, "count": c} for v, c in variants.most_common(20)],
    }

def conformance_check(mining: dict[str, Any], document: dict[str, Any]) -> dict[str, Any]:
    nodes, node_map, edges, _, _ = _active_graph(document)
    label = {str(n["id"]): str((n.get("data") or {}).get("label") or n["id"]) for n in nodes}
    documented = {(label[str(e["source"])], label[str(e["target"])]) for e in edges if str(e["source"]) in label and str(e["target"]) in label}
    observed = {(str(r["source"]), str(r["target"])) for r in mining.get("dfg", [])}
    matching = observed & documented
    unexpected = sorted(observed - documented)
    missing = sorted(documented - observed)
    fitness = (len(matching) / len(observed) * 100.0) if observed else 100.0
    return {
        "fitness_percent": round(fitness, 2),
        "observed_edges": len(observed),
        "matching_edges": len(matching),
        "unexpected": [{"source": a, "target": b} for a, b in unexpected],
        "not_observed": [{"source": a, "target": b} for a, b in missing],
    }

def bpmn_export(document: dict[str, Any]) -> bytes:
    definitions = ET.Element(f"{{{BPMN_NS}}}definitions", attrib={"id": "Definitions_ProdutoTools", "targetNamespace": "https://produto-tools.local/bpmn"})
    process = ET.SubElement(definitions, f"{{{BPMN_NS}}}process", attrib={"id": str(document.get("flow", {}).get("id") or "Process_1"), "name": str(document.get("flow", {}).get("name") or "Processo"), "isExecutable": "false"})
    node_tag = {
        "start": "startEvent",
        "end": "endEvent",
        "decision": "exclusiveGateway",
        "subprocess": "subProcess",
        "api": "serviceTask",
        "wait": "intermediateCatchEvent",
        "event": "intermediateCatchEvent",
        "document": "dataObjectReference",
        "note": "textAnnotation",
        "task": "userTask",
    }
    for node in document.get("nodes", []):
        tag = node_tag.get(str(node.get("type")), "task")
        attrib = {"id": str(node.get("id")), "name": str((node.get("data") or {}).get("label") or node.get("id"))}
        ET.SubElement(process, f"{{{BPMN_NS}}}{tag}", attrib=attrib)
    for edge in document.get("edges", []):
        if not edge.get("enabled", True):
            continue
        attrib = {"id": str(edge.get("id")), "sourceRef": str(edge.get("source")), "targetRef": str(edge.get("target"))}
        name = str(edge.get("label") or edge.get("condition") or "").strip()
        if name:
            attrib["name"] = name
        ET.SubElement(process, f"{{{BPMN_NS}}}sequenceFlow", attrib=attrib)
    return ET.tostring(definitions, encoding="utf-8", xml_declaration=True)

def bpmn_import(content: bytes, owner_email: str = "") -> dict[str, Any]:
    root = ET.fromstring(content)
    process = next((el for el in root.iter() if el.tag.endswith("process")), None)
    if process is None:
        raise ValueError("Nenhum elemento BPMN process foi encontrado.")
    doc = new_flowchart_document(process.attrib.get("name") or "Processo BPMN importado", owner_email)
    doc["flow"]["id"] = process.attrib.get("id") or doc["flow"]["id"]
    type_map = {
        "startEvent": "start", "endEvent": "end", "exclusiveGateway": "decision",
        "inclusiveGateway": "decision", "parallelGateway": "decision", "subProcess": "subprocess",
        "serviceTask": "api", "userTask": "task", "manualTask": "task", "task": "task",
        "intermediateCatchEvent": "wait", "intermediateThrowEvent": "event",
        "dataObjectReference": "document", "textAnnotation": "note",
    }
    nodes = []
    edges = []
    x = 80
    y = 80
    for el in list(process):
        local = el.tag.split("}")[-1]
        if local == "sequenceFlow":
            edges.append({
                "id": el.attrib.get("id") or f"edge_{len(edges)+1}",
                "source": el.attrib.get("sourceRef", ""),
                "target": el.attrib.get("targetRef", ""),
                "sourceHandle": "output",
                "targetHandle": "input",
                "type": "step",
                "label": el.attrib.get("name", ""),
                "condition": el.attrib.get("name", ""),
                "enabled": True,
            })
        elif local in type_map:
            node_type = type_map[local]
            nodes.append({
                "id": el.attrib.get("id") or f"node_{len(nodes)+1}",
                "type": node_type,
                "laneId": "lane_process",
                "position": {"x": x, "y": y},
                "data": {
                    "label": el.attrib.get("name") or el.attrib.get("id") or local,
                    "description": "",
                    "owner": "",
                    "enabled": True,
                    "locked": False,
                    "slaMinutes": None,
                    "tags": ["BPMN importado"],
                    "level": "technical" if node_type == "api" else "operational",
                    "category": "process",
                    "criticality": "medium",
                    "linkedFlowId": None,
                    "linkedFlowEntryNodeId": None,
                    "linkedFlowExitNodeId": None,
                    "preferredEdgeId": None,
                    "documentationUrl": "",
                    "raci": {"responsible": "", "accountable": "", "consulted": [], "informed": []},
                },
            })
            x += 250
            if x > 1300:
                x = 80
                y += 220
    doc["nodes"] = nodes
    doc["edges"] = edges
    return normalize_document(doc, owner_email)

def dmn_evaluate(rules: list[dict[str, Any]], inputs: dict[str, Any]) -> dict[str, Any] | None:
    for rule in rules:
        when = rule.get("when") if isinstance(rule.get("when"), dict) else {}
        matched = True
        for key, expected in when.items():
            actual = inputs.get(key)
            if str(expected).strip() in {"", "*"}:
                continue
            if str(actual).strip().lower() != str(expected).strip().lower():
                matched = False
                break
        if matched:
            result = rule.get("then")
            return result if isinstance(result, dict) else {"result": result}
    return None

def generate_sop(document: dict[str, Any]) -> str:
    analysis = analyze_document(document)
    flow = document.get("flow", {})
    raci = build_raci_rows(document)
    lines = [
        f"# POP / SOP — {flow.get('name', 'Processo')}",
        "",
        "## Objetivo",
        str(flow.get("description") or "Documentar e padronizar a execução do processo."),
        "",
        "## Escopo",
        "Aplica-se às áreas e responsáveis representados nas raias do processo.",
        "",
        "## Procedimento",
    ]
    ordered = document.get("nodes", [])
    for index, node in enumerate(ordered, 1):
        data = node.get("data") or {}
        if node.get("type") in {"note"}:
            continue
        lines.append(f"{index}. **{data.get('label') or node.get('id')}** — {data.get('description') or 'Sem descrição.'} Responsável: {data.get('owner') or 'não definido'}. SLA: {data.get('slaMinutes') or 'não definido'} min.")
    lines += ["", "## Indicadores", f"- Qualidade estrutural: {analysis['quality_score']}/100", f"- SLA total cadastrado: {analysis['total_sla_minutes']} min", "", "## RACI"]
    for row in raci:
        lines.append(f"- {row['Etapa']}: R={row['Responsável'] or '—'}; A={row['Aprovador'] or '—'}; C={row['Consultados'] or '—'}; I={row['Informados'] or '—'}")
    lines += ["", "## Exceções e controles", f"- Exceções identificadas automaticamente: {analysis['counts']['exceptions']}", f"- Ciclos identificados: {analysis['counts']['cycles']}"]
    return "\n".join(lines)

def quality_copilot(document: dict[str, Any]) -> str:
    analysis = analyze_document(document)
    issues = analysis.get("issues") or {}
    recommendations = []
    if issues.get("missing_description"):
        recommendations.append(f"Documentar {len(issues['missing_description'])} etapa(s) sem descrição.")
    if issues.get("missing_owner"):
        recommendations.append(f"Definir responsáveis em {len(issues['missing_owner'])} etapa(s).")
    if issues.get("missing_sla_critical"):
        recommendations.append(f"Definir SLA em {len(issues['missing_sla_critical'])} etapa(s) críticas.")
    if issues.get("decisions_invalid"):
        recommendations.append(f"Corrigir {len(issues['decisions_invalid'])} decisão(ões) incompletas.")
    if issues.get("cycles"):
        recommendations.append("Validar ciclos e registrar condição explícita de saída.")
    if not recommendations:
        recommendations.append("O fluxo está estruturalmente consistente; avance para simulação, custos e mineração do processo real.")
    return (
        f"Qualidade atual: {analysis['quality_score']}/100. "
        f"Há {analysis['counts']['nodes']} etapas, {analysis['counts']['decisions']} decisões e "
        f"{analysis['counts']['subprocesses']} subprocessos. Prioridades: " + " ".join(recommendations)
    )

def flow_from_text(text: str, owner_email: str = "") -> dict[str, Any]:
    raw_lines = [line.strip(" -\t0123456789.)") for line in text.splitlines()]
    lines = [line.strip() for line in raw_lines if line.strip()]
    if not lines:
        raise ValueError("Informe pelo menos uma etapa.")
    doc = new_flowchart_document("Processo gerado por texto", owner_email)
    nodes = []
    edges = []
    sequence = [("start_auto", "start", "Início"), *[(f"task_{i}", "task", line) for i, line in enumerate(lines[:50], 1)], ("end_auto", "end", "Fim")]
    for index, (node_id, node_type, label) in enumerate(sequence):
        nodes.append({
            "id": node_id,
            "type": node_type,
            "laneId": "lane_process",
            "position": {"x": 80 + index * 240, "y": 90},
            "data": {
                "label": label, "description": "", "owner": "", "enabled": True, "locked": False,
                "slaMinutes": None, "tags": ["Gerado por texto"], "level": "operational",
                "category": "process", "criticality": "medium", "linkedFlowId": None,
                "linkedFlowEntryNodeId": None, "linkedFlowExitNodeId": None,
                "preferredEdgeId": None, "documentationUrl": "",
                "raci": {"responsible": "", "accountable": "", "consulted": [], "informed": []},
            },
        })
        if index:
            edges.append({
                "id": f"edge_auto_{index}", "source": sequence[index-1][0], "target": node_id,
                "sourceHandle": "output", "targetHandle": "input", "type": "step",
                "label": "", "condition": "", "enabled": True,
            })
    doc["nodes"] = nodes
    doc["edges"] = edges
    return doc

def call_openai_compatible(api_url: str, api_key: str, model: str, prompt: str) -> str:
    body = json.dumps({
        "model": model,
        "messages": [
            {"role": "system", "content": "Você é um analista de processos corporativos. Responda em português do Brasil de forma objetiva."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }).encode("utf-8")
    request = urllib.request.Request(
        api_url,
        data=body,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        data = json.loads(response.read().decode("utf-8"))
    choices = data.get("choices") or []
    if not choices:
        raise RuntimeError("O provedor de IA não retornou choices.")
    return str(((choices[0] or {}).get("message") or {}).get("content") or "").strip()

def send_webhook(url: str, event: str, payload: dict[str, Any], secret: str = "") -> tuple[int, str]:
    body = json.dumps({"event": event, "payload": payload}, ensure_ascii=False, default=str).encode("utf-8")
    headers = {"Content-Type": "application/json", "User-Agent": "ProdutoTools/4.0"}
    if secret:
        headers["X-Produto-Tools-Secret"] = secret
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(request, timeout=20) as response:
        return int(response.status), response.read(4096).decode("utf-8", errors="replace")