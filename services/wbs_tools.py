from __future__ import annotations

import csv
import io
import json
import re
import zipfile
from collections import defaultdict
from copy import deepcopy
from datetime import date, datetime
from html import escape
from typing import Any
from uuid import uuid4
from xml.etree import ElementTree as ET

WBS_SCHEMA_VERSION = "1.0"
SUPPORTED_IMPORT_EXTENSIONS = (".json", ".xml", ".xlsx", ".xls", ".csv", ".tsv", ".txt", ".md", ".pdf")
SUPPORTED_EXPORT_FORMATS = ("json", "xml", "xlsx", "csv", "tsv", "pdf", "md", "txt", "zip")
STATUS_OPTIONS = ("planned", "in_progress", "blocked", "done", "cancelled")
STATUS_LABELS = {
    "planned": "Planejado",
    "in_progress": "Em andamento",
    "blocked": "Bloqueado",
    "done": "Concluído",
    "cancelled": "Cancelado",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return default if value in (None, "") else float(value)
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        return default if value in (None, "") else int(float(value))
    except (TypeError, ValueError):
        return default


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _text(value).lower() in {"1", "true", "yes", "sim", "s", "x"}


def _date_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = _text(value)
    return text.split("T", 1)[0] if "T" in text else text


def _tags(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        parts = [str(item).strip() for item in value]
    else:
        parts = re.split(r"[,;\n]+", _text(value))
    return sorted({item for item in parts if item})


def _slug_id(prefix: str = "wbsnode") -> str:
    return f"{prefix}_{uuid4().hex[:14]}"


def new_node(name: str = "Novo pacote de trabalho", *, parent_id: str = "", order: int = 1, node_id: str | None = None) -> dict[str, Any]:
    return {
        "id": _text(node_id) or _slug_id(),
        "parent_id": _text(parent_id),
        "code": "",
        "name": _text(name) or "Novo pacote de trabalho",
        "description": "",
        "owner": "",
        "status": "planned",
        "deliverable": "",
        "start_date": "",
        "due_date": "",
        "duration_days": 0.0,
        "progress_percent": 0.0,
        "cost": 0.0,
        "milestone": False,
        "tags": [],
        "order": max(1, _int(order, 1)),
    }


def _key(value: Any) -> str:
    text = _text(value).lower()
    for src, dst in (("á", "a"), ("à", "a"), ("ã", "a"), ("â", "a"), ("é", "e"), ("ê", "e"), ("í", "i"), ("ó", "o"), ("ô", "o"), ("õ", "o"), ("ú", "u"), ("ç", "c")):
        text = text.replace(src, dst)
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


ALIASES = {
    "id": {"id", "uid", "node_id", "task_id"},
    "parent_id": {"parent_id", "parent", "pai_id", "id_pai", "parent_uid"},
    "parent_code": {"parent_code", "codigo_pai", "wbs_pai"},
    "code": {"code", "codigo", "wbs", "outline_number", "outline", "estrutura"},
    "name": {"name", "nome", "task_name", "pacote", "pacote_de_trabalho", "entrega"},
    "description": {"description", "descricao", "detalhes", "observacao", "observacoes"},
    "owner": {"owner", "responsavel", "assigned_to", "recurso"},
    "status": {"status", "situacao"},
    "deliverable": {"deliverable", "entregavel"},
    "start_date": {"start_date", "inicio", "data_inicio", "start"},
    "due_date": {"due_date", "fim", "data_fim", "deadline", "finish"},
    "duration_days": {"duration_days", "duracao", "duracao_dias", "duration"},
    "progress_percent": {"progress_percent", "progresso", "percentual", "percent_complete", "pct"},
    "cost": {"cost", "custo", "orcamento", "budget"},
    "milestone": {"milestone", "marco"},
    "tags": {"tags", "etiquetas"},
    "order": {"order", "ordem", "sequence", "sequencia"},
}
ALIAS_LOOKUP = {_key(alias): canonical for canonical, values in ALIASES.items() for alias in values}


def records_from_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for index, row in enumerate(rows, start=1):
        mapped = {ALIAS_LOOKUP[_key(key)]: value for key, value in row.items() if _key(key) in ALIAS_LOOKUP}
        if mapped.get("name") or mapped.get("code"):
            mapped.setdefault("order", index)
            result.append(mapped)
    return result


def _natural_code(code: str) -> tuple:
    return tuple((0, int(part)) if part.isdigit() else (1, part.lower()) for part in re.split(r"[.\-_/]+", _text(code)))


def _normalize_code(value: Any) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = _text(value)
    return text[:-2] if re.fullmatch(r"\d+\.0", text) else text


def _parent_code(code: str) -> str:
    code = _normalize_code(code)
    return code.rsplit(".", 1)[0] if "." in code else ""


def validate_wbs(nodes: list[dict[str, Any]], *, require_nodes: bool = True) -> list[str]:
    if require_nodes and not nodes:
        return ["A WBS precisa conter pelo menos um pacote de trabalho."]
    errors: list[str] = []
    ids = [_text(node.get("id")) for node in nodes]
    id_set = {item for item in ids if item}
    if len(id_set) != len([item for item in ids if item]):
        errors.append("Existem IDs de pacote duplicados.")
    by_id = {str(node.get("id")): node for node in nodes if node.get("id")}
    for node in nodes:
        identifier = _text(node.get("id"))
        name = _text(node.get("name"))
        parent = _text(node.get("parent_id"))
        if not identifier:
            errors.append("Existe pacote sem ID.")
        if not name:
            errors.append(f"O pacote {identifier or '(sem ID)'} está sem nome.")
        if parent and parent not in id_set:
            errors.append(f"O pacote {name or identifier} aponta para um pai inexistente.")
        if parent == identifier and parent:
            errors.append(f"O pacote {name or identifier} não pode ser pai de si próprio.")
    for identifier in id_set:
        cursor = identifier
        path: set[str] = set()
        while cursor:
            if cursor in path:
                errors.append(f"Foi detectado ciclo hierárquico envolvendo o pacote {identifier}.")
                break
            path.add(cursor)
            parent = _text((by_id.get(cursor) or {}).get("parent_id"))
            cursor = parent if parent in by_id else ""
    return list(dict.fromkeys(errors))


def normalize_nodes(nodes: list[dict[str, Any]], *, recode: bool = True) -> list[dict[str, Any]]:
    if not isinstance(nodes, list):
        raise ValueError("A estrutura WBS deve conter uma lista de nós.")
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    code_to_id: dict[str, str] = {}
    for index, raw in enumerate(nodes, start=1):
        if not isinstance(raw, dict):
            continue
        identifier = _text(raw.get("id"))
        if not identifier or identifier in seen:
            identifier = _slug_id()
        seen.add(identifier)
        status = _text(raw.get("status")).lower() or "planned"
        if status not in STATUS_OPTIONS:
            status = "planned"
        node = {
            "id": identifier,
            "parent_id": _text(raw.get("parent_id")),
            "parent_code": _normalize_code(raw.get("parent_code")),
            "code": _normalize_code(raw.get("code")),
            "name": _text(raw.get("name")) or f"Pacote {index}",
            "description": _text(raw.get("description")),
            "owner": _text(raw.get("owner")),
            "status": status,
            "deliverable": _text(raw.get("deliverable")),
            "start_date": _date_text(raw.get("start_date")),
            "due_date": _date_text(raw.get("due_date")),
            "duration_days": max(0.0, _float(raw.get("duration_days"))),
            "progress_percent": min(100.0, max(0.0, _float(raw.get("progress_percent")))),
            "cost": max(0.0, _float(raw.get("cost"))),
            "milestone": _bool(raw.get("milestone")),
            "tags": _tags(raw.get("tags")),
            "order": max(1, _int(raw.get("order"), index)),
        }
        result.append(node)
        if node["code"]:
            code_to_id[node["code"]] = identifier
    ids = {node["id"] for node in result}
    for node in result:
        parent = node["parent_id"]
        if parent and parent not in ids:
            node["parent_id"] = code_to_id.get(parent, "")
        if not node["parent_id"]:
            pcode = node.pop("parent_code", "") or _parent_code(node["code"])
            node["parent_id"] = code_to_id.get(pcode, "") if pcode else ""
        else:
            node.pop("parent_code", None)
        if node["parent_id"] == node["id"]:
            node["parent_id"] = ""
    cycle_errors = [item for item in validate_wbs(result, require_nodes=False) if "ciclo" in item.lower()]
    if cycle_errors:
        raise ValueError("Estrutura WBS inválida: " + " | ".join(cycle_errors))
    return recalculate_codes(result) if recode else result


def recalculate_codes(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = deepcopy(nodes)
    by_id = {str(node["id"]): node for node in result}
    children: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for node in result:
        parent = _text(node.get("parent_id"))
        if parent not in by_id or parent == node["id"]:
            parent = ""
            node["parent_id"] = ""
        children[parent].append(node)
    for group in children.values():
        group.sort(key=lambda item: (max(1, _int(item.get("order"), 1)), _natural_code(item.get("code", "")), _text(item.get("name")).lower()))
    visited: set[str] = set()
    def assign(parent_id: str, prefix: str = "") -> None:
        for position, node in enumerate(children.get(parent_id, []), start=1):
            identifier = str(node["id"])
            if identifier in visited:
                continue
            visited.add(identifier)
            node["order"] = position
            node["code"] = f"{prefix}.{position}" if prefix else str(position)
            assign(identifier, node["code"])
    assign("")
    for node in result:
        if str(node["id"]) not in visited:
            node["parent_id"] = ""
    if len(visited) != len(result):
        return recalculate_codes(result)
    return sorted(result, key=lambda item: _natural_code(item.get("code", "")))


def node_depths(nodes: list[dict[str, Any]]) -> dict[str, int]:
    by_id = {str(node["id"]): node for node in nodes}
    cache: dict[str, int] = {}
    def depth(identifier: str, active: set[str] | None = None) -> int:
        if identifier in cache:
            return cache[identifier]
        active = set(active or set())
        if identifier in active:
            return 0
        active.add(identifier)
        parent = _text((by_id.get(identifier) or {}).get("parent_id"))
        value = 0 if not parent or parent not in by_id else depth(parent, active) + 1
        cache[identifier] = value
        return value
    for identifier in by_id:
        depth(identifier)
    return cache


def table_rows(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = normalize_nodes(nodes)
    depths = node_depths(normalized)
    by_id = {str(node["id"]): node for node in normalized}
    rows = []
    for node in normalized:
        parent = by_id.get(_text(node.get("parent_id"))) or {}
        rows.append({
            "ID": node["id"], "Código": node["code"], "Código pai": parent.get("code", ""),
            "Nível": depths.get(str(node["id"]), 0) + 1, "Nome": node["name"], "Responsável": node["owner"],
            "Status": STATUS_LABELS.get(node["status"], node["status"]), "Entregável": node["deliverable"],
            "Início": node["start_date"], "Fim": node["due_date"], "Duração (dias)": node["duration_days"],
            "Progresso (%)": node["progress_percent"], "Custo": node["cost"], "Marco": "Sim" if node["milestone"] else "Não",
            "Tags": ", ".join(node["tags"]), "Descrição": node["description"],
        })
    return rows


def summary_metrics(nodes: list[dict[str, Any]]) -> dict[str, Any]:
    normalized = normalize_nodes(nodes)
    depths = node_depths(normalized)
    parent_ids = {node.get("parent_id") for node in normalized if node.get("parent_id")}
    leaves = [node for node in normalized if node["id"] not in parent_ids]
    weights = [max(_float(node.get("cost")), 1.0) for node in leaves]
    progress = sum(_float(node.get("progress_percent")) * w for node, w in zip(leaves, weights)) / sum(weights) if weights else 0.0
    return {
        "packages": len(normalized), "work_packages": len(leaves), "levels": max(depths.values()) + 1 if depths else 0,
        "total_cost": sum(_float(node.get("cost")) for node in normalized), "progress_percent": progress,
        "done": sum(1 for node in normalized if node.get("status") == "done"),
        "blocked": sum(1 for node in normalized if node.get("status") == "blocked"),
    }


def outline_text(nodes: list[dict[str, Any]]) -> str:
    normalized = normalize_nodes(nodes)
    depths = node_depths(normalized)
    return "\n".join(f"{'  ' * depths.get(node['id'], 0)}{node['code']} {node['name']}" for node in normalized)


def graphviz_dot(nodes: list[dict[str, Any]], *, rankdir: str = "TB") -> str:
    normalized = normalize_nodes(nodes)
    safe_rank = "LR" if str(rankdir).upper() == "LR" else "TB"
    lines = ["digraph WBS {", f"rankdir={safe_rank};", 'graph [pad="0.3", nodesep="0.35", ranksep="0.55"];', 'node [shape=box, style="rounded", fontname="Arial", fontsize=10];']
    for node in normalized:
        label = f"{node['code']}\\n{node['name']}" + (f"\\n{node['owner']}" if node.get("owner") else "")
        label = label.replace('"', '\\"')
        identifier = str(node["id"]).replace('"', '\\"')
        lines.append(f'"{identifier}" [label="{label}"];')
    for node in normalized:
        if node.get("parent_id"):
            lines.append(f'"{node["parent_id"]}" -> "{node["id"]}";')
    lines.append("}")
    return "\n".join(lines)


def top_level_rollup(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = normalize_nodes(nodes)
    by_id = {node["id"]: node for node in normalized}
    children: dict[str, list[str]] = defaultdict(list)
    for node in normalized:
        children[_text(node.get("parent_id"))].append(node["id"])
    def branch(identifier: str) -> list[dict[str, Any]]:
        result = [by_id[identifier]]
        for child in children.get(identifier, []):
            result.extend(branch(child))
        return result
    rows = []
    for root_id in children.get("", []):
        items = branch(root_id)
        leaves = [item for item in items if not children.get(item["id"])]
        weights = [max(_float(item.get("cost")), 1.0) for item in leaves]
        progress = sum(_float(item.get("progress_percent")) * w for item, w in zip(leaves, weights)) / sum(weights) if weights else 0.0
        rows.append({"Código": by_id[root_id]["code"], "Pacote": by_id[root_id]["name"], "Itens": len(items), "Custo": sum(_float(item.get("cost")) for item in items), "Progresso (%)": progress})
    return rows


def delete_node(nodes: list[dict[str, Any]], node_id: str, *, cascade: bool = True) -> list[dict[str, Any]]:
    normalized = normalize_nodes(nodes, recode=False)
    target = next((node for node in normalized if node["id"] == node_id), None)
    if not target:
        return normalize_nodes(normalized)
    if cascade:
        remove_ids = {node_id}
        changed = True
        while changed:
            changed = False
            for node in normalized:
                if node.get("parent_id") in remove_ids and node["id"] not in remove_ids:
                    remove_ids.add(node["id"]); changed = True
        normalized = [node for node in normalized if node["id"] not in remove_ids]
    else:
        parent = target.get("parent_id", "")
        normalized = [node for node in normalized if node["id"] != node_id]
        for node in normalized:
            if node.get("parent_id") == node_id:
                node["parent_id"] = parent
    return normalize_nodes(normalized) if normalized else []


def _payload(record: dict[str, Any]) -> dict[str, Any]:
    return {"schema": "produto-tools-wbs", "schema_version": WBS_SCHEMA_VERSION, "id": _text(record.get("id")), "name": _text(record.get("name")) or "WBS", "description": _text(record.get("description")), "project_id": _text(record.get("project_id")), "nodes": normalize_nodes(deepcopy(record.get("nodes") or []))}


def export_json(record: dict[str, Any]) -> bytes:
    return json.dumps(_payload(record), ensure_ascii=False, indent=2, default=str).encode("utf-8")


def import_json(content: bytes) -> dict[str, Any]:
    data = json.loads(content.decode("utf-8-sig"))
    if isinstance(data, list):
        nodes, name, description = data, "WBS importada", ""
    elif isinstance(data, dict):
        nodes = data.get("nodes") or data.get("items") or data.get("tasks") or []
        name, description = _text(data.get("name")) or "WBS importada", _text(data.get("description"))
    else:
        raise ValueError("JSON de WBS inválido.")
    return {"name": name, "description": description, "nodes": normalize_nodes(nodes), "warnings": []}


def export_xml(record: dict[str, Any]) -> bytes:
    payload = _payload(record)
    root = ET.Element("wbs", {"schema": payload["schema"], "schemaVersion": payload["schema_version"], "id": payload["id"], "name": payload["name"], "projectId": payload["project_id"]})
    ET.SubElement(root, "description").text = payload["description"]
    nodes_el = ET.SubElement(root, "nodes")
    for node in payload["nodes"]:
        attrs = {"id": node["id"], "parentId": node["parent_id"], "code": node["code"], "name": node["name"], "owner": node["owner"], "status": node["status"], "startDate": node["start_date"], "dueDate": node["due_date"], "durationDays": str(node["duration_days"]), "progressPercent": str(node["progress_percent"]), "cost": str(node["cost"]), "milestone": "true" if node["milestone"] else "false", "order": str(node["order"])}
        element = ET.SubElement(nodes_el, "node", attrs)
        ET.SubElement(element, "description").text = node["description"]
        ET.SubElement(element, "deliverable").text = node["deliverable"]
        tags = ET.SubElement(element, "tags")
        for tag in node["tags"]:
            ET.SubElement(tags, "tag").text = tag
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _child_text(element: ET.Element, name: str) -> str:
    for child in list(element):
        if _local(child.tag).lower() == name.lower():
            return _text(child.text)
    return ""


def import_xml(content: bytes) -> dict[str, Any]:
    root = ET.fromstring(content)
    if _local(root.tag).lower() == "wbs":
        nodes = []
        for element in root.iter():
            if _local(element.tag).lower() != "node":
                continue
            nodes.append({"id": element.attrib.get("id", ""), "parent_id": element.attrib.get("parentId", ""), "code": element.attrib.get("code", ""), "name": element.attrib.get("name", ""), "owner": element.attrib.get("owner", ""), "status": element.attrib.get("status", "planned"), "start_date": element.attrib.get("startDate", ""), "due_date": element.attrib.get("dueDate", ""), "duration_days": element.attrib.get("durationDays", 0), "progress_percent": element.attrib.get("progressPercent", 0), "cost": element.attrib.get("cost", 0), "milestone": element.attrib.get("milestone", "false"), "order": element.attrib.get("order", 1), "description": _child_text(element, "description"), "deliverable": _child_text(element, "deliverable"), "tags": [_text(item.text) for item in element.iter() if _local(item.tag).lower() == "tag" and _text(item.text)]})
        return {"name": _text(root.attrib.get("name")) or "WBS importada", "description": _child_text(root, "description"), "nodes": normalize_nodes(nodes), "warnings": []}
    tasks = [element for element in root.iter() if _local(element.tag).lower() == "task"]
    if tasks:
        raw = []
        for index, task in enumerate(tasks, start=1):
            name = _child_text(task, "Name")
            if not name:
                continue
            code = _child_text(task, "WBS") or _child_text(task, "OutlineNumber")
            raw.append({"id": _child_text(task, "UID") or _child_text(task, "ID") or _slug_id("msp"), "code": code, "parent_code": _parent_code(code), "name": name, "start_date": _child_text(task, "Start"), "due_date": _child_text(task, "Finish"), "progress_percent": _child_text(task, "PercentComplete"), "milestone": _child_text(task, "Milestone"), "order": index})
        return {"name": _child_text(root, "Name") or "WBS Microsoft Project", "description": "", "nodes": normalize_nodes(raw), "warnings": ["XML interpretado como Microsoft Project; campos não relacionados à WBS foram ignorados."]}
    raise ValueError("XML não reconhecido como WBS nem como Microsoft Project XML.")


def export_excel(record: dict[str, Any]) -> bytes:
    import pandas as pd
    payload = _payload(record); buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame(table_rows(payload["nodes"])).to_excel(writer, sheet_name="WBS", index=False)
        pd.DataFrame([{"Campo": "Nome", "Valor": payload["name"]}, {"Campo": "Descrição", "Valor": payload["description"]}, {"Campo": "Projeto", "Valor": payload["project_id"]}, {"Campo": "Schema", "Valor": payload["schema_version"]}]).to_excel(writer, sheet_name="Metadados", index=False)
    return buffer.getvalue()


def _nodes_from_frame(frame: Any) -> list[dict[str, Any]]:
    return normalize_nodes(records_from_rows(frame.fillna("").to_dict("records")))


def import_excel(content: bytes) -> dict[str, Any]:
    import pandas as pd
    book = pd.ExcelFile(io.BytesIO(content)); sheet = "WBS" if "WBS" in book.sheet_names else book.sheet_names[0]
    nodes = _nodes_from_frame(pd.read_excel(book, sheet_name=sheet)); name, description = "WBS importada do Excel", ""
    if "Metadados" in book.sheet_names:
        meta = pd.read_excel(book, sheet_name="Metadados").fillna("")
        if {"Campo", "Valor"}.issubset(meta.columns):
            values = {str(row["Campo"]): row["Valor"] for _, row in meta.iterrows()}
            name, description = _text(values.get("Nome")) or name, _text(values.get("Descrição"))
    return {"name": name, "description": description, "nodes": nodes, "warnings": []}


def export_delimited(record: dict[str, Any], *, delimiter: str = ",") -> bytes:
    rows = table_rows(record.get("nodes") or []); buffer = io.StringIO()
    if rows:
        writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()), delimiter=delimiter, lineterminator="\n"); writer.writeheader(); writer.writerows(rows)
    return buffer.getvalue().encode("utf-8-sig")


def import_delimited(content: bytes, *, delimiter: str | None = None) -> dict[str, Any]:
    import pandas as pd
    frame = pd.read_csv(io.BytesIO(content), sep=delimiter if delimiter else None, engine="python", encoding="utf-8-sig")
    return {"name": "WBS importada", "description": "", "nodes": _nodes_from_frame(frame), "warnings": []}


def export_markdown(record: dict[str, Any]) -> bytes:
    payload = _payload(record); depths = node_depths(payload["nodes"]); lines = [f"# {payload['name']}", ""]
    if payload["description"]: lines += [payload["description"], ""]
    for node in payload["nodes"]:
        lines.append(f"{'  ' * depths.get(node['id'], 0)}- {node['code']} {node['name']}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def export_text(record: dict[str, Any]) -> bytes:
    return (outline_text(record.get("nodes") or []) + "\n").encode("utf-8")


CODE_LINE_RE = re.compile(r"^(?P<code>\d+(?:\.\d+)*)\s*[-–—:]?\s*(?P<name>.+)$")


def import_outline_text(
    content: bytes,
    *,
    default_name: str = "WBS importada",
) -> dict[str, Any]:
    text = content.decode("utf-8-sig", errors="replace")
    raw: list[dict[str, Any]] = []
    stack: list[tuple[int, str]] = []

    for index, line in enumerate(text.splitlines(), start=1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue

        indent = len(line) - len(line.lstrip(" \t"))
        clean = re.sub(r"^[-*+]\s+", "", line.strip())
        code_match = CODE_LINE_RE.match(clean)

        if code_match:
            code = code_match.group("code")
            raw.append(
                {
                    "code": code,
                    "parent_code": _parent_code(code),
                    "name": code_match.group("name").strip(),
                    "order": index,
                }
            )
            continue

        depth = max(0, indent // 2)
        while stack and stack[-1][0] >= depth:
            stack.pop()

        identifier = _slug_id()
        raw.append(
            {
                "id": identifier,
                "parent_id": stack[-1][1] if stack else "",
                "name": clean,
                "order": index,
            }
        )
        stack.append((depth, identifier))

    if not raw:
        raise ValueError(
            "Não foi possível identificar itens de WBS no arquivo textual."
        )

    return {
        "name": default_name,
        "description": "",
        "nodes": normalize_nodes(raw),
        "warnings": [],
    }



def export_pdf(record: dict[str, Any]) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    payload = _payload(record); buffer = io.BytesIO(); styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(A4), rightMargin=10*mm, leftMargin=10*mm, topMargin=12*mm, bottomMargin=12*mm, title=payload["name"])
    story = [Paragraph(escape(payload["name"]), styles["Title"])]
    if payload["description"]: story += [Paragraph(escape(payload["description"]), styles["BodyText"]), Spacer(1, 5*mm)]
    rows = [["Código", "Pacote / Entrega", "Responsável", "Status", "Progresso", "Custo"]]
    for node in payload["nodes"]:
        label = node["name"] + (f" / {node['deliverable']}" if node["deliverable"] else "")
        rows.append([node["code"], label, node["owner"], STATUS_LABELS.get(node["status"], node["status"]), f"{node['progress_percent']:.0f}%", f"{node['cost']:.2f}"])
    table = Table(rows, repeatRows=1, colWidths=[24*mm, 105*mm, 42*mm, 32*mm, 25*mm, 30*mm])
    table.setStyle(TableStyle([("BACKGROUND", (0,0), (-1,0), colors.HexColor("#EDEDED")), ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"), ("GRID", (0,0), (-1,-1), .35, colors.HexColor("#B0B0B0")), ("VALIGN", (0,0), (-1,-1), "TOP"), ("FONTSIZE", (0,0), (-1,-1), 8)])); story.append(table); doc.build(story)
    return buffer.getvalue()


def import_pdf(content: bytes) -> dict[str, Any]:
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(content)); text = "\n".join(page.extract_text() or "" for page in reader.pages)
    if not text.strip(): raise ValueError("O PDF não possui texto extraível. PDFs apenas com imagem precisam de OCR externo.")
    result = import_outline_text(text.encode("utf-8"), default_name="WBS importada do PDF")
    result["warnings"] = ["Importação de PDF é best-effort: a hierarquia é inferida pelos códigos WBS e pela indentação do texto extraído."]
    return result


def import_wbs(filename: str, content: bytes) -> dict[str, Any]:
    extension = "." + filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
    if extension == ".json": return import_json(content)
    if extension == ".xml": return import_xml(content)
    if extension in {".xlsx", ".xls"}: return import_excel(content)
    if extension == ".csv": return import_delimited(content)
    if extension == ".tsv": return import_delimited(content, delimiter="\t")
    if extension in {".txt", ".md"}: return import_outline_text(content, default_name=filename.rsplit(".", 1)[0])
    if extension == ".pdf": return import_pdf(content)
    raise ValueError(f"Formato de importação não suportado: {extension or '(sem extensão)'}")


def export_wbs(record: dict[str, Any], fmt: str) -> tuple[bytes, str, str]:
    fmt = _text(fmt).lower().lstrip("."); name = re.sub(r"[^a-zA-Z0-9_-]+", "_", _text(record.get("name")) or "wbs").strip("_") or "wbs"
    if fmt == "json": return export_json(record), "application/json", f"{name}.json"
    if fmt == "xml": return export_xml(record), "application/xml", f"{name}.xml"
    if fmt == "xlsx": return export_excel(record), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", f"{name}.xlsx"
    if fmt == "csv": return export_delimited(record), "text/csv", f"{name}.csv"
    if fmt == "tsv": return export_delimited(record, delimiter="\t"), "text/tab-separated-values", f"{name}.tsv"
    if fmt == "pdf": return export_pdf(record), "application/pdf", f"{name}.pdf"
    if fmt == "md": return export_markdown(record), "text/markdown", f"{name}.md"
    if fmt == "txt": return export_text(record), "text/plain", f"{name}.txt"
    if fmt == "zip":
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for item in ("json", "xml", "xlsx", "csv", "tsv", "pdf", "md", "txt"):
                data, _, filename = export_wbs(record, item); archive.writestr(filename, data)
        return buffer.getvalue(), "application/zip", f"{name}_completo.zip"
    raise ValueError(f"Formato de exportação não suportado: {fmt}")
