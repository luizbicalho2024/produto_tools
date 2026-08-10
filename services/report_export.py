from __future__ import annotations

import csv
import html
import io
import json
import math
import re
import zipfile
from datetime import datetime
from typing import Any

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A3, A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas
from reportlab.platypus import (
    Flowable,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from services.flow_analytics import analyze_document, build_raci_rows, issue_detail_rows

NODE_TYPES = {
    "start": {"label": "Inicio", "color": "#059669", "width": 160},
    "end": {"label": "Fim", "color": "#dc2626", "width": 160},
    "task": {"label": "Atividade", "color": "#2563eb", "width": 184},
    "decision": {"label": "Decisao", "color": "#d97706", "width": 174},
    "subprocess": {"label": "Subprocesso", "color": "#7c3aed", "width": 184},
    "event": {"label": "Evento", "color": "#0891b2", "width": 170},
    "wait": {"label": "Espera", "color": "#475569", "width": 170},
    "document": {"label": "Documento", "color": "#0f766e", "width": 184},
    "api": {"label": "Integracao", "color": "#9333ea", "width": 184},
    "note": {"label": "Observacao", "color": "#ca8a04", "width": 184},
}
NODE_HEIGHT = 72


def _safe_text(value: Any) -> str:
    text = str(value or "")
    return (
        text.replace("—", "-")
        .replace("–", "-")
        .replace("“", '"')
        .replace("”", '"')
        .replace("’", "'")
        .replace("…", "...")
    )


def _slug(value: Any) -> str:
    text = _safe_text(value).lower().strip()
    replacements = str.maketrans("áàãâäéèêëíìîïóòõôöúùûüç", "aaaaaeeeeiiiiooooouuuuc")
    text = text.translate(replacements)
    text = re.sub(r"[^a-z0-9_-]+", "_", text).strip("_")
    return text or "fluxo"


def _node_dimensions(node: dict[str, Any]) -> tuple[float, float]:
    meta = NODE_TYPES.get(str(node.get("type") or "task"), NODE_TYPES["task"])
    return float(meta["width"]), float(NODE_HEIGHT)


def _lane_geometry(document: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, dict[str, float]], float]:
    lanes = sorted(document.get("lanes", []), key=lambda lane: float(lane.get("order") or 0))
    top = 0.0
    geometry: dict[str, dict[str, float]] = {}
    for lane in lanes:
        height = 48.0 if lane.get("collapsed") else min(1200.0, max(110.0, float(lane.get("height") or 240)))
        lane_id = str(lane.get("id") or "")
        geometry[lane_id] = {"top": top, "height": height, "bottom": top + height}
        top += height
    return lanes, geometry, top


def _flow_bounds(document: dict[str, Any]) -> tuple[float, float]:
    _, _, lane_height = _lane_geometry(document)
    max_x = 900.0
    max_y = max(500.0, lane_height)
    for node in document.get("nodes", []):
        width, height = _node_dimensions(node)
        position = node.get("position") or {}
        max_x = max(max_x, float(position.get("x") or 0) + width)
        max_y = max(max_y, float(position.get("y") or 0) + height)
    return max_x + 120.0, max_y + 80.0


def _mix_with_white(hex_color: str, amount: float = 0.82) -> colors.Color:
    try:
        base = colors.HexColor(hex_color)
    except Exception:
        base = colors.HexColor("#eef2ff")
    return colors.Color(
        base.red * (1 - amount) + amount,
        base.green * (1 - amount) + amount,
        base.blue * (1 - amount) + amount,
    )


def _truncate(text: Any, font: str, size: float, max_width: float) -> str:
    value = _safe_text(text).strip()
    if not value:
        return ""
    if stringWidth(value, font, size) <= max_width:
        return value
    suffix = "..."
    while value and stringWidth(value + suffix, font, size) > max_width:
        value = value[:-1]
    return value.rstrip() + suffix




def _wrap_text_lines(text: Any, font: str, size: float, max_width: float, max_lines: int) -> list[str]:
    value = _safe_text(text).strip()
    if not value or max_width <= 2 or max_lines <= 0:
        return []
    words = value.replace("\n", " ").split()
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = word if not current else f"{current} {word}"
        if stringWidth(candidate, font, size) <= max_width:
            current = candidate
            continue
        if current:
            lines.append(current)
            current = word
        else:
            lines.append(_truncate(word, font, size, max_width))
            current = ""
        if len(lines) >= max_lines:
            break
    if current and len(lines) < max_lines:
        lines.append(current)
    if len(lines) == max_lines and words:
        consumed = " ".join(lines)
        if len(consumed) < len(value):
            lines[-1] = _truncate(lines[-1] + "...", font, size, max_width)
    return lines

def _decision_semantic(edge: dict[str, Any], source: dict[str, Any] | None, target: dict[str, Any] | None) -> str:
    if not source or str(source.get("type")) != "decision":
        return "neutral"
    target_data = (target or {}).get("data") or {}
    text = " ".join(
        [
            _safe_text(edge.get("label")),
            _safe_text(edge.get("condition")),
            _safe_text(target_data.get("label")),
            " ".join(map(_safe_text, target_data.get("tags") or [])),
        ]
    ).lower()
    negatives = (
        "não", "nao", "negativo", "recus", "rejeit", "falha", "erro", "cancel",
        "inválid", "invalid", "expir", "indispon", "reprov", "sem ", "bloque",
    )
    positives = (
        "sim", "positivo", "aprov", "aceit", "válid", "valid", "conclu", "dispon",
        "pago", "sucesso", "ativo", "permit", "ok",
    )
    if any(token in text for token in negatives):
        return "negative"
    if any(token in text for token in positives):
        return "positive"
    return "neutral"


def _draw_arrow(c: canvas.Canvas, x: float, y: float, direction_x: float, color: colors.Color, size: float) -> None:
    direction = 1.0 if direction_x >= 0 else -1.0
    c.setFillColor(color)
    path = c.beginPath()
    path.moveTo(x, y)
    path.lineTo(x - direction * size, y + size * 0.55)
    path.lineTo(x - direction * size, y - size * 0.55)
    path.close()
    c.drawPath(path, fill=1, stroke=0)


def _draw_flow_diagram(
    c: canvas.Canvas,
    document: dict[str, Any],
    x: float,
    y: float,
    width: float,
    height: float,
    *,
    show_title: bool = False,
) -> None:
    world_w, world_h = _flow_bounds(document)
    title_height = 22.0 if show_title else 0.0
    available_h = max(20.0, height - title_height)
    scale = min(width / world_w, available_h / world_h)
    scale = max(scale, 0.001)
    draw_w = world_w * scale
    draw_h = world_h * scale
    ox = x + max(0.0, (width - draw_w) / 2)
    oy = y + max(0.0, (available_h - draw_h) / 2)

    if show_title:
        c.setFont("Helvetica-Bold", 10)
        c.setFillColor(colors.HexColor("#102a43"))
        c.drawString(x, y + height - 13, _truncate((document.get("flow") or {}).get("name"), "Helvetica-Bold", 10, width))

    def px(value: float) -> float:
        return ox + value * scale

    def py(value: float) -> float:
        return oy + draw_h - value * scale

    lanes, lane_map, _ = _lane_geometry(document)
    for lane in lanes:
        box = lane_map.get(str(lane.get("id") or ""))
        if not box:
            continue
        lane_y = py(box["top"] + box["height"])
        lane_h = box["height"] * scale
        c.setFillColor(_mix_with_white(str(lane.get("color") or "#eef2ff"), 0.78))
        c.setStrokeColor(colors.HexColor("#b8c8d8"))
        c.setLineWidth(max(0.3, 0.8 * scale))
        c.roundRect(px(18), lane_y, max(1, (world_w - 36) * scale), lane_h, max(2, 10 * scale), fill=1, stroke=1)
        if lane_h >= 14:
            c.setFillColor(colors.HexColor("#102a43"))
            c.setFont("Helvetica-Bold", max(4.5, min(8.5, 11 * scale)))
            c.drawString(px(31), py(box["top"] + 24), _truncate(lane.get("name"), "Helvetica-Bold", max(4.5, min(8.5, 11 * scale)), max(20, (world_w - 90) * scale)))

    nodes = {str(node.get("id")): node for node in document.get("nodes", []) if (node.get("data") or {}).get("enabled", True) is not False}
    routing = str((document.get("settings") or {}).get("edgeRouting") or "smooth")
    for edge in document.get("edges", []):
        if edge.get("enabled", True) is False:
            continue
        source = nodes.get(str(edge.get("source") or ""))
        target = nodes.get(str(edge.get("target") or ""))
        if not source or not target:
            continue
        sw, sh = _node_dimensions(source)
        tw, th = _node_dimensions(target)
        sp = source.get("position") or {}
        tp = target.get("position") or {}
        sx = float(sp.get("x") or 0) + sw + 3
        sy = float(sp.get("y") or 0) + sh / 2
        tx = float(tp.get("x") or 0) - 6
        ty = float(tp.get("y") or 0) + th / 2
        x1, y1, x2, y2 = px(sx), py(sy), px(tx), py(ty)
        semantic = _decision_semantic(edge, source, target)
        edge_color = colors.HexColor("#16a34a" if semantic == "positive" else "#dc2626" if semantic == "negative" else "#536b7c")
        c.setStrokeColor(edge_color)
        c.setLineWidth(max(0.45, min(1.8, 2.0 * scale)))
        path = c.beginPath()
        path.moveTo(x1, y1)
        if routing == "straight":
            path.lineTo(x2, y2)
        elif routing == "smooth":
            delta = max(20 * scale, abs(x2 - x1) * 0.42)
            direction = 1 if x2 >= x1 else -1
            path.curveTo(x1 + direction * delta, y1, x2 - direction * delta, y2, x2, y2)
        else:
            mid_x = (x1 + x2) / 2
            path.lineTo(mid_x, y1)
            path.lineTo(mid_x, y2)
            path.lineTo(x2, y2)
        c.drawPath(path, fill=0, stroke=1)
        _draw_arrow(c, x2, y2, x2 - x1, edge_color, max(2.2, min(6.0, 7.0 * scale)))

        edge_label = _safe_text(edge.get("label") or edge.get("condition")).strip()
        if edge_label and scale > 0.035:
            label_size = max(4.2, min(7.0, 8.5 * scale))
            label = _truncate(edge_label, "Helvetica", label_size, max(25, abs(x2 - x1) * 0.55))
            if label:
                mx, my = (x1 + x2) / 2, (y1 + y2) / 2
                label_w = stringWidth(label, "Helvetica", label_size) + 5
                c.setFillColor(colors.white)
                c.roundRect(mx - label_w / 2, my - 4, label_w, 9, 2, fill=1, stroke=0)
                c.setFillColor(edge_color)
                c.setFont("Helvetica", label_size)
                c.drawCentredString(mx, my - 1.5, label)

    for node in nodes.values():
        node_type = str(node.get("type") or "task")
        meta = NODE_TYPES.get(node_type, NODE_TYPES["task"])
        nw, nh = _node_dimensions(node)
        pos = node.get("position") or {}
        nx = px(float(pos.get("x") or 0))
        ny = py(float(pos.get("y") or 0) + nh)
        nwp, nhp = nw * scale, nh * scale
        border = colors.HexColor(meta["color"])
        c.setFillColor(colors.white)
        c.setStrokeColor(border)
        c.setLineWidth(max(0.5, min(1.5, 1.6 * scale)))
        radius = max(2, min(8, 10 * scale))
        c.roundRect(nx, ny, nwp, nhp, radius, fill=1, stroke=1)
        c.setFillColor(border)
        c.roundRect(nx + 8 * scale, ny + nhp - max(2, 4 * scale), max(3, nwp - 16 * scale), max(1.5, 4 * scale), max(1, 2 * scale), fill=1, stroke=0)

        if scale >= 0.045:
            data = node.get("data") or {}
            title_size = max(4.2, min(8.2, 10.0 * scale))
            description_size = max(3.5, min(6.1, 7.4 * scale))
            owner_size = max(3.5, min(6.0, 7.6 * scale))
            max_text_w = max(10, nwp - 14 * scale)
            text_x = nx + 7 * scale
            top_y = ny + nhp - max(7.0, 16 * scale)

            title_lines = _wrap_text_lines(data.get("label"), "Helvetica-Bold", title_size, max_text_w, 2)
            c.setFillColor(colors.HexColor("#102a43"))
            c.setFont("Helvetica-Bold", title_size)
            line_y = top_y
            for line in title_lines:
                c.drawString(text_x, line_y, line)
                line_y -= title_size * 1.12

            # A descrição é a mensagem principal do card. Em fluxos grandes ela continua
            # vetorial; ao ampliar o PDF, o texto permanece nítido.
            description = _safe_text(data.get("description")).strip()
            owner_floor = ny + max(4.0, 8 * scale) + owner_size
            available_description_h = max(0.0, line_y - owner_floor - 1.5)
            max_description_lines = max(0, min(3, int(available_description_h / max(description_size * 1.12, 1))))
            if description and max_description_lines:
                description_lines = _wrap_text_lines(description, "Helvetica", description_size, max_text_w, max_description_lines)
                c.setFillColor(colors.HexColor("#334e68"))
                c.setFont("Helvetica", description_size)
                for line in description_lines:
                    c.drawString(text_x, line_y, line)
                    line_y -= description_size * 1.12

            c.setFillColor(colors.HexColor("#486581"))
            c.setFont("Helvetica", owner_size)
            subtitle = _safe_text(data.get("owner") or meta["label"])
            c.drawString(text_x, ny + max(4.0, 7 * scale), _truncate(subtitle, "Helvetica", owner_size, max_text_w))

        dot = max(1.2, min(3.2, 3.8 * scale))
        c.setFillColor(colors.HexColor("#64748b"))
        c.circle(nx - dot * 0.8, ny + nhp / 2, dot, fill=1, stroke=0)
        c.circle(nx + nwp + dot * 0.8, ny + nhp / 2, dot, fill=1, stroke=0)


class FlowDiagramFlowable(Flowable):
    def __init__(self, document: dict[str, Any], width: float, height: float):
        super().__init__()
        self.document = document
        self.width = width
        self.height = height

    def wrap(self, avail_width: float, avail_height: float) -> tuple[float, float]:
        return min(self.width, avail_width), min(self.height, avail_height)

    def draw(self) -> None:
        _draw_flow_diagram(self.canv, self.document, 0, 0, self.width, self.height)


def nodes_csv(document: dict[str, Any]) -> bytes:
    lane_names = {lane.get("id"): lane.get("name", "") for lane in document.get("lanes", [])}
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["id", "tipo", "nome", "raia", "responsavel", "criticidade", "nivel", "sla_minutos", "descricao", "tags"])
    writer.writeheader()
    for node in document.get("nodes", []):
        data = node.get("data", {})
        writer.writerow({
            "id": node.get("id", ""), "tipo": node.get("type", ""),
            "nome": data.get("label", ""), "raia": lane_names.get(node.get("laneId"), ""),
            "responsavel": data.get("owner", ""), "criticidade": data.get("criticality", ""),
            "nivel": data.get("level", ""), "sla_minutos": data.get("slaMinutes") or "",
            "descricao": data.get("description", ""), "tags": ", ".join(data.get("tags", [])),
        })
    return buffer.getvalue().encode("utf-8-sig")


def raci_csv(document: dict[str, Any]) -> bytes:
    rows = build_raci_rows(document)
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0]) if rows else ["Etapa"])
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8-sig")


def html_report(document: dict[str, Any]) -> bytes:
    analysis = analyze_document(document)
    flow = document.get("flow", {})
    lane_names = {lane.get("id"): lane.get("name", "Sem raia") for lane in document.get("lanes", [])}
    rows = []
    for node in document.get("nodes", []):
        data = node.get("data", {})
        rows.append(
            "<tr>" + "".join(f"<td>{html.escape(str(value or ''))}</td>" for value in [
                data.get("label"), node.get("type"), lane_names.get(node.get("laneId")),
                data.get("owner"), data.get("criticality"), data.get("slaMinutes"), data.get("description"),
            ]) + "</tr>"
        )
    page = f"""<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'><title>{html.escape(str(flow.get('name')))}</title>
<style>body{{font-family:Arial,sans-serif;margin:32px;color:#102a43}}h1{{color:#00684a}}.score{{font-size:32px;font-weight:700;color:#00a35c}}table{{width:100%;border-collapse:collapse;margin-top:18px}}th,td{{border:1px solid #d9e2ec;padding:7px;text-align:left;vertical-align:top}}th{{background:#e8f5f0}}small{{color:#486581}}</style></head><body>
<h1>{html.escape(str(flow.get('name') or 'Processo'))}</h1><p>{html.escape(str(flow.get('description') or ''))}</p>
<div class='score'>{analysis['quality_score']}/100</div><small>Indice de qualidade do processo</small>
<p>Elementos: {analysis['counts']['nodes']} · Conexoes: {analysis['counts']['edges']} · Raias: {analysis['counts']['lanes']} · Decisoes: {analysis['counts']['decisions']} · SLA estimado: {analysis['total_sla_minutes']:.0f} min</p>
<table><thead><tr><th>Etapa</th><th>Tipo</th><th>Raia</th><th>Responsavel</th><th>Criticidade</th><th>SLA</th><th>Descricao</th></tr></thead><tbody>{''.join(rows)}</tbody></table>
</body></html>"""
    return page.encode("utf-8")




def _draw_flow_card_detail_page(
    c: canvas.Canvas,
    document: dict[str, Any],
    lane: dict[str, Any],
    nodes: list[dict[str, Any]],
    page_number: int,
    page_total: int,
) -> None:
    page_w, page_h = landscape(A3)
    c.setPageSize((page_w, page_h))
    c.setFillColor(colors.white)
    c.rect(0, 0, page_w, page_h, fill=1, stroke=0)
    margin = 12 * mm
    c.setFillColor(colors.HexColor("#102a43"))
    c.setFont("Helvetica-Bold", 15)
    c.drawString(margin, page_h - margin - 4, _truncate(lane.get("name") or "Sem raia", "Helvetica-Bold", 15, page_w - 2 * margin - 130))
    c.setFont("Helvetica", 8)
    c.setFillColor(colors.HexColor("#526d82"))
    c.drawRightString(page_w - margin, page_h - margin - 2, f"Detalhe dos cards {page_number}/{page_total}")
    c.setFont("Helvetica", 7.5)
    c.drawString(margin, page_h - margin - 18, "Os cards abaixo preservam título, mensagem/descrição e contexto das conexões para leitura e impressão.")

    node_map = {str(node.get("id") or ""): node for node in document.get("nodes", [])}
    edges = [edge for edge in document.get("edges", []) if edge.get("enabled", True) is not False]
    cols = 2
    rows = 5
    gap_x = 8 * mm
    gap_y = 6 * mm
    top = page_h - margin - 30
    bottom = margin
    usable_h = top - bottom
    card_h = (usable_h - gap_y * (rows - 1)) / rows
    card_w = (page_w - 2 * margin - gap_x) / cols

    for index, node in enumerate(nodes[: cols * rows]):
        row = index // cols
        col = index % cols
        x = margin + col * (card_w + gap_x)
        y = top - (row + 1) * card_h - row * gap_y
        data = node.get("data") or {}
        meta = NODE_TYPES.get(str(node.get("type") or "task"), NODE_TYPES["task"])
        border = colors.HexColor(meta["color"])
        c.setFillColor(colors.HexColor("#fbfdff"))
        c.setStrokeColor(border)
        c.setLineWidth(1.1)
        c.roundRect(x, y, card_w, card_h, 7, fill=1, stroke=1)
        c.setFillColor(border)
        c.roundRect(x + 8, y + card_h - 5, card_w - 16, 4, 2, fill=1, stroke=0)

        criticality = _safe_text(data.get("criticality") or "medium").lower()
        criticality_label = {"critical": "CRÍTICO", "high": "ALTO", "medium": "MÉDIO", "low": "BAIXO"}.get(criticality, criticality.upper())
        if criticality in {"critical", "high"}:
            badge_color = colors.HexColor("#b91c1c" if criticality == "critical" else "#b45309")
            badge_w = stringWidth(criticality_label, "Helvetica-Bold", 6.5) + 12
            c.setFillColor(badge_color)
            c.roundRect(x + card_w - badge_w - 8, y + card_h - 19, badge_w, 11, 5, fill=1, stroke=0)
            c.setFillColor(colors.white)
            c.setFont("Helvetica-Bold", 6.5)
            c.drawCentredString(x + card_w - badge_w / 2 - 8, y + card_h - 15.5, criticality_label)

        c.setFillColor(colors.HexColor("#102a43"))
        c.setFont("Helvetica-Bold", 10)
        title_lines = _wrap_text_lines(data.get("label") or node.get("id"), "Helvetica-Bold", 10, card_w - 26, 2)
        line_y = y + card_h - 22
        for line in title_lines:
            c.drawString(x + 10, line_y, line)
            line_y -= 11.5

        c.setFillColor(colors.HexColor("#526d82"))
        c.setFont("Helvetica", 7)
        context = f"{meta['label']} · {data.get('owner') or 'Sem responsável'} · ID {node.get('id') or '-'}"
        c.drawString(x + 10, line_y - 1, _truncate(context, "Helvetica", 7, card_w - 20))
        line_y -= 13

        c.setFillColor(colors.HexColor("#243b53"))
        c.setFont("Helvetica", 7.7)
        description_lines = _wrap_text_lines(data.get("description") or "Sem descrição cadastrada.", "Helvetica", 7.7, card_w - 20, 5)
        for line in description_lines:
            c.drawString(x + 10, line_y, line)
            line_y -= 9.2

        incoming = [edge for edge in edges if str(edge.get("target") or "") == str(node.get("id") or "")]
        outgoing = [edge for edge in edges if str(edge.get("source") or "") == str(node.get("id") or "")]
        incoming_names = [((node_map.get(str(edge.get("source") or ""), {}).get("data") or {}).get("label") or str(edge.get("source") or "")) for edge in incoming]
        outgoing_names = [((node_map.get(str(edge.get("target") or ""), {}).get("data") or {}).get("label") or str(edge.get("target") or "")) for edge in outgoing]
        c.setFont("Helvetica", 6.6)
        c.setFillColor(colors.HexColor("#486581"))
        receives = "Recebe de: " + (", ".join(map(_safe_text, incoming_names[:3])) if incoming_names else "-")
        sends = "Envia para: " + (", ".join(map(_safe_text, outgoing_names[:3])) if outgoing_names else "-")
        c.drawString(x + 10, y + 18, _truncate(receives, "Helvetica", 6.6, card_w - 20))
        c.drawString(x + 10, y + 8, _truncate(sends, "Helvetica", 6.6, card_w - 20))


def _append_dense_flow_detail_pages(c: canvas.Canvas, document: dict[str, Any], scale: float) -> None:
    enabled_nodes = [node for node in document.get("nodes", []) if (node.get("data") or {}).get("enabled", True) is not False]
    if scale >= 0.55 and len(enabled_nodes) <= 70:
        return
    lanes, _, _ = _lane_geometry(document)
    nodes_by_lane: dict[str, list[dict[str, Any]]] = {}
    for node in enabled_nodes:
        nodes_by_lane.setdefault(str(node.get("laneId") or ""), []).append(node)
    ordered_lanes = list(lanes)
    if nodes_by_lane.get(""):
        ordered_lanes.append({"id": "", "name": "Sem raia"})
    cards_per_page = 10
    for lane in ordered_lanes:
        lane_nodes = sorted(
            nodes_by_lane.get(str(lane.get("id") or ""), []),
            key=lambda node: (float((node.get("position") or {}).get("x") or 0), float((node.get("position") or {}).get("y") or 0)),
        )
        if not lane_nodes:
            continue
        total = math.ceil(len(lane_nodes) / cards_per_page)
        for page_index in range(total):
            c.showPage()
            chunk = lane_nodes[page_index * cards_per_page : (page_index + 1) * cards_per_page]
            _draw_flow_card_detail_page(c, document, lane, chunk, page_index + 1, total)


def flow_only_pdf(document: dict[str, Any], metadata: dict[str, Any] | None = None) -> bytes:
    """PDF vetorial com visão geral e detalhe legível dos cards em fluxos densos."""
    flow = document.get("flow", {})
    world_w, world_h = _flow_bounds(document)
    margin = 26.0
    header = 34.0
    max_pdf_dimension = 14000.0
    desired_scale = 0.72
    scale = min(
        desired_scale,
        (max_pdf_dimension - margin * 2) / max(world_w, 1),
        (max_pdf_dimension - margin * 2 - header) / max(world_h, 1),
    )
    scale = max(scale, 0.04)
    page_w = min(max_pdf_dimension, world_w * scale + margin * 2)
    page_h = min(max_pdf_dimension, world_h * scale + margin * 2 + header)
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=(page_w, page_h), pageCompression=1)
    c.setTitle(_safe_text(flow.get("name") or "Fluxo"))
    c.setAuthor("Produto Tools")
    c.setFillColor(colors.white)
    c.rect(0, 0, page_w, page_h, fill=1, stroke=0)
    c.setFillColor(colors.HexColor("#102a43"))
    c.setFont("Helvetica-Bold", 14)
    c.drawString(margin, page_h - margin - 4, _truncate(flow.get("name") or "Fluxo", "Helvetica-Bold", 14, page_w - margin * 2 - 180))
    c.setFont("Helvetica", 7.5)
    c.setFillColor(colors.HexColor("#526d82"))
    version = (metadata or {}).get("current_version")
    revision = (metadata or {}).get("revision")
    stamp = datetime.now().strftime("%d/%m/%Y %H:%M")
    detail = f"Gerado em {stamp}"
    if version:
        detail += f" | versao {version}"
    if revision:
        detail += f" | revisao {revision}"
    c.drawRightString(page_w - margin, page_h - margin - 3, detail)
    _draw_flow_diagram(c, document, margin, margin, page_w - margin * 2, page_h - margin * 2 - header)
    # Para fluxos densos, o PDF mantém a visão geral vetorial e acrescenta páginas
    # de detalhe por raia. Assim as mensagens/descrições dos cards permanecem legíveis.
    _append_dense_flow_detail_pages(c, document, scale)
    c.showPage()
    c.save()
    return buffer.getvalue()


def _paragraph(value: Any, style: ParagraphStyle) -> Paragraph:
    return Paragraph(html.escape(_safe_text(value)).replace("\n", "<br/>"), style)


def _footer(canvas_obj: canvas.Canvas, doc_obj, flow_name: str) -> None:
    canvas_obj.saveState()
    canvas_obj.setStrokeColor(colors.HexColor("#d9e2ec"))
    canvas_obj.line(12 * mm, 8 * mm, landscape(A4)[0] - 12 * mm, 8 * mm)
    canvas_obj.setFont("Helvetica", 6.5)
    canvas_obj.setFillColor(colors.HexColor("#607d8b"))
    canvas_obj.drawString(12 * mm, 4.8 * mm, _truncate(flow_name, "Helvetica", 6.5, 190 * mm))
    canvas_obj.drawRightString(landscape(A4)[0] - 12 * mm, 4.8 * mm, f"Pagina {doc_obj.page}")
    canvas_obj.restoreState()


def full_documentation_pdf(document: dict[str, Any], metadata: dict[str, Any] | None = None) -> bytes:
    """PDF paginado com diagrama, inventario das etapas, decisoes, conexoes, qualidade e RACI."""
    flow = document.get("flow", {})
    analysis = analyze_document(document)
    issues = issue_detail_rows(document, analysis)
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        rightMargin=12 * mm,
        leftMargin=12 * mm,
        topMargin=12 * mm,
        bottomMargin=13 * mm,
        title=_safe_text(flow.get("name") or "Processo"),
        author="Produto Tools",
    )
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="PTSection", parent=styles["Heading1"], fontName="Helvetica-Bold", fontSize=15, leading=18, textColor=colors.HexColor("#00684a"), spaceAfter=8))
    styles.add(ParagraphStyle(name="PTSub", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=10, leading=12, textColor=colors.HexColor("#102a43"), spaceBefore=6, spaceAfter=4))
    styles.add(ParagraphStyle(name="PTBody", parent=styles["BodyText"], fontName="Helvetica", fontSize=8.2, leading=11, textColor=colors.HexColor("#243b53")))
    styles.add(ParagraphStyle(name="PTSmall", parent=styles["BodyText"], fontName="Helvetica", fontSize=6.6, leading=8.5, textColor=colors.HexColor("#486581")))
    styles.add(ParagraphStyle(name="PTCover", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=24, leading=28, textColor=colors.HexColor("#00684a"), alignment=TA_CENTER, spaceAfter=12))

    story: list[Any] = []
    story.append(Spacer(1, 16 * mm))
    story.append(_paragraph(flow.get("name") or "Processo", styles["PTCover"]))
    if flow.get("description"):
        story.append(_paragraph(flow.get("description"), ParagraphStyle(name="CoverDesc", parent=styles["PTBody"], alignment=TA_CENTER, fontSize=10, leading=14)))
    story.append(Spacer(1, 8 * mm))

    status = _safe_text((metadata or {}).get("workflow_status") or flow.get("status") or "draft")
    meta_rows = [
        ["Documento", "Documentacao completa do fluxo", "Status", status],
        ["Versao", _safe_text((metadata or {}).get("current_version") or "-"), "Revisao", _safe_text((metadata or {}).get("revision") or "-")],
        ["Responsavel", _safe_text((metadata or {}).get("owner_username") or flow.get("createdBy") or "-"), "Gerado em", datetime.now().strftime("%d/%m/%Y %H:%M")],
    ]
    meta_table = Table(meta_rows, colWidths=[28 * mm, 78 * mm, 25 * mm, 82 * mm])
    meta_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#e8f5f0")),
        ("BACKGROUND", (2, 0), (2, -1), colors.HexColor("#e8f5f0")),
        ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#243b53")),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#d9e2ec")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    story.append(meta_table)
    story.append(PageBreak())

    story.append(Paragraph("Resumo executivo", styles["PTSection"]))
    counts = analysis["counts"]
    metric_data = [
        ["Qualidade", "Elementos", "Conexoes", "Raias", "Decisoes", "SLA estimado"],
        [f"{analysis['quality_score']}/100", counts.get("nodes", 0), counts.get("edges", 0), counts.get("lanes", 0), counts.get("decisions", 0), f"{analysis['total_sla_minutes']:.0f} min"],
    ]
    metric_table = Table(metric_data, colWidths=[38 * mm] * 6)
    metric_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#00684a")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#d9e2ec")),
        ("BACKGROUND", (0, 1), (-1, 1), colors.HexColor("#f7fafc")),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
    ]))
    story.append(metric_table)
    story.append(Spacer(1, 5 * mm))
    story.append(Paragraph("Visao do fluxo", styles["PTSub"]))
    story.append(Paragraph("O diagrama abaixo e uma visao geral. Para zoom detalhado e impressao em plotter, use o arquivo PDF - somente fluxo.", styles["PTSmall"]))
    story.append(Spacer(1, 2 * mm))
    story.append(FlowDiagramFlowable(document, 260 * mm, 135 * mm))
    story.append(PageBreak())

    story.append(Paragraph("Raias e responsabilidades", styles["PTSection"]))
    lane_data: list[list[Any]] = [["Ordem", "Raia", "Responsavel", "Altura", "Status"]]
    for lane in sorted(document.get("lanes", []), key=lambda item: float(item.get("order") or 0)):
        lane_data.append([
            _safe_text(lane.get("order")),
            _paragraph(lane.get("name"), styles["PTSmall"]),
            _paragraph(lane.get("owner") or "-", styles["PTSmall"]),
            f"{float(lane.get('height') or 0):.0f}px",
            "Ativa" if lane.get("enabled", True) is not False else "Inativa",
        ])
    lane_table = Table(lane_data, repeatRows=1, colWidths=[16 * mm, 78 * mm, 78 * mm, 25 * mm, 28 * mm])
    lane_table.setStyle(_standard_table_style())
    story.append(lane_table)
    story.append(PageBreak())

    lane_names = {str(lane.get("id") or ""): _safe_text(lane.get("name") or "Sem raia") for lane in document.get("lanes", [])}
    story.append(Paragraph("Documentacao das etapas", styles["PTSection"]))
    node_data: list[list[Any]] = [["ID", "Etapa", "Tipo", "Raia", "Responsavel", "Criticidade", "SLA", "Descricao e tags"]]
    for node in document.get("nodes", []):
        data = node.get("data") or {}
        tags = ", ".join(map(_safe_text, data.get("tags") or []))
        description = _safe_text(data.get("description"))
        combined = description + (f"\nTags: {tags}" if tags else "")
        linked = _safe_text(data.get("linkedFlowId"))
        if linked:
            combined += f"\nFluxo vinculado: {linked}"
        node_data.append([
            _paragraph(node.get("id"), styles["PTSmall"]),
            _paragraph(data.get("label"), styles["PTSmall"]),
            _safe_text(node.get("type")),
            _paragraph(lane_names.get(str(node.get("laneId") or ""), "Sem raia"), styles["PTSmall"]),
            _paragraph(data.get("owner") or "-", styles["PTSmall"]),
            _safe_text(data.get("criticality") or "-"),
            _safe_text(data.get("slaMinutes") or "-"),
            _paragraph(combined or "-", styles["PTSmall"]),
        ])
    node_table = Table(node_data, repeatRows=1, colWidths=[27 * mm, 43 * mm, 18 * mm, 34 * mm, 31 * mm, 19 * mm, 13 * mm, 70 * mm])
    node_table.setStyle(_standard_table_style(font_size=6.3))
    story.append(node_table)
    story.append(PageBreak())

    node_map = {str(node.get("id") or ""): node for node in document.get("nodes", [])}
    story.append(Paragraph("Decisoes e ramificacoes", styles["PTSection"]))
    decision_rows: list[list[Any]] = [["Decisao", "Resultado", "Destino", "Classificacao"]]
    for node in document.get("nodes", []):
        if str(node.get("type")) != "decision":
            continue
        outgoing = [edge for edge in document.get("edges", []) if str(edge.get("source") or "") == str(node.get("id") or "") and edge.get("enabled", True) is not False]
        for edge in outgoing or [{}]:
            target = node_map.get(str(edge.get("target") or ""), {})
            target_data = target.get("data") or {}
            semantic = _decision_semantic(edge, node, target)
            semantic_label = {"positive": "Positiva / Sim", "negative": "Negativa / Nao", "neutral": "Neutra"}[semantic]
            decision_rows.append([
                _paragraph((node.get("data") or {}).get("label") or node.get("id"), styles["PTSmall"]),
                _paragraph(edge.get("label") or edge.get("condition") or "Sem rotulo", styles["PTSmall"]),
                _paragraph(target_data.get("label") or edge.get("target") or "Sem destino", styles["PTSmall"]),
                semantic_label,
            ])
    if len(decision_rows) == 1:
        story.append(Paragraph("Este fluxo nao possui decisoes.", styles["PTBody"]))
    else:
        decision_table = Table(decision_rows, repeatRows=1, colWidths=[72 * mm, 62 * mm, 72 * mm, 44 * mm])
        decision_table.setStyle(_standard_table_style())
        story.append(decision_table)
    story.append(PageBreak())

    story.append(Paragraph("Conexoes do processo", styles["PTSection"]))
    edge_rows: list[list[Any]] = [["Origem", "Condicao / rotulo", "Destino", "Status"]]
    for edge in document.get("edges", []):
        source_data = (node_map.get(str(edge.get("source") or ""), {}).get("data") or {})
        target_data = (node_map.get(str(edge.get("target") or ""), {}).get("data") or {})
        edge_rows.append([
            _paragraph(source_data.get("label") or edge.get("source"), styles["PTSmall"]),
            _paragraph(edge.get("label") or edge.get("condition") or "-", styles["PTSmall"]),
            _paragraph(target_data.get("label") or edge.get("target"), styles["PTSmall"]),
            "Ativa" if edge.get("enabled", True) is not False else "Inativa",
        ])
    edge_table = Table(edge_rows, repeatRows=1, colWidths=[78 * mm, 82 * mm, 78 * mm, 26 * mm])
    edge_table.setStyle(_standard_table_style())
    story.append(edge_table)
    story.append(PageBreak())

    story.append(Paragraph("Qualidade e pontos de melhoria", styles["PTSection"]))
    if issues:
        issue_rows: list[list[Any]] = [["Gravidade", "Card", "Raia", "Problema", "Por que importa", "Como corrigir"]]
        for issue in issues:
            issue_rows.append([
                _safe_text(issue.get("Gravidade")),
                _paragraph(issue.get("Card"), styles["PTSmall"]),
                _paragraph(issue.get("Raia"), styles["PTSmall"]),
                _paragraph(issue.get("Problema"), styles["PTSmall"]),
                _paragraph(issue.get("Por que importa"), styles["PTSmall"]),
                _paragraph(issue.get("Como corrigir"), styles["PTSmall"]),
            ])
        issue_table = Table(issue_rows, repeatRows=1, colWidths=[22 * mm, 42 * mm, 38 * mm, 42 * mm, 62 * mm, 62 * mm])
        issue_table.setStyle(_standard_table_style(font_size=6.2))
        story.append(issue_table)
    else:
        story.append(Paragraph("Nenhum problema de qualidade foi identificado neste fluxo.", styles["PTBody"]))
    story.append(PageBreak())

    story.append(Paragraph("Matriz RACI", styles["PTSection"]))
    raci = build_raci_rows(document)
    if raci:
        headers = list(raci[0])
        raci_data = [headers] + [[_paragraph(row.get(header, ""), styles["PTSmall"]) for header in headers] for row in raci]
        available = 268 * mm
        raci_widths = [available / max(1, len(headers))] * len(headers)
        raci_table = Table(raci_data, repeatRows=1, colWidths=raci_widths)
        raci_table.setStyle(_standard_table_style(font_size=6.1))
        story.append(raci_table)
    else:
        story.append(Paragraph("Nao ha informacoes RACI preenchidas.", styles["PTBody"]))

    story.append(PageBreak())
    story.append(Paragraph("Configuracoes do fluxo", styles["PTSection"]))
    settings = document.get("settings") or {}
    config_rows = [["Configuracao", "Valor"]] + [[_safe_text(key), _safe_text(value)] for key, value in sorted(settings.items())]
    config_table = Table(config_rows, repeatRows=1, colWidths=[70 * mm, 180 * mm])
    config_table.setStyle(_standard_table_style())
    story.append(config_table)

    flow_name = _safe_text(flow.get("name") or "Processo")
    doc.build(
        story,
        onFirstPage=lambda c, d: _footer(c, d, flow_name),
        onLaterPages=lambda c, d: _footer(c, d, flow_name),
    )
    return buffer.getvalue()


def _standard_table_style(font_size: float = 6.8) -> TableStyle:
    return TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#00684a")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), font_size),
        ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#d9e2ec")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f7fafc")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 3.5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3.5),
        ("TOPPADDING", (0, 0), (-1, -1), 3.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3.5),
    ])


def pdf_report(document: dict[str, Any]) -> bytes:
    """Compatibilidade com chamadas antigas: o relatorio PDF agora e a documentacao completa."""
    return full_documentation_pdf(document)


def export_bundle(
    document: dict[str, Any],
    metadata: dict[str, Any] | None = None,
    *,
    diagram_pdf: bytes | None = None,
    documentation_pdf: bytes | None = None,
) -> bytes:
    flow_name = _slug((document.get("flow") or {}).get("name") or "fluxo")
    version = _safe_text((metadata or {}).get("current_version") or "")
    revision = _safe_text((metadata or {}).get("revision") or "")
    suffix = ""
    if version:
        suffix += f"_v{version}"
    if revision:
        suffix += f"_r{revision}"
    base = f"{flow_name}{suffix}"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(f"{base}.json", json.dumps(document, ensure_ascii=False, indent=2).encode("utf-8"))
        archive.writestr(f"{base}_fluxo.pdf", diagram_pdf if diagram_pdf is not None else flow_only_pdf(document, metadata))
        archive.writestr(f"{base}_documentacao_completa.pdf", documentation_pdf if documentation_pdf is not None else full_documentation_pdf(document, metadata))
        archive.writestr(f"{base}_relatorio.html", html_report(document))
        archive.writestr(f"{base}_etapas.csv", nodes_csv(document))
        archive.writestr(f"{base}_raci.csv", raci_csv(document))
        archive.writestr(
            "LEIA-ME.txt",
            (
                "Produto Tools - pacote de exportacao do fluxo\n\n"
                "Arquivos:\n"
                "- JSON: documento editavel e reimportavel.\n"
                "- *_fluxo.pdf: diagrama vetorial completo para zoom/impressao.\n"
                "- *_documentacao_completa.pdf: diagrama, etapas, decisoes, conexoes, qualidade e RACI.\n"
                "- *_relatorio.html: documentacao em HTML.\n"
                "- *_etapas.csv: inventario dos cards.\n"
                "- *_raci.csv: matriz de responsabilidades.\n"
            ).encode("utf-8"),
        )
    return buffer.getvalue()
