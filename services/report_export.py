from __future__ import annotations

import csv
import html
import io
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from services.flow_analytics import analyze_document, build_raci_rows


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
<div class='score'>{analysis['quality_score']}/100</div><small>Índice de qualidade do processo</small>
<p>Elementos: {analysis['counts']['nodes']} · Conexões: {analysis['counts']['edges']} · Raias: {analysis['counts']['lanes']} · Decisões: {analysis['counts']['decisions']} · SLA estimado: {analysis['total_sla_minutes']:.0f} min</p>
<table><thead><tr><th>Etapa</th><th>Tipo</th><th>Raia</th><th>Responsável</th><th>Criticidade</th><th>SLA</th><th>Descrição</th></tr></thead><tbody>{''.join(rows)}</tbody></table>
</body></html>"""
    return page.encode("utf-8")


def pdf_report(document: dict[str, Any]) -> bytes:
    flow = document.get("flow", {})
    analysis = analyze_document(document)
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(A4), rightMargin=12*mm, leftMargin=12*mm, topMargin=12*mm, bottomMargin=12*mm)
    styles = getSampleStyleSheet()
    story = [
        Paragraph(str(flow.get("name") or "Processo"), styles["Title"]),
        Paragraph(str(flow.get("description") or ""), styles["BodyText"]),
        Spacer(1, 8),
        Paragraph(f"Qualidade: <b>{analysis['quality_score']}/100</b> | Elementos: {analysis['counts']['nodes']} | Conexões: {analysis['counts']['edges']} | Raias: {analysis['counts']['lanes']} | SLA: {analysis['total_sla_minutes']:.0f} min", styles["BodyText"]),
        Spacer(1, 10),
    ]
    lane_names = {lane.get("id"): lane.get("name", "Sem raia") for lane in document.get("lanes", [])}
    table_data = [["Etapa", "Tipo", "Raia", "Responsável", "Criticidade", "SLA", "Descrição"]]
    for node in document.get("nodes", []):
        data = node.get("data", {})
        table_data.append([
            Paragraph(str(data.get("label") or ""), styles["BodyText"]), node.get("type", ""),
            lane_names.get(node.get("laneId"), ""), data.get("owner", ""),
            data.get("criticality", ""), str(data.get("slaMinutes") or ""),
            Paragraph(str(data.get("description") or ""), styles["BodyText"]),
        ])
    table = Table(table_data, repeatRows=1, colWidths=[47*mm, 20*mm, 36*mm, 33*mm, 22*mm, 16*mm, 83*mm])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#00684A")),
        ("TEXTCOLOR", (0,0), (-1,0), colors.white),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("GRID", (0,0), (-1,-1), .35, colors.HexColor("#D9E2EC")),
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("FONTSIZE", (0,0), (-1,-1), 7),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#F7FAFC")]),
        ("LEFTPADDING", (0,0), (-1,-1), 4), ("RIGHTPADDING", (0,0), (-1,-1), 4),
    ]))
    story.append(table)
    if document.get("nodes"):
        story.extend([PageBreak(), Paragraph("Matriz RACI", styles["Heading1"])])
        raci = build_raci_rows(document)
        headers = list(raci[0]) if raci else ["Etapa"]
        data = [headers] + [[row.get(header, "") for header in headers] for row in raci]
        raci_table = Table(data, repeatRows=1)
        raci_table.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#00684A")), ("TEXTCOLOR", (0,0), (-1,0), colors.white),
            ("GRID", (0,0), (-1,-1), .35, colors.HexColor("#D9E2EC")), ("FONTSIZE", (0,0), (-1,-1), 7),
            ("VALIGN", (0,0), (-1,-1), "TOP"),
        ]))
        story.append(raci_table)
    doc.build(story)
    return buffer.getvalue()
