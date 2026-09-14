from __future__ import annotations

from collections import defaultdict
from typing import Any

import database as db
from services.flow_analytics import analyze_document, issue_detail_rows
from services.flowchart_repository import list_flowcharts

FLOW_COLLECTION = "produto_tools_flowcharts"
COMMENT_COLLECTION = "produto_tools_flowchart_comments"

def portfolio_rows(username: str, *, include_all: bool = False) -> list[dict[str, Any]]:
    summaries = list_flowcharts(username, include_all=include_all)
    ids = [item["id"] for item in summaries]
    if not ids:
        return []
    flow_collection = db.get_collection(FLOW_COLLECTION)
    comment_collection = db.get_collection(COMMENT_COLLECTION)
    if flow_collection is None:
        return []
    docs = {
        str(row.get("id")): row
        for row in flow_collection.find({"id": {"$in": ids}}, {"_id": 0, "id": 1, "document": 1})
    }
    open_comments: dict[str, int] = defaultdict(int)
    if comment_collection is not None:
        pipeline = [
            {"$match": {"flowchart_id": {"$in": ids}, "resolved": {"$ne": True}}},
            {"$group": {"_id": "$flowchart_id", "count": {"$sum": 1}}},
        ]
        for row in comment_collection.aggregate(pipeline):
            open_comments[str(row["_id"])] = int(row["count"])
    result = []
    for summary in summaries:
        stored = docs.get(str(summary["id"])) or {}
        document = stored.get("document") or {}
        analysis = analyze_document(document)
        details = issue_detail_rows(document, analysis)
        result.append({
            **summary,
            "analysis": analysis,
            "quality_details": details,
            "open_comments": open_comments.get(str(summary["id"]), 0),
        })
    return result

def build_flow_catalog(flows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ids = [str(item.get("id")) for item in flows if item.get("id")]
    collection = db.get_collection(FLOW_COLLECTION)
    documents = {}
    if collection is not None and ids:
        documents = {
            str(row.get("id")): row.get("document") or {}
            for row in collection.find({"id": {"$in": ids}}, {"_id": 0, "id": 1, "document": 1})
        }
    catalog = []
    for item in flows:
        flow_id = str(item.get("id") or "")
        doc = documents.get(flow_id) or {}
        nodes = doc.get("nodes", [])
        edges = [edge for edge in doc.get("edges", []) if edge.get("enabled", True)]
        incoming = {str(edge.get("target") or "") for edge in edges}
        outgoing = {str(edge.get("source") or "") for edge in edges}
        catalog.append({
            "id": flow_id,
            "name": item.get("name") or flow_id,
            "status": item.get("workflow_status", "draft"),
            "role": item.get("project_role", ""),
            "group": item.get("project_group", ""),
            "entries": [
                {"id": str(node.get("id") or ""), "label": str((node.get("data") or {}).get("label") or node.get("id"))}
                for node in nodes
                if node.get("type") == "start" or str(node.get("id") or "") not in incoming
            ][:30],
            "exits": [
                {"id": str(node.get("id") or ""), "label": str((node.get("data") or {}).get("label") or node.get("id"))}
                for node in nodes
                if node.get("type") == "end" or str(node.get("id") or "") not in outgoing
            ][:30],
        })
    return catalog