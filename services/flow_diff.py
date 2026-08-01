from __future__ import annotations

from copy import deepcopy
from typing import Any


def _index(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(item.get("id")): item for item in items if item.get("id")}


def _changes(before: dict[str, Any], after: dict[str, Any]) -> list[str]:
    changed: list[str] = []
    keys = sorted(set(before) | set(after))
    for key in keys:
        if key in {"position", "updatedAt"}:
            continue
        if before.get(key) != after.get(key):
            changed.append(key)
    if before.get("position") != after.get("position"):
        changed.append("posição")
    return changed


def compare_documents(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {"nodes": {}, "edges": {}, "lanes": {}, "summary": {}}
    labels = (("nodes", "elementos"), ("edges", "conexões"), ("lanes", "raias"))
    for key, _ in labels:
        left, right = _index(before.get(key, [])), _index(after.get(key, []))
        added = sorted(set(right) - set(left))
        removed = sorted(set(left) - set(right))
        modified = []
        for item_id in sorted(set(left) & set(right)):
            changed = _changes(left[item_id], right[item_id])
            if changed:
                modified.append({"id": item_id, "fields": changed, "before": deepcopy(left[item_id]), "after": deepcopy(right[item_id])})
        result[key] = {"added": added, "removed": removed, "modified": modified}
    result["summary"] = {
        "nodes_added": len(result["nodes"]["added"]),
        "nodes_removed": len(result["nodes"]["removed"]),
        "nodes_modified": len(result["nodes"]["modified"]),
        "edges_added": len(result["edges"]["added"]),
        "edges_removed": len(result["edges"]["removed"]),
        "edges_modified": len(result["edges"]["modified"]),
        "lanes_added": len(result["lanes"]["added"]),
        "lanes_removed": len(result["lanes"]["removed"]),
        "lanes_modified": len(result["lanes"]["modified"]),
    }
    result["has_changes"] = any(result["summary"].values()) or before.get("flow") != after.get("flow")
    return result
