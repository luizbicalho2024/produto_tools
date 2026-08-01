from __future__ import annotations

from typing import Any

from core.configuration import FLOW_ACCESS_LEVELS


def collaborator_level(record: dict[str, Any], username: str) -> str | None:
    normalized = username.strip().lower()
    for item in record.get("collaborators") or []:
        if str(item.get("username") or "").strip().lower() == normalized:
            level = str(item.get("level") or "viewer")
            return level if level in FLOW_ACCESS_LEVELS else "viewer"
    return None


def permission_for(record: dict[str, Any], username: str, *, is_admin: bool = False) -> str | None:
    normalized = username.strip().lower()
    if is_admin or str(record.get("owner_username") or "").strip().lower() == normalized:
        return "owner"
    level = collaborator_level(record, normalized)
    if level:
        return level
    if record.get("visibility") == "organization":
        return "viewer"
    return None


def can_edit(permission: str | None) -> bool:
    return permission in {"owner", "editor", "reviewer", "approver"}


def can_review(permission: str | None) -> bool:
    return permission in {"owner", "reviewer", "approver"}


def can_approve(permission: str | None) -> bool:
    return permission in {"owner", "approver"}
