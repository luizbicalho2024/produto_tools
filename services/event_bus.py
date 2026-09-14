from __future__ import annotations

from typing import Any

import database as db
from services.enterprise_repository import list_records
from services.enterprise_tools import send_webhook

def emit_event(event: str, payload: dict[str, Any]) -> dict[str, int]:
    delivered = 0
    failed = 0
    try:
        hooks = list_records("webhook", "sistema", limit=100)
    except Exception:
        return {"delivered": 0, "failed": 0}
    for hook in hooks:
        if hook.get("active") is False:
            continue
        events = hook.get("events") or []
        if event not in events and "*" not in events:
            continue
        try:
            send_webhook(str(hook.get("url") or ""), event, payload, str(hook.get("secret") or ""))
            delivered += 1
        except Exception as exc:
            failed += 1
            db.add_log("sistema", "Webhook falhou", {"event": event, "webhook_id": hook.get("id"), "error": str(exc)[:500]})
    return {"delivered": delivered, "failed": failed}