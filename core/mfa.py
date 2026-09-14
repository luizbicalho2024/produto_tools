from __future__ import annotations

import pyotp

import database as db
from database import utc_now

def new_secret() -> str:
    return pyotp.random_base32()

def provisioning_uri(secret: str, username: str, issuer: str = "Produto Tools") -> str:
    return pyotp.TOTP(secret).provisioning_uri(name=username, issuer_name=issuer)

def verify(secret: str, code: str) -> bool:
    if not secret or not code:
        return False
    try:
        return bool(pyotp.TOTP(secret).verify(str(code).strip(), valid_window=1))
    except Exception:
        return False

def mfa_state(username: str) -> dict:
    user = db.get_user(username) or {}
    return {
        "enabled": bool(user.get("mfa_enabled")),
        "secret": str(user.get("mfa_secret") or ""),
    }

def enable(username: str, secret: str) -> bool:
    collection = db.get_users_collection()
    if collection is None:
        return False
    result = collection.update_one(
        {"username": username.strip().lower()},
        {"$set": {"mfa_enabled": True, "mfa_secret": secret, "updated_at": utc_now()}},
    )
    return result.matched_count > 0

def disable(username: str) -> bool:
    collection = db.get_users_collection()
    if collection is None:
        return False
    result = collection.update_one(
        {"username": username.strip().lower()},
        {"$set": {"mfa_enabled": False, "updated_at": utc_now()}, "$unset": {"mfa_secret": ""}},
    )
    return result.matched_count > 0