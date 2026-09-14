from __future__ import annotations

import hashlib
import json
import os
from typing import Any, Iterable

import streamlit as st
import streamlit_authenticator as stauth

import database as db
from core.styles import remember_ui_theme, render_theme_selector

LOGIN_FIELDS = {
    "Form name": "Acesso à plataforma",
    "Username": "Usuário",
    "Password": "Senha",
    "Login": "Entrar",
}

_AUTHENTICATOR_KEY = "_produto_tools_authenticator"
_AUTHENTICATOR_SIGNATURE_KEY = "_produto_tools_authenticator_signature"

def _secret(name: str, default: Any = None) -> Any:
    try:
        value = st.secrets.get(name)
    except Exception:
        value = None
    return value if value not in (None, "") else os.getenv(name, default)

def auth_mode() -> str:
    value = str(_secret("AUTH_MODE", "local") or "local").strip().lower()
    return value if value in {"local", "oidc"} else "local"

def _apply_profile_theme(profile: dict | None) -> None:
    if not isinstance(profile, dict):
        return
    theme = str(profile.get("ui_theme") or "").lower()
    if theme not in {"light", "dark"}:
        return
    source = str(st.session_state.get("ui_theme_source") or "")
    current = str(st.session_state.get("ui_theme") or "").lower()
    if current not in {"light", "dark"} or source in {"", "default", "profile"}:
        st.session_state["ui_theme"] = theme
        st.session_state["ui_theme_source"] = "profile"
        try:
            st.query_params["theme"] = theme
        except Exception:
            pass

def _credentials_signature(credentials: dict, cookie_name: str, cookie_key: str, cookie_expiry_days: int) -> str:
    payload = {
        "credentials": credentials,
        "cookie_name": cookie_name,
        "cookie_key": cookie_key,
        "cookie_expiry_days": cookie_expiry_days,
    }
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()

def build_authenticator() -> tuple[Any, dict]:
    credentials = db.fetch_all_users_for_auth()
    cookie_name = str(_secret("AUTH_COOKIE_NAME", "simulador_telemetria_auth") or "simulador_telemetria_auth").strip()
    cookie_key = str(_secret("AUTH_COOKIE_KEY", "") or "").strip()
    cookie_expiry_days = int(_secret("AUTH_COOKIE_EXPIRY_DAYS", 30) or 30)
    if len(cookie_key) < 32:
        raise RuntimeError("AUTH_COOKIE_KEY deve possuir pelo menos 32 caracteres.")
    signature = _credentials_signature(credentials, cookie_name, cookie_key, cookie_expiry_days)
    existing = st.session_state.get(_AUTHENTICATOR_KEY)
    if existing is not None and st.session_state.get(_AUTHENTICATOR_SIGNATURE_KEY) == signature:
        return existing, credentials
    authenticator = stauth.Authenticate(
        credentials,
        cookie_name,
        cookie_key,
        cookie_expiry_days=cookie_expiry_days,
        pre_authorized=None,
    )
    st.session_state[_AUTHENTICATOR_KEY] = authenticator
    st.session_state[_AUTHENTICATOR_SIGNATURE_KEY] = signature
    return authenticator, credentials

def _profile_by_email(email: str) -> dict | None:
    normalized = str(email or "").strip().lower()
    if not normalized:
        return None
    for item in db.get_all_users():
        if item.get("active") is False:
            continue
        if str(item.get("email") or "").strip().lower() == normalized:
            return db.get_user_profile(str(item.get("username") or ""))
    return None

def restore_authentication() -> None:
    if auth_mode() == "oidc":
        try:
            logged = bool(st.user.is_logged_in)
        except Exception:
            logged = False
        if not logged:
            return
        email = str(getattr(st.user, "email", "") or "")
        profile = _profile_by_email(email)
        if not profile:
            return
        st.session_state["authentication_status"] = True
        st.session_state["username"] = profile["username"]
        st.session_state["name"] = profile["name"]
        st.session_state["role"] = profile["role"]
        st.session_state["user_info"] = profile
        _apply_profile_theme(profile)
        return

    try:
        authenticator, _ = build_authenticator()
    except Exception:
        return
    if not st.session_state.get("authentication_status"):
        try:
            authenticator.login(location="unrendered", key="produto_tools_background_login", max_login_attempts=5)
        except Exception:
            return
    username = str(st.session_state.get("username") or "").strip().lower()
    if st.session_state.get("authentication_status") and username:
        profile = db.get_user_profile(username)
        if profile:
            st.session_state["username"] = username
            st.session_state["role"] = profile["role"]
            st.session_state["user_info"] = profile
            _apply_profile_theme(profile)

def current_user() -> dict | None:
    restore_authentication()
    profile = st.session_state.get("user_info")
    if isinstance(profile, dict):
        return profile
    username = str(st.session_state.get("username") or "").strip().lower()
    profile = db.get_user_profile(username) if username else None
    if profile:
        st.session_state["user_info"] = profile
    return profile

def require_login() -> dict:
    restore_authentication()
    if not st.session_state.get("authentication_status"):
        st.error("Acesso restrito. Entre novamente pela página inicial.")
        if st.button("Ir para o login", use_container_width=False):
            st.switch_page("login_app.py")
        st.stop()
    username = str(st.session_state.get("username") or "").strip().lower()
    profile = db.get_user_profile(username) if username else None
    if not profile:
        clear_auth_state()
        st.error("A conta está inativa ou não existe mais. Entre novamente.")
        st.stop()
    st.session_state["username"] = username
    st.session_state["role"] = profile["role"]
    st.session_state["user_info"] = profile
    _apply_profile_theme(profile)
    return profile

def require_roles(roles: Iterable[str]) -> dict:
    user = require_login()
    allowed = {str(role).lower() for role in roles}
    if str(user.get("role", "")).lower() not in allowed:
        st.error("Seu perfil não possui permissão para acessar esta página.")
        st.stop()
    return user

def require_admin() -> dict:
    return require_roles(["admin"])

def clear_auth_state(*, keep_authenticator: bool = False) -> None:
    for key in (
        "authentication_status", "name", "username", "role", "user_info",
        "logged_in_log", "logout", "failed_login_attempts", "mfa_verified_username",
    ):
        st.session_state.pop(key, None)
    if not keep_authenticator:
        st.session_state.pop(_AUTHENTICATOR_KEY, None)
        st.session_state.pop(_AUTHENTICATOR_SIGNATURE_KEY, None)

def perform_logout() -> None:
    if auth_mode() == "oidc":
        clear_auth_state()
        st.logout()
        return
    authenticator, _ = build_authenticator()
    try:
        authenticator.logout(location="unrendered")
    finally:
        clear_auth_state(keep_authenticator=False)
    st.switch_page("login_app.py")

def render_account_sidebar() -> None:
    user = current_user()
    if not user:
        return
    with st.sidebar:
        st.divider()
        st.write(f"**{user.get('name', 'Usuário')}**")
        st.caption(f"@{user.get('username', '')}")
        st.divider()
        render_theme_selector(key="shared_ui_theme")
        if st.button("Sair", use_container_width=True, key="global_logout"):
            perform_logout()