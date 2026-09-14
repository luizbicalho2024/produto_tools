from __future__ import annotations

import streamlit as st

import database as db
from core.auth import LOGIN_FIELDS, auth_mode, build_authenticator, clear_auth_state
from core.configuration import APP_NAME
from core.mfa import mfa_state, verify
from core.styles import apply_global_styles, get_ui_theme, render_theme_selector

st.set_page_config(page_title=f"Acesso | {APP_NAME}", page_icon="🧩", layout="centered")
apply_global_styles()

with st.sidebar:
    render_theme_selector(key="login_ui_theme")

st.markdown(
    """
    <section class="pt-login-card">
      <div class="pt-login-mark">PT</div>
      <h1>Produto Tools</h1>
    </section>
    """,
    unsafe_allow_html=True,
)

if db.get_mongo_client() is None:
    st.error("Não foi possível conectar ao MongoDB Atlas.")
    st.stop()
if not db.initialize_database():
    st.error("Não foi possível preparar o banco de dados.")
    st.stop()

mode = auth_mode()
profile = None

if mode == "oidc":
    try:
        logged = bool(st.user.is_logged_in)
    except Exception:
        logged = False
    if not logged:
        st.info("A autenticação corporativa está habilitada.")
        if st.button("Entrar com SSO", type="primary", use_container_width=True):
            st.login()
        st.stop()
    email = str(getattr(st.user, "email", "") or "").strip().lower()
    for item in db.get_all_users():
        if item.get("active") is False:
            continue
        if str(item.get("email") or "").strip().lower() == email:
            profile = db.get_user_profile(str(item.get("username") or ""))
            break
    if profile is None:
        st.error("Seu e-mail foi autenticado pelo provedor, mas não está autorizado no diretório do Produto Tools.")
        if st.button("Sair do SSO"):
            st.logout()
        st.stop()
    st.session_state["authentication_status"] = True
    st.session_state["username"] = profile["username"]
    st.session_state["name"] = profile["name"]
else:
    try:
        authenticator, credentials = build_authenticator()
    except RuntimeError as exc:
        st.error(str(exc))
        st.stop()
    if not credentials.get("usernames"):
        st.warning("Nenhum usuário ativo foi encontrado.")
        st.stop()
    if st.session_state.get("authentication_status") is True:
        authenticator.login(location="unrendered", key="produto_tools_cookie_login", max_login_attempts=5)
    else:
        with st.container(border=True):
            authenticator.login(location="main", fields=LOGIN_FIELDS, key="produto_tools_login", max_login_attempts=5)
    if st.session_state.get("authentication_status") is False:
        st.error("Usuário ou senha inválidos.")
        st.stop()
    if not st.session_state.get("authentication_status"):
        st.stop()
    username = str(st.session_state.get("username") or "").strip().lower()
    profile = db.get_user_profile(username)
    if profile is None:
        clear_auth_state()
        st.error("A conta está inativa ou não existe mais.")
        st.stop()
    mfa = mfa_state(username)
    if mfa["enabled"] and st.session_state.get("mfa_verified_username") != username:
        st.info("Confirme o segundo fator de autenticação.")
        code = st.text_input("Código TOTP", max_chars=6, type="password")
        if st.button("Validar MFA", type="primary", use_container_width=True):
            if verify(mfa["secret"], code):
                st.session_state["mfa_verified_username"] = username
                st.rerun()
            else:
                st.error("Código MFA inválido.")
        st.stop()

if "logged_in_log" not in st.session_state:
    st.session_state.logged_in_log = False

profile_theme = str(profile.get("ui_theme") or "").lower()
theme_source = str(st.session_state.get("ui_theme_source") or "default")
if profile_theme in {"light", "dark"} and theme_source in {"", "default", "profile"}:
    st.session_state["ui_theme"] = profile_theme
    st.session_state["ui_theme_source"] = "profile"
    try:
        st.query_params["theme"] = profile_theme
    except Exception:
        pass
else:
    selected_theme = get_ui_theme()
    if profile.get("ui_theme") != selected_theme:
        db.set_user_ui_theme(profile["username"], selected_theme)
        profile["ui_theme"] = selected_theme

st.session_state["username"] = profile["username"]
st.session_state["role"] = profile["role"]
st.session_state["user_info"] = profile
st.session_state["authentication_status"] = True
if not st.session_state.logged_in_log:
    db.add_log(profile["username"], f"Login realizado no Produto Tools ({mode})")
    st.session_state.logged_in_log = True
st.switch_page("pages/2_Central_de_Processos.py")