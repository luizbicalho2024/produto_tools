from __future__ import annotations

import streamlit as st

import database as db
from core.auth import LOGIN_FIELDS, build_authenticator, clear_auth_state
from core.configuration import APP_NAME
from core.styles import apply_global_styles, get_ui_theme, render_theme_selector

st.set_page_config(page_title=f"Acesso | {APP_NAME}", page_icon="🧩", layout="centered")
apply_global_styles()

with st.sidebar:
    render_theme_selector(key="login_ui_theme", compact=True)

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

try:
    authenticator, credentials = build_authenticator()
except RuntimeError as exc:
    st.error(str(exc))
    st.stop()

if not credentials.get("usernames"):
    st.warning("Nenhum usuário ativo foi encontrado.")
    st.stop()

if st.session_state.get("authentication_status") is True:
    authenticator.login(
        location="unrendered",
        key="produto_tools_cookie_login",
        max_login_attempts=5,
    )
else:
    with st.container(border=True):
        authenticator.login(
            location="main",
            fields=LOGIN_FIELDS,
            key="produto_tools_login",
            max_login_attempts=5,
        )

if "logged_in_log" not in st.session_state:
    st.session_state.logged_in_log = False

if st.session_state.get("authentication_status"):
    username = str(st.session_state.get("username") or "").strip().lower()
    profile = db.get_user_profile(username)
    if profile is None:
        clear_auth_state()
        st.error("A conta está inativa ou não existe mais.")
        st.stop()

    profile_theme = str(profile.get("ui_theme") or "").lower()
    theme_source = str(st.session_state.get("ui_theme_source") or "default")
    if profile_theme in {"light", "dark"} and theme_source in {"", "default", "profile"}:
        st.session_state["ui_theme"] = profile_theme
        st.session_state["ui_theme_source"] = "profile"
        try:
            st.query_params["theme"] = profile_theme
        except Exception:
            pass
        selected_theme = profile_theme
    else:
        selected_theme = get_ui_theme()
        if profile.get("ui_theme") != selected_theme:
            db.set_user_ui_theme(username, selected_theme)
            profile["ui_theme"] = selected_theme

    st.session_state["username"] = username
    st.session_state["role"] = profile["role"]
    st.session_state["user_info"] = profile
    if not st.session_state.logged_in_log:
        db.add_log(username, "Login realizado no Produto Tools")
        st.session_state.logged_in_log = True

    st.switch_page("pages/2_Central_de_Processos.py")
elif st.session_state.get("authentication_status") is False:
    st.session_state.logged_in_log = False
    st.error("Usuário ou senha inválidos.")
