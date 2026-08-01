from __future__ import annotations

import streamlit as st

import database as db
from core.auth import LOGIN_FIELDS, build_authenticator, clear_auth_state, render_account_sidebar
from core.configuration import APP_NAME, APP_VERSION
from core.styles import apply_global_styles

st.set_page_config(page_title=f"Acesso | {APP_NAME}", page_icon="🧰", layout="centered")
apply_global_styles()

st.markdown(
    """
    <div style="max-width:560px;margin:5vh auto 1.4rem;text-align:center">
      <div style="width:62px;height:62px;border-radius:18px;margin:0 auto 14px;display:grid;place-items:center;color:white;background:linear-gradient(135deg,#4f46e5,#7c3aed);font-size:24px;font-weight:800">PT</div>
      <h1 style="margin:0;color:#0f172a;font-size:2rem">Produto Tools</h1>
      <p style="color:#64748b;margin:.45rem 0 0">Editor visual de processos com usuários compartilhados do Simulador de Telemetria.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

if db.get_mongo_client() is None:
    st.error("Não foi possível conectar ao MongoDB Atlas.")
    st.info("Configure `MONGO_CONNECTION_STRING` nos Secrets do Streamlit Cloud e libere a rede no Atlas.")
    st.stop()

if not db.initialize_database():
    st.error("O banco foi localizado, mas não foi possível preparar os índices necessários.")
    st.stop()

try:
    authenticator, credentials = build_authenticator()
except RuntimeError as exc:
    st.error(str(exc))
    st.info("Use a mesma `AUTH_COOKIE_KEY` configurada no Simulador-Telemetria.")
    st.stop()

if not credentials.get("usernames"):
    st.warning("A coleção compartilhada `simulador_db.users` ainda não possui usuários ativos.")
    st.info("Crie o primeiro administrador no Simulador-Telemetria e recarregue esta aplicação.")
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

    st.session_state["username"] = username
    st.session_state["role"] = profile["role"]
    st.session_state["user_info"] = profile
    if not st.session_state.logged_in_log:
        db.add_log(username, "Login realizado no Produto Tools")
        st.session_state.logged_in_log = True

    render_account_sidebar()
    st.success(f"Bem-vindo, **{profile['name']}**.")
    st.caption("As mesmas credenciais também são válidas no Simulador-Telemetria.")
    st.page_link(
        "pages/5_Editor_de_Fluxos.py",
        label="Abrir Editor de Processos",
        icon="🧩",
        use_container_width=True,
    )
    if profile.get("role") == "admin":
        st.page_link(
            "pages/1_Gestão_de_Acesso.py",
            label="Gerenciar usuários compartilhados",
            icon="👥",
            use_container_width=True,
        )
elif st.session_state.get("authentication_status") is False:
    st.session_state.logged_in_log = False
    st.error("Usuário ou senha inválidos.")
else:
    st.caption("Entre com o mesmo usuário e senha utilizados no Simulador-Telemetria.")

st.caption(f"{APP_NAME} • versão {APP_VERSION}")
