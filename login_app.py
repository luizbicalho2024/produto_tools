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
    <section class="pt-login-card">
      <div class="pt-login-mark">PT</div>
      <h1>Produto Tools</h1>
      <p>Editor visual de processos e projetos com autenticação compartilhada, fluxos vinculados, releases e persistência no MongoDB Atlas.</p>
    </section>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <section class="pt-soft-card" style="max-width:620px;margin:0 auto 1rem;">
      <strong style="display:block;margin-bottom:.35rem;">O que você encontra aqui</strong>
      <span style="display:block;color:var(--pt-muted);">Projetos com vários fluxos, mapa de dependências, navegação por subprocessos, busca global, execução guiada, governança, autosave, colaboração e armazenamento versionado no MongoDB.</span>
    </section>
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
    col_portfolio, col_projects, col_editor = st.columns(3)
    with col_portfolio:
        st.page_link(
            "pages/2_Central_de_Processos.py",
            label="Central de Processos",
            icon="◫",
            use_container_width=True,
        )
    with col_projects:
        st.page_link(
            "pages/3_Gestão_de_Projetos.py",
            label="Gestão de Projetos",
            icon="▦",
            use_container_width=True,
        )
    with col_editor:
        st.page_link(
            "pages/5_Editor_de_Fluxos.py",
            label="Abrir Editor",
            icon="◈",
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
