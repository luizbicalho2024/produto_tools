from __future__ import annotations

import os

import streamlit as st

from core.auth import render_account_sidebar, require_login
from core.mfa import disable, enable, mfa_state, new_secret, provisioning_uri, verify
from core.styles import apply_global_styles, page_header

st.set_page_config(page_title="Segurança", page_icon="🔐", layout="wide")
apply_global_styles(full_width=True)
user = require_login()
render_account_sidebar()
username = str(user["username"]).strip().lower()

page_header("Segurança da Conta", "MFA TOTP e preparação para autenticação corporativa via OpenID Connect.")

state = mfa_state(username)
tabs = st.tabs(["MFA", "SSO / OIDC"])

with tabs[0]:
    if state["enabled"]:
        st.success("MFA TOTP está ativo para sua conta.")
        if st.button("Desativar MFA"):
            if disable(username):
                st.success("MFA desativado.")
                st.rerun()
    else:
        st.info("Ative MFA usando um aplicativo autenticador compatível com TOTP.")
        secret = st.session_state.setdefault("mfa_enrollment_secret", new_secret())
        st.code(secret)
        st.caption("Adicione este segredo manualmente no autenticador ou use a URI abaixo.")
        st.code(provisioning_uri(secret, username))
        code = st.text_input("Código de 6 dígitos para confirmar", max_chars=6)
        if st.button("Ativar MFA", type="primary"):
            if verify(secret, code):
                enable(username, secret)
                st.session_state.pop("mfa_enrollment_secret", None)
                st.success("MFA ativado.")
                st.rerun()
            else:
                st.error("Código inválido.")

with tabs[1]:
    mode = str(os.getenv("AUTH_MODE") or "local").strip().lower()
    st.metric("Modo atual", mode.upper())
    st.write(
        "Para SSO corporativo, configure `AUTH_MODE=oidc` e a seção `[auth]` do `secrets.toml` "
        "com `redirect_uri`, `cookie_secret`, `client_id`, `client_secret` e `server_metadata_url`. "
        "O e-mail retornado pelo provedor deve existir no diretório de usuários do Produto Tools."
    )
    st.caption("O modo local continua suportado e pode exigir MFA TOTP por usuário.")