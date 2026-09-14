from __future__ import annotations

import pandas as pd
import streamlit as st

from core.auth import render_account_sidebar, require_login
from core.styles import apply_global_styles, page_header
from services.enterprise_repository import initialize_enterprise_tables, list_records, save_record
from services.flowchart_repository import list_custom_templates
from services.template_library import built_in_templates

st.set_page_config(page_title="Biblioteca Corporativa", page_icon="🧰", layout="wide")
apply_global_styles(full_width=True)
user = require_login()
render_account_sidebar()
username = str(user["username"]).strip().lower()
is_admin = user.get("role") == "admin"
initialize_enterprise_tables()

page_header("Biblioteca Corporativa de Templates", "Catálogo, favoritos, avaliação, versão e governança de templates reutilizáveis.")

templates = [*built_in_templates(username), *list_custom_templates(username, include_all=is_admin)]
if not templates:
    st.info("Nenhum template disponível.")
    st.stop()

meta = list_records("template_meta", username)
my_meta = {str(item.get("template_id")): item for item in meta if str(item.get("username")) == username}

search = st.text_input("Pesquisar template")
category = st.multiselect("Categorias", sorted({str(item.get("category") or "Geral") for item in templates}))
rows = []
for item in templates:
    template_id = str(item.get("id") or item.get("_id") or item.get("name"))
    name = str(item.get("name") or "Template")
    cat = str(item.get("category") or "Geral")
    if search and search.lower() not in f"{name} {cat} {item.get('description','')}".lower():
        continue
    if category and cat not in category:
        continue
    m = my_meta.get(template_id, {})
    rows.append({
        "template_id": template_id,
        "Nome": name,
        "Categoria": cat,
        "Descrição": item.get("description") or "",
        "Favorito": bool(m.get("favorite")),
        "Minha nota": int(m.get("rating") or 0),
        "Versão": str(m.get("template_version") or "1.0"),
        "Corporativo": bool(item.get("organization")),
    })

st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True, column_config={"template_id": None})
template_id = st.selectbox("Template", [row["template_id"] for row in rows], format_func=lambda value: next(row["Nome"] for row in rows if row["template_id"] == value))
current = my_meta.get(template_id, {})
favorite = st.checkbox("Favorito", value=bool(current.get("favorite")))
rating = st.slider("Avaliação", 0, 5, int(current.get("rating") or 0))
version = st.text_input("Versão do template", value=str(current.get("template_version") or "1.0"))
notes = st.text_area("Notas de governança", value=str(current.get("notes") or ""), height=90)
if st.button("Salvar preferências e governança", type="primary"):
    save_record(
        "template_meta",
        {
            "template_id": template_id,
            "username": username,
            "favorite": favorite,
            "rating": rating,
            "template_version": version,
            "notes": notes,
        },
        username,
        record_id=f"template_meta::{username}::{template_id}",
    )
    st.success("Biblioteca atualizada.")