from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Callable

import streamlit as st

ASSET_DIR = Path(__file__).resolve().parent / "frontend"


@lru_cache(maxsize=1)
def _load_assets() -> tuple[str, str, str]:
    return (
        (ASSET_DIR / "index.html").read_text(encoding="utf-8"),
        (ASSET_DIR / "styles.css").read_text(encoding="utf-8"),
        (ASSET_DIR / "main.js").read_text(encoding="utf-8"),
    )


@lru_cache(maxsize=1)
def _renderer():
    if not hasattr(st.components, "v2"):
        raise RuntimeError(
            "O Editor de Processos requer Streamlit 1.52 ou superior. "
            "Execute: pip install -r requirements.txt"
        )
    html, css, js = _load_assets()
    return st.components.v2.component(
        "produto_tools_flow_editor_v323",
        html=html,
        css=css,
        js=js,
        isolate_styles=True,
    )


def flow_editor(
    document: dict[str, Any],
    *,
    key: str,
    height: int = 900,
    theme: str = "light",
    revision: int = 1,
    permission: str = "viewer",
    flow_catalog: list[dict[str, Any]] | None = None,
    comments: list[dict[str, Any]] | None = None,
    autosave_seconds: int = 10,
    project_id: str = "",
    user_id: str = "",
    initial_node_id: str = "",
    project_playback: dict[str, Any] | None = None,
    on_save_change: Callable[[], None] | None = None,
    on_autosave_change: Callable[[], None] | None = None,
    on_open_flow_change: Callable[[], None] | None = None,
    on_project_return_change: Callable[[], None] | None = None,
    on_comment_create_change: Callable[[], None] | None = None,
):
    """Renderiza o editor profissional e devolve eventos transitórios do frontend."""
    renderer = _renderer()
    data = {
        "document": document,
        "height": height,
        "theme": theme,
        "revision": int(revision),
        "permission": permission,
        "flowCatalog": flow_catalog or [],
        "comments": comments or [],
        "autosaveSeconds": max(5, int(autosave_seconds)),
        "projectId": str(project_id or ""),
        "userId": str(user_id or ""),
        "initialNodeId": str(initial_node_id or ""),
        "projectPlayback": project_playback or {},
    }
    return renderer(
        data=data,
        key=key,
        on_save_change=on_save_change or (lambda: None),
        on_autosave_change=on_autosave_change or (lambda: None),
        on_open_flow_change=on_open_flow_change or (lambda: None),
        on_project_return_change=on_project_return_change or (lambda: None),
        on_comment_create_change=on_comment_create_change or (lambda: None),
        height=height,
    )
