from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Callable

import streamlit as st

ASSET_DIR = Path(__file__).resolve().parent / "frontend"


@lru_cache(maxsize=1)
def _load_assets() -> tuple[str, str, str]:
    html = (ASSET_DIR / "index.html").read_text(encoding="utf-8")
    css = (ASSET_DIR / "styles.css").read_text(encoding="utf-8")
    js = (ASSET_DIR / "main.js").read_text(encoding="utf-8")
    return html, css, js


@lru_cache(maxsize=1)
def _renderer():
    if not hasattr(st.components, "v2"):
        raise RuntimeError(
            "O Editor de Processos requer Streamlit 1.52 ou superior. "
            "Execute: pip install -r requirements.txt"
        )
    html, css, js = _load_assets()
    return st.components.v2.component(
        "produto_tools_flow_editor",
        html=html,
        css=css,
        js=js,
        isolate_styles=True,
    )


def flow_editor(
    document: dict[str, Any],
    *,
    key: str,
    height: int = 820,
    on_save_change: Callable[[], None] | None = None,
):
    """Monta o editor e retorna os eventos transitórios emitidos pelo frontend."""
    renderer = _renderer()
    return renderer(
        data={"document": document, "height": height},
        key=key,
        on_save_change=on_save_change or (lambda: None),
        height=height,
    )
