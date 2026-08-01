from __future__ import annotations

from html import escape

import streamlit as st

THEME_LABELS = {
    "light": "Claro",
    "dark": "Escuro",
}


def get_ui_theme() -> str:
    theme = str(st.session_state.get("ui_theme") or "light").lower()
    if theme not in THEME_LABELS:
        theme = "light"
    st.session_state["ui_theme"] = theme
    return theme


def render_theme_selector(*, key: str = "global_ui_theme") -> str:
    current = get_ui_theme()
    selected = st.selectbox(
        "Aparência",
        options=list(THEME_LABELS),
        index=list(THEME_LABELS).index(current),
        format_func=lambda value: "☀️ Claro" if value == "light" else "🌙 Escuro",
        key=key,
        help="Altera a aparência das páginas e define o tema inicial do editor.",
    )
    st.session_state["ui_theme"] = selected
    return selected


def apply_global_styles(*, full_width: bool = False) -> None:
    theme = get_ui_theme()
    if theme == "dark":
        palette = {
            "bg": "#07101f",
            "panel": "#0f172a",
            "panel_soft": "#111c2f",
            "text": "#e5e7eb",
            "muted": "#94a3b8",
            "line": "#283548",
            "input": "#111827",
            "hero_a": "#111827",
            "hero_b": "#1e1b4b",
            "shadow": "rgba(0,0,0,.32)",
        }
    else:
        palette = {
            "bg": "#f8fafc",
            "panel": "#ffffff",
            "panel_soft": "#f1f5f9",
            "text": "#0f172a",
            "muted": "#64748b",
            "line": "#e2e8f0",
            "input": "#ffffff",
            "hero_a": "#ffffff",
            "hero_b": "#eef2ff",
            "shadow": "rgba(15,23,42,.06)",
        }

    max_width = "100%" if full_width else "1680px"
    horizontal_padding = ".75rem" if full_width else "1rem"

    st.markdown(
        f"""
        <style>
        :root {{
          --pt-primary: #4f46e5;
          --pt-primary-dark: #3730a3;
          --pt-bg: {palette['bg']};
          --pt-panel: {palette['panel']};
          --pt-panel-soft: {palette['panel_soft']};
          --pt-text: {palette['text']};
          --pt-muted: {palette['muted']};
          --pt-line: {palette['line']};
          --pt-input: {palette['input']};
          --pt-shadow: {palette['shadow']};
        }}
        .stApp {{background: var(--pt-bg); color: var(--pt-text);}}
        .block-container {{
          padding-top: .9rem;
          padding-right: {horizontal_padding};
          padding-bottom: 1.5rem;
          padding-left: {horizontal_padding};
          max-width: {max_width};
        }}
        [data-testid="stSidebar"] {{
          background: var(--pt-panel);
          border-right: 1px solid var(--pt-line);
        }}
        [data-testid="stSidebar"] * {{color: var(--pt-text);}}
        [data-testid="stHeader"] {{background: color-mix(in srgb, var(--pt-bg) 88%, transparent);}}
        [data-testid="stMetric"],
        [data-testid="stForm"],
        [data-testid="stVerticalBlockBorderWrapper"] {{
          background: var(--pt-panel);
          border-color: var(--pt-line) !important;
          color: var(--pt-text);
        }}
        [data-testid="stMetric"] {{
          border: 1px solid var(--pt-line);
          border-radius: 14px;
          padding: .72rem .9rem;
          box-shadow: 0 4px 16px var(--pt-shadow);
        }}
        [data-testid="stMetricLabel"],
        [data-testid="stMetricDelta"],
        .stCaption, small {{color: var(--pt-muted) !important;}}
        h1, h2, h3, h4, h5, h6, p, label, [data-testid="stMarkdownContainer"] {{color: var(--pt-text);}}
        div[data-baseweb="input"] > div,
        div[data-baseweb="select"] > div,
        div[data-baseweb="textarea"] > div {{
          background: var(--pt-input);
          border-color: var(--pt-line);
          color: var(--pt-text);
        }}
        input, textarea {{color: var(--pt-text) !important;}}
        .stButton > button, .stDownloadButton > button {{
          border-radius: 10px;
          min-height: 2.5rem;
          font-weight: 600;
        }}
        .pt-hero {{
          border: 1px solid var(--pt-line);
          border-radius: 16px;
          padding: .9rem 1.05rem;
          background: linear-gradient(135deg, {palette['hero_a']} 0%, {palette['hero_b']} 100%);
          margin-bottom: .7rem;
          box-shadow: 0 5px 18px var(--pt-shadow);
        }}
        .pt-hero h1 {{font-size: 1.5rem; margin: 0 0 .25rem 0; color: var(--pt-text);}}
        .pt-hero p {{margin: 0; color: var(--pt-muted);}}
        hr {{border-color: var(--pt-line) !important;}}
        </style>
        """,
        unsafe_allow_html=True,
    )


def page_header(title: str, subtitle: str) -> None:
    st.markdown(
        f'<section class="pt-hero"><h1>{escape(title)}</h1><p>{escape(subtitle)}</p></section>',
        unsafe_allow_html=True,
    )
