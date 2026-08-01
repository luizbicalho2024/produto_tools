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
            "bg": "#00141f",
            "panel": "#001e2b",
            "panel_soft": "#082631",
            "text": "#f5fbf7",
            "muted": "#9bc3b3",
            "line": "#173c36",
            "input": "#08212c",
            "hero_a": "#001e2b",
            "hero_b": "#082631",
            "shadow": "rgba(0,0,0,.34)",
        }
    else:
        palette = {
            "bg": "#f4f8fb",
            "panel": "#ffffff",
            "panel_soft": "#f7fafc",
            "text": "#102a43",
            "muted": "#486581",
            "line": "#d9e2ec",
            "input": "#ffffff",
            "hero_a": "#ffffff",
            "hero_b": "#eefbf5",
            "shadow": "rgba(16,42,67,.08)",
        }

    max_width = "100%" if full_width else "1680px"
    horizontal_padding = ".75rem" if full_width else "1rem"

    st.markdown(
        f"""
        <style>
        :root {{
          --pt-primary: #00a35c;
          --pt-primary-dark: #00684a;
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
        div[data-testid="stToolbar"] {{right: .6rem;}}
        [data-testid="stMetric"],
        [data-testid="stForm"],
        [data-testid="stVerticalBlockBorderWrapper"],
        div[data-testid="stExpander"] {{
          background: color-mix(in srgb, var(--pt-panel) 96%, transparent);
          border-color: var(--pt-line) !important;
          color: var(--pt-text);
          border-radius: 18px !important;
          box-shadow: 0 14px 36px var(--pt-shadow);
        }}
        [data-testid="stMetric"] {{
          border: 1px solid var(--pt-line);
          border-radius: 18px;
          padding: .85rem .95rem;
          box-shadow: 0 10px 30px var(--pt-shadow);
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
          border-radius: 999px;
          min-height: 2.65rem;
          font-weight: 700;
          border: 1px solid var(--pt-line);
          box-shadow: 0 8px 22px var(--pt-shadow);
        }}
        .stButton > button[kind="primary"], .stForm button[kind="primary"] {{
          background: linear-gradient(135deg, var(--pt-primary) 0%, var(--pt-primary-dark) 100%);
          color: white;
          border-color: color-mix(in srgb, var(--pt-primary) 60%, var(--pt-line));
        }}
        .pt-hero {{
          border: 1px solid var(--pt-line);
          border-radius: 24px;
          padding: 1rem 1.15rem;
          background: linear-gradient(135deg, {palette['hero_a']} 0%, {palette['hero_b']} 100%);
          margin-bottom: .8rem;
          box-shadow: 0 18px 42px var(--pt-shadow);
        }}
        .pt-hero h1 {{font-size: 1.55rem; margin: 0 0 .28rem 0; color: var(--pt-text);}}
        .pt-hero p {{margin: 0; color: var(--pt-muted);}}
        .pt-login-card {{
          max-width: 620px;
          margin: 4vh auto 1.4rem;
          padding: 1.25rem;
          border-radius: 28px;
          border: 1px solid var(--pt-line);
          background: linear-gradient(135deg, {palette['hero_a']} 0%, {palette['hero_b']} 100%);
          box-shadow: 0 22px 54px var(--pt-shadow);
          text-align: center;
        }}
        .pt-login-mark {{
          width: 74px; height: 74px; border-radius: 24px; margin: 0 auto 16px;
          display:grid; place-items:center; color:white; font-size:28px; font-weight:800;
          background: linear-gradient(135deg, var(--pt-primary) 0%, var(--pt-primary-dark) 100%);
          box-shadow: 0 16px 34px color-mix(in srgb, var(--pt-primary) 26%, transparent);
        }}
        .pt-login-card h1 {{margin:0; font-size:2rem; color:var(--pt-text);}}
        .pt-login-card p {{margin:.45rem 0 0; color:var(--pt-muted);}}
        .pt-soft-card {{
          border: 1px solid var(--pt-line);
          border-radius: 20px;
          padding: .95rem 1rem;
          background: color-mix(in srgb, var(--pt-panel) 96%, transparent);
          box-shadow: 0 12px 30px var(--pt-shadow);
        }}
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
