from __future__ import annotations

from html import escape

import streamlit as st

THEME_LABELS = {
    "light": "Claro",
    "dark": "Escuro",
}


def _valid_theme(value: object) -> str | None:
    theme = str(value or "").strip().lower()
    return theme if theme in THEME_LABELS else None


def get_ui_theme() -> str:
    session_theme = _valid_theme(st.session_state.get("ui_theme"))
    if session_theme:
        return session_theme

    query_theme: str | None = None
    try:
        query_theme = _valid_theme(st.query_params.get("theme"))
    except Exception:
        query_theme = None

    profile = st.session_state.get("user_info")
    profile_theme = _valid_theme(profile.get("ui_theme")) if isinstance(profile, dict) else None
    theme = query_theme or profile_theme or "light"
    st.session_state["ui_theme"] = theme
    st.session_state["ui_theme_source"] = "query" if query_theme else ("profile" if profile_theme else "default")
    return theme


def remember_ui_theme(theme: str) -> str:
    selected = _valid_theme(theme) or "light"
    st.session_state["ui_theme"] = selected
    st.session_state["ui_theme_source"] = "user"
    try:
        st.query_params["theme"] = selected
    except Exception:
        pass

    username = str(st.session_state.get("username") or "").strip().lower()
    if username:
        try:
            import database as db

            db.set_user_ui_theme(username, selected)
        except Exception:
            pass
        profile = st.session_state.get("user_info")
        if isinstance(profile, dict):
            profile["ui_theme"] = selected
    return selected


def render_theme_selector(*, key: str = "global_ui_theme", compact: bool = False) -> str:
    current = get_ui_theme()
    if key not in st.session_state:
        st.session_state[key] = current
    selected = st.selectbox(
        "Tema",
        options=list(THEME_LABELS),
        format_func=lambda value: "Claro" if value == "light" else "Escuro",
        key=key,
        label_visibility="collapsed" if compact else "visible",
        help="A preferência é lembrada para este usuário.",
    )
    if selected != current:
        remember_ui_theme(selected)
        st.rerun()
    return selected


def apply_global_styles(*, full_width: bool = False) -> None:
    theme = get_ui_theme()
    if theme == "dark":
        palette = {
            "bg": "#00141f",
            "panel": "#001e2b",
            "panel_soft": "#082631",
            "panel_hover": "#0c3440",
            "text": "#f5fbf7",
            "muted": "#9bc3b3",
            "line": "#1f4b44",
            "input": "#082631",
            "hero_a": "#001e2b",
            "hero_b": "#082631",
            "shadow": "rgba(0,0,0,.36)",
            "menu": "#082631",
        }
    else:
        palette = {
            "bg": "#f4f8fb",
            "panel": "#ffffff",
            "panel_soft": "#f7fafc",
            "panel_hover": "#edf6f2",
            "text": "#102a43",
            "muted": "#486581",
            "line": "#d9e2ec",
            "input": "#ffffff",
            "hero_a": "#ffffff",
            "hero_b": "#eefbf5",
            "shadow": "rgba(16,42,67,.08)",
            "menu": "#ffffff",
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
          --pt-panel-hover: {palette['panel_hover']};
          --pt-text: {palette['text']};
          --pt-muted: {palette['muted']};
          --pt-line: {palette['line']};
          --pt-input: {palette['input']};
          --pt-menu: {palette['menu']};
          --pt-shadow: {palette['shadow']};
          color-scheme: {theme};
        }}
        html, body, [data-testid="stAppViewContainer"], .stApp {{
          background: var(--pt-bg) !important;
          color: var(--pt-text) !important;
        }}
        .block-container {{
          padding-top: .9rem;
          padding-right: {horizontal_padding};
          padding-bottom: 1.5rem;
          padding-left: {horizontal_padding};
          max-width: {max_width};
        }}
        [data-testid="stSidebar"], [data-testid="stSidebarContent"] {{
          background: var(--pt-panel) !important;
          border-right: 1px solid var(--pt-line);
        }}
        [data-testid="stSidebar"] * {{color: var(--pt-text);}}
        [data-testid="stHeader"] {{background: color-mix(in srgb, var(--pt-bg) 88%, transparent) !important;}}
        div[data-testid="stToolbar"] {{right: .6rem;}}

        h1, h2, h3, h4, h5, h6, p, label,
        [data-testid="stMarkdownContainer"], [data-testid="stWidgetLabel"],
        [data-testid="stText"], [data-testid="stCaptionContainer"] {{color: var(--pt-text) !important;}}
        [data-testid="stMetricLabel"], [data-testid="stMetricDelta"],
        .stCaption, small {{color: var(--pt-muted) !important;}}

        [data-testid="stMetric"], [data-testid="stForm"],
        [data-testid="stVerticalBlockBorderWrapper"], div[data-testid="stExpander"],
        [data-testid="stDialog"] > div, [data-testid="stPopoverBody"] {{
          background: color-mix(in srgb, var(--pt-panel) 97%, transparent) !important;
          border-color: var(--pt-line) !important;
          color: var(--pt-text) !important;
          border-radius: 18px !important;
          box-shadow: 0 14px 36px var(--pt-shadow);
        }}
        [data-testid="stMetric"] {{
          border: 1px solid var(--pt-line);
          padding: .85rem .95rem;
          box-shadow: 0 10px 30px var(--pt-shadow);
        }}

        /* Inputs BaseWeb: texto, senha, número, data, select e multiselect. */
        div[data-baseweb="base-input"], div[data-baseweb="base-input"] > div,
        div[data-baseweb="input"], div[data-baseweb="input"] > div,
        div[data-baseweb="select"], div[data-baseweb="select"] > div,
        div[data-baseweb="textarea"], div[data-baseweb="textarea"] > div,
        div[data-baseweb="phone-input"], div[data-baseweb="tag"] {{
          background: var(--pt-input) !important;
          border-color: var(--pt-line) !important;
          color: var(--pt-text) !important;
        }}
        input, textarea, select, button[role="combobox"],
        div[data-baseweb="select"] span, div[data-baseweb="input"] input,
        div[data-baseweb="textarea"] textarea {{
          background-color: transparent !important;
          color: var(--pt-text) !important;
          caret-color: var(--pt-text) !important;
          -webkit-text-fill-color: var(--pt-text) !important;
        }}
        input:-webkit-autofill, input:-webkit-autofill:hover,
        input:-webkit-autofill:focus {{
          -webkit-box-shadow: 0 0 0 1000px var(--pt-input) inset !important;
          -webkit-text-fill-color: var(--pt-text) !important;
          caret-color: var(--pt-text) !important;
        }}
        input::placeholder, textarea::placeholder {{color: var(--pt-muted) !important; opacity: .9;}}
        div[data-baseweb="select"] svg, div[data-baseweb="input"] svg {{fill: var(--pt-muted) !important; color: var(--pt-muted) !important;}}

        /* Menus, calendários e popovers são portais fora do container principal. */
        div[data-baseweb="popover"], div[data-baseweb="popover"] > div,
        div[data-baseweb="menu"], ul[role="listbox"], div[role="listbox"],
        li[role="option"], div[role="option"],
        div[data-baseweb="calendar"], div[data-baseweb="datepicker"],
        [role="dialog"] {{
          background: var(--pt-menu) !important;
          color: var(--pt-text) !important;
          border-color: var(--pt-line) !important;
        }}
        li[role="option"] *, div[role="option"] *, [role="dialog"] * {{color: var(--pt-text) !important;}}
        li[role="option"]:hover, div[role="option"]:hover,
        li[role="option"][aria-selected="true"], div[role="option"][aria-selected="true"] {{
          background: var(--pt-panel-hover) !important;
          color: var(--pt-text) !important;
        }}

        /* Radio, checkbox, toggle e slider. */
        [data-testid="stCheckbox"], [data-testid="stRadio"], [data-testid="stToggle"],
        [data-testid="stSlider"] {{color: var(--pt-text) !important;}}
        [data-testid="stCheckbox"] label, [data-testid="stRadio"] label,
        [data-testid="stToggle"] label {{color: var(--pt-text) !important;}}
        [data-baseweb="checkbox"] > div, [data-baseweb="radio"] > div {{border-color: var(--pt-line) !important;}}

        /* Uploads e áreas que o Streamlit costuma deixar brancas no modo escuro. */
        [data-testid="stFileUploader"], [data-testid="stFileUploaderDropzone"],
        [data-testid="stFileUploaderDropzone"] > div,
        [data-testid="stFileUploaderDropzoneInstructions"] {{
          background: var(--pt-panel-soft) !important;
          color: var(--pt-text) !important;
          border-color: var(--pt-line) !important;
        }}
        [data-testid="stFileUploader"] button {{background: var(--pt-panel) !important; color: var(--pt-text) !important;}}

        [data-baseweb="tab-list"] {{background: transparent !important; gap: .35rem;}}
        [data-baseweb="tab"] {{
          color: var(--pt-muted) !important;
          background: var(--pt-panel) !important;
          border: 1px solid var(--pt-line) !important;
          border-radius: 999px !important;
          padding-inline: 1rem !important;
        }}
        [data-baseweb="tab"][aria-selected="true"] {{
          color: #ffffff !important;
          background: linear-gradient(135deg, var(--pt-primary), var(--pt-primary-dark)) !important;
          border-color: var(--pt-primary) !important;
        }}
        [data-testid="stAlert"] {{
          color: var(--pt-text) !important;
          background: color-mix(in srgb, var(--pt-panel) 94%, transparent) !important;
          border-color: var(--pt-line) !important;
        }}
        [data-testid="stAlert"] * {{color: var(--pt-text) !important;}}
        [data-testid="stDataFrame"], [data-testid="stTable"] {{
          color: var(--pt-text) !important;
          background: var(--pt-panel) !important;
          border-radius: 16px;
          overflow: hidden;
        }}

        .stButton > button, .stDownloadButton > button,
        [data-testid="stPageLink"] a {{
          border-radius: 999px !important;
          min-height: 2.65rem;
          font-weight: 700;
          border: 1px solid var(--pt-line) !important;
          box-shadow: 0 8px 22px var(--pt-shadow);
        }}
        .stButton > button:not([kind="primary"]), .stDownloadButton > button,
        [data-testid="stPageLink"] a {{
          color: var(--pt-text) !important;
          background: var(--pt-panel) !important;
          border-color: var(--pt-line) !important;
        }}
        .stButton > button:not([kind="primary"]):hover, .stDownloadButton > button:hover,
        [data-testid="stPageLink"] a:hover {{
          color: var(--pt-primary-dark) !important;
          background: var(--pt-panel-hover) !important;
          border-color: var(--pt-primary) !important;
        }}
        .stButton > button[kind="primary"], .stForm button[kind="primary"] {{
          background: linear-gradient(135deg, var(--pt-primary) 0%, var(--pt-primary-dark) 100%) !important;
          color: #ffffff !important;
          border-color: var(--pt-primary) !important;
        }}
        button:disabled {{opacity: .5 !important;}}

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
          max-width: 520px;
          margin: 8vh auto 1.1rem;
          padding: 1.25rem;
          border-radius: 28px;
          border: 1px solid var(--pt-line);
          background: linear-gradient(135deg, {palette['hero_a']} 0%, {palette['hero_b']} 100%);
          box-shadow: 0 22px 54px var(--pt-shadow);
          text-align: center;
        }}
        .pt-login-mark {{
          width: 70px; height: 70px; border-radius: 22px; margin: 0 auto 14px;
          display:grid; place-items:center; color:white; font-size:27px; font-weight:800;
          background: linear-gradient(135deg, var(--pt-primary) 0%, var(--pt-primary-dark) 100%);
          box-shadow: 0 16px 34px color-mix(in srgb, var(--pt-primary) 26%, transparent);
        }}
        .pt-login-card h1 {{margin:0; font-size:1.9rem; color:var(--pt-text);}}
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
