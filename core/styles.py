from __future__ import annotations

from html import escape

import streamlit as st

GLOBAL_CSS = """
<style>
:root {
  --pt-primary: #4f46e5;
  --pt-primary-dark: #3730a3;
  --pt-slate-950: #020617;
  --pt-slate-900: #0f172a;
  --pt-slate-700: #334155;
  --pt-slate-500: #64748b;
  --pt-slate-300: #cbd5e1;
  --pt-slate-200: #e2e8f0;
  --pt-slate-100: #f1f5f9;
}
.block-container {padding-top: 1.4rem; padding-bottom: 2rem; max-width: 1680px;}
[data-testid="stSidebar"] {border-right: 1px solid var(--pt-slate-200);}
[data-testid="stMetric"] {
  background: #fff;
  border: 1px solid var(--pt-slate-200);
  border-radius: 14px;
  padding: .8rem 1rem;
  box-shadow: 0 4px 16px rgba(15,23,42,.04);
}
.stButton > button, .stDownloadButton > button {
  border-radius: 10px;
  min-height: 2.5rem;
  font-weight: 600;
}
.pt-hero {
  border: 1px solid #e2e8f0;
  border-radius: 18px;
  padding: 1.25rem 1.4rem;
  background: linear-gradient(135deg, #ffffff 0%, #eef2ff 100%);
  margin-bottom: 1rem;
}
.pt-hero h1 {font-size: 1.7rem; margin: 0 0 .35rem 0; color: #0f172a;}
.pt-hero p {margin: 0; color: #64748b;}
</style>
"""


def apply_global_styles() -> None:
    st.markdown(GLOBAL_CSS, unsafe_allow_html=True)


def page_header(title: str, subtitle: str) -> None:
    st.markdown(
        f'<section class="pt-hero"><h1>{escape(title)}</h1><p>{escape(subtitle)}</p></section>',
        unsafe_allow_html=True,
    )
