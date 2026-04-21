"""Carrega a identidade visual da InsightGeoLab AI a partir de /brand/brand.yaml.

Fallback: paleta azul profundo + verde vibrante (brand default).
"""
from __future__ import annotations

from pathlib import Path
import yaml
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
BRAND_DIR = ROOT / "brand"
BRAND_YAML = BRAND_DIR / "brand.yaml"


DEFAULT = {
    "name": "InsightGeoLab AI",
    "tagline": "Inteligência Artificial aplicada à geociência do crime.",
    "subtagline": "Transformando dados em decisões inteligentes.",
    "primary_color": "#0C2B4E",
    "accent_color": "#29A645",
    "background_color": "#FFFFFF",
    "text_color": "#000000",
    "on_primary_color": "#FFFFFF",
    "font": "Source Sans Pro, Helvetica Neue, Arial, sans-serif",
    "logo_path": None,
}


@st.cache_data
def load_brand() -> dict:
    if BRAND_YAML.exists():
        data = yaml.safe_load(BRAND_YAML.read_text(encoding="utf-8"))
        merged = {**DEFAULT, **(data or {})}
    else:
        merged = dict(DEFAULT)
    logo = merged.get("logo_path")
    if logo and not Path(logo).is_absolute():
        merged["logo_path"] = str(BRAND_DIR / logo)
    return merged


def apply_brand(page_title: str | None = None, layout: str = "wide") -> dict:
    brand = load_brand()
    st.set_page_config(
        page_title=page_title or brand["name"],
        page_icon=brand.get("logo_path") or "🛡️",
        layout=layout,
        initial_sidebar_state="expanded",
    )
    css = f"""
    <style>
        :root {{
            --primary:   {brand["primary_color"]};
            --accent:    {brand["accent_color"]};
            --text:      {brand["text_color"]};
            --on-primary:{brand["on_primary_color"]};
        }}
        .block-container {{padding-top: 1.2rem; max-width: 1400px;}}
        h1, h2, h3, h4 {{color: var(--primary); font-family: {brand["font"]};}}
        .stMetric {{background: #F7F9FC; padding: .75rem 1rem;
                    border-left: 4px solid var(--accent); border-radius: 6px;}}
        .stButton > button[kind="primary"] {{background: var(--accent); color: var(--on-primary);
                                              border: none;}}
        .stDownloadButton > button {{background: var(--primary); color: var(--on-primary);
                                     border: none;}}
        [data-testid="stSidebar"] {{background: #F7F9FC; border-right: 1px solid #E5E7EB;}}
        [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3 {{color: var(--primary);}}
        a, a:visited {{color: var(--accent);}}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)
    return brand


def header(subtitle: str | None = None) -> None:
    brand = load_brand()
    cols = st.columns([1, 6])
    with cols[0]:
        if brand.get("logo_path") and Path(brand["logo_path"]).exists():
            st.image(brand["logo_path"], use_container_width=True)
        else:
            st.markdown("### 🛡️")
    with cols[1]:
        st.title(brand["name"])
        st.markdown(
            f"<div style='color:{brand['primary_color']}; font-size:1.05rem; "
            f"font-weight:500; margin-top:-8px;'>{brand['tagline']}</div>",
            unsafe_allow_html=True,
        )
        if subtitle:
            st.caption(subtitle)
        elif brand.get("subtagline"):
            st.caption(brand["subtagline"])
