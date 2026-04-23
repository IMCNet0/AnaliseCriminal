"""Carrega a identidade visual da InsightGeoLab AI a partir de /brand/brand.yaml.

Fallback: paleta azul profundo + verde vibrante (brand default).

UI conventions:
  • Logo fica no TOPO da sidebar (via ``sidebar_logo()``), chamado antes dos filtros.
  • ``header()`` no corpo da página é minimalista: só o título da página navegada
    (a área superior fica livre, como pediu o cliente).
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

    # --- Logo ACIMA do menu de navegação: API nativa st.logo() ---
    # Streamlit >= 1.35 insere o logo fixo no topo do sidebar, *acima* do
    # stSidebarNav (menu multipage). Dispensa CSS hacks flex-order.
    logo = brand.get("logo_path")
    if logo and Path(logo).exists():
        try:
            st.logo(
                logo,
                link=None,
                icon_image=logo,
            )
        except Exception:
            # Streamlit antigo: cai no fallback via image() + aviso silencioso.
            st.sidebar.image(logo, use_container_width=True)

    css = f"""
    <style>
        :root {{
            --primary:   {brand["primary_color"]};
            --accent:    {brand["accent_color"]};
            --text:      {brand["text_color"]};
            --on-primary:{brand["on_primary_color"]};
        }}
        .block-container {{padding-top: 1.1rem; max-width: 1400px;}}
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
        /* O logo nativo (st.logo) vai pra dentro do stSidebarHeader, que o
           Streamlit renderiza ACIMA de stSidebarNav. Queremos o logo o MAIOR
           possível — ocupando a largura útil da aba lateral. Desligamos as
           restrições default de altura/largura que o Streamlit aplica. */
        [data-testid="stSidebarHeader"] {{
            padding: .25rem .25rem .35rem .25rem !important;
            max-width: 100% !important;
        }}
        [data-testid="stSidebarHeader"] img {{
            display: block; margin: 0 auto;
            width: 100% !important; max-width: 100% !important;
            height: auto !important; max-height: none !important;
            object-fit: contain;
        }}
        [data-testid="stLogo"],
        [data-testid="stSidebarHeader"] [data-testid="stLogo"] {{
            height: auto !important; max-height: none !important;
            width: 100% !important; max-width: 100% !important;
        }}
        /* Em alguns temas o stSidebarHeader tem um filho com padding enorme;
           zera pra o logo encostar nas bordas internas da sidebar. */
        [data-testid="stSidebarHeader"] > div,
        [data-testid="stSidebarHeader"] > div > div {{
            padding-left: 0 !important; padding-right: 0 !important;
            max-width: 100% !important; width: 100% !important;
        }}
        a, a:visited {{color: var(--accent);}}
        /* Título de página compacto (a área superior fica livre). */
        .page-title h1 {{margin: 0 0 .15rem 0; font-size: 1.9rem; line-height: 1.1;}}
        .page-title .page-subtitle {{
            color: var(--primary); font-size: 1rem; font-weight: 500; margin-bottom: .6rem;
        }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

    # Sub-linha da marca (abaixo do menu nativo). Discreta — só reforça o nome.
    st.sidebar.markdown(
        f"<div style='text-align:center; color:{brand['primary_color']}; "
        f"font-size:.78rem; font-weight:600; margin: .2rem 0 .4rem 0;'>"
        f"{brand['name']}</div>",
        unsafe_allow_html=True,
    )
    return brand


def sidebar_logo() -> None:
    """Compat: chamadas antigas continuam funcionando, mas são no-op.

    A identidade visual do sidebar agora é plantada em ``apply_brand()``
    via st.logo() (acima do menu nativo) + sub-linha abaixo do menu.
    """
    return None


def header(title: str | None = None, subtitle: str | None = None) -> None:
    """Cabeçalho minimalista no corpo da página — só título + subtítulo opcional.

    A identidade visual (logo + nome) vive na sidebar; essa função libera o topo
    da página para respirar o conteúdo principal (mapa, gráficos, KPIs).
    """
    if not title:
        return
    html = "<div class='page-title'>"
    html += f"<h1>{title}</h1>"
    if subtitle:
        html += f"<div class='page-subtitle'>{subtitle}</div>"
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)
