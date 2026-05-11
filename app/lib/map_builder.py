"""Construtor único de mapa folium para o portal.

O mapa da Home tem uma estrutura estável (que NÃO muda entre interações):

  • Base: CartoDB Positron (claro, não rouba atenção dos dados).
  • Camadas permanentes PMESP (controláveis via LayerControl), **opacas**
    (fundo transparente, linha sólida) e empilhadas nessa ordem de
    renderização (de baixo pra cima):
        Companhia PMESP  → linha preta     weight=1   (FUNDO)
        Batalhão PMESP   → linha vermelha  weight=2   (MEIO)
        Comando (CPA)    → linha cinza     weight=3   (TOPO)
    Ordem controlada adicionando os FeatureGroups nessa mesma sequência ao
    mapa — leaflet desenha na ordem de inserção, o último vira o mais "alto".
  • Uma e apenas uma camada de DADOS por vez: `choropleth`, `pontos` ou `hotspot`.
  • Scroll-zoom habilitado.
  • Busca por endereço via Nominatim (OSM) com `geocode_many` pra preview.
  • Centralização programática via `(center, zoom)` — permite persistir zoom
    entre reruns e re-centrar ao selecionar polígono/endereço.

Sem drill-down (cliente suprimiu).
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Iterable

import numpy as np
import pandas as pd
import streamlit as st

import folium
from folium.features import DivIcon
from folium.plugins import HeatMap, Draw
from branca.element import MacroElement, Template


# =========================================================================
# Paleta das camadas permanentes PMESP (espelho do briefing)
# Ordem lógica "do mais genérico pro mais específico" é Comando→BTL→CIA,
# mas a ORDEM DE EMPILHAMENTO no mapa é o inverso: CIA no fundo, Comando no topo.
# =========================================================================
LAYER_STYLE = {
    "Comando (CPA)":     {"color": "#6b7280", "weight": 3, "opacity": 1.0},  # cinza
    "Batalhão PMESP":    {"color": "#c8102e", "weight": 2, "opacity": 1.0},  # vermelho
    "Companhia PMESP":   {"color": "#111111", "weight": 1, "opacity": 1.0},  # preto
}

# Ordem de inserção (última entra por CIMA — Comando fica no topo)
STACKING_ORDER = ["Companhia PMESP", "Batalhão PMESP", "Comando (CPA)"]


def _halo_label_css(color: str, size_px: int = 11) -> str:
    return (
        f"font-size:{size_px}px; font-weight:700; color:{color}; "
        "text-shadow: "
        " 1px 0 0 #fff, -1px 0 0 #fff, 0 1px 0 #fff, 0 -1px 0 #fff, "
        " 1px 1px 0 #fff,-1px 1px 0 #fff, 1px -1px 0 #fff,-1px -1px 0 #fff;"
        "white-space:nowrap; pointer-events:none;"
    )


# =========================================================================
# Geocoder — busca de endereço via Nominatim
# =========================================================================
@st.cache_data(show_spinner=False, ttl=86400)
def geocode_many(endereco: str, limit: int = 5) -> list[dict]:
    """Nominatim (OSM). Retorna até `limit` resultados (lat, lon, display_name).

    Viés agressivo para a CIDADE DE SÃO PAULO via viewbox+bounded
    (rodada abr/26 #4): a busca passa a retornar só endereços dentro do
    bbox da Capital (-46.83/-46.36 lon × -23.36/-24.01 lat). Resultados
    fora da Capital são descartados pelo Nominatim com ``bounded=1``.
    """
    import requests

    q = (endereco or "").strip()
    if not q or len(q) < 3:
        return []
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": q, "format": "json", "limit": int(limit),
                "countrycodes": "br",
                # bbox SP-Capital (W,S,E,N) — pequeno o bastante pra cortar
                # ABC, Guarulhos, Osasco e demais municípios da RM.
                "viewbox": "-46.83,-24.01,-46.36,-23.36",
                "bounded": 1,
                "addressdetails": 0,
            },
            headers={"User-Agent": "InsightGeoLab-AI/1.0 (portal análise criminal SP-Capital)"},
            timeout=8,
        )
        r.raise_for_status()
        hits = r.json() or []
        out = []
        for h in hits:
            try:
                out.append({
                    "lat": float(h["lat"]),
                    "lon": float(h["lon"]),
                    "display_name": h.get("display_name", q),
                })
            except (KeyError, TypeError, ValueError):
                continue
        return out
    except Exception:
        return []


def geocode(endereco: str) -> Optional[tuple[float, float, str]]:
    """Compat: pega o primeiro hit (usado em paths que não fazem preview)."""
    hits = geocode_many(endereco, limit=1)
    if not hits:
        return None
    h = hits[0]
    return h["lat"], h["lon"], h["display_name"]


# =========================================================================
# Camadas permanentes PMESP (Comando / BTL / CIA)
# =========================================================================
@st.cache_resource(show_spinner="Carregando camadas PMESP…")
def load_pmesp_layers() -> dict:
    """Carrega os três geojson PMESP uma única vez e calcula centróides.

    Retorna dict: { 'Comando (CPA)': gdf, 'Batalhão PMESP': gdf, 'Companhia PMESP': gdf }.
    Cada gdf tem colunas ``__label__``, ``__lat__``, ``__lon__``.
    """
    from . import geo as geolib  # evita import circular com o módulo global

    result = {}
    for recorte, label_col in [
        ("Comando (CPA)",    "cmdo_label"),
        ("Batalhão PMESP",   "OPM"),
        ("Companhia PMESP",  "OPM"),  # acrescenta btl no rótulo abaixo
    ]:
        gdf, _key = geolib.load_layer(recorte)
        if gdf is None:
            continue
        gdf = gdf.copy()
        if recorte == "Companhia PMESP" and "btl" in gdf.columns:
            gdf["__label__"] = (
                gdf[label_col].astype("string").fillna("?") + "/" +
                gdf["btl"].astype("string").fillna("?")
            )
        else:
            gdf["__label__"] = gdf[label_col].astype("string").fillna("?")
        cent = gdf.geometry.representative_point()
        gdf["__lat__"] = cent.y
        gdf["__lon__"] = cent.x
        result[recorte] = gdf
    return result


def pmesp_options(recorte: str) -> list[str]:
    """Rótulos PMESP disponíveis para os dropdowns de centralização."""
    layers = load_pmesp_layers()
    gdf = layers.get(recorte)
    if gdf is None:
        return []
    return sorted(gdf["__label__"].dropna().astype(str).unique().tolist())


def pmesp_centroid(recorte: str, label: str) -> Optional[tuple[float, float]]:
    """Centróide (lat, lon) do polígono com ``__label__`` == label."""
    layers = load_pmesp_layers()
    gdf = layers.get(recorte)
    if gdf is None:
        return None
    row = gdf.loc[gdf["__label__"].astype(str) == str(label)]
    if row.empty:
        return None
    r = row.iloc[0]
    if pd.isna(r["__lat__"]) or pd.isna(r["__lon__"]):
        return None
    return float(r["__lat__"]), float(r["__lon__"])


def pmesp_bounds(recorte: str, label: str) -> Optional[tuple[float, float, float, float]]:
    """Bounding box (south, west, north, east) do polígono selecionado.

    Usado para ``fit_bounds`` no leaflet: zoom automático no retângulo
    envolvente (em vez de centróide + nível fixo). Retorna None se o
    polígono não existir ou tiver geometria vazia.
    """
    layers = load_pmesp_layers()
    gdf = layers.get(recorte)
    if gdf is None:
        return None
    row = gdf.loc[gdf["__label__"].astype(str) == str(label)]
    if row.empty:
        return None
    try:
        # total_bounds retorna (minx, miny, maxx, maxy) == (west, south, east, north)
        minx, miny, maxx, maxy = row.geometry.total_bounds
        if any(pd.isna(v) for v in (minx, miny, maxx, maxy)):
            return None
        return float(miny), float(minx), float(maxy), float(maxx)
    except Exception:
        return None


def zoom_for_bounds(bounds: tuple[float, float, float, float]) -> int:
    """Estimativa de nível de zoom que enquadra o bbox fornecido.

    Usado pra sincronizar ``st.session_state.map_zoom`` logo após um
    ``fit_bounds`` programático — sem isso, o st_folium só captura o novo
    zoom após uma interação do usuário, e mudanças subsequentes de filtro
    reconstroem o mapa com o zoom antigo (bug de "reset de zoom").

    Baseado na fórmula leaflet: a cada nível de zoom a resolução dobra.
    Em zoom 0 o mundo inteiro ocupa 360°; em zoom N, ocupa 360/2^N graus.
    Ajustamos pra metade do viewport (~1024px) — zoom onde a maior
    diferença (lat ou lon) cobre ~80% do viewport.
    """
    import math
    south, west, north, east = bounds
    lat_diff = max(abs(float(north) - float(south)), 1e-6)
    lon_diff = max(abs(float(east) - float(west)), 1e-6)
    # Corrige leve compressão de longitude em latitudes de SP (~-22°).
    lon_diff_eq = lon_diff * math.cos(math.radians((float(north) + float(south)) / 2.0))
    deg = max(lat_diff, lon_diff_eq)
    # 360 / 2^zoom ≈ deg/0.8 (queremos 80% do viewport ocupado)
    try:
        zoom = int(round(math.log2(360.0 * 0.8 / deg)))
    except (ValueError, ZeroDivisionError):
        zoom = 10
    return max(5, min(17, zoom))


# --- Filtros hierárquicos CPA → BTL → CIA ---------------------------------
# A PMESP organiza a estrutura em três níveis. Quando o usuário fixa um
# Comando, só faz sentido listar os batalhões DAQUELE comando; idem para CIA
# dentro do batalhão. Essas helpers leem as colunas de relacionamento já
# presentes nos GeoJSON e filtram os rótulos disponíveis.
def btl_options_by_cpa(cpa_label: Optional[str]) -> list[str]:
    """Rótulos de Batalhão filtrados pelo Comando pai.

    ``cpa_label`` None/vazio → retorna todos os batalhões.
    """
    layers = load_pmesp_layers()
    gdf = layers.get("Batalhão PMESP")
    if gdf is None:
        return []
    if not cpa_label:
        return sorted(gdf["__label__"].dropna().astype(str).unique().tolist())
    # Nas nossas camadas, o batalhão guarda o pai em 'cmdo' ou 'cmdo_label'.
    parent_col = next(
        (c for c in ("cmdo_label", "cmdo", "CMDO", "CPA", "cmdo_BTL") if c in gdf.columns),
        None,
    )
    if parent_col is None:
        return sorted(gdf["__label__"].dropna().astype(str).unique().tolist())
    mask = gdf[parent_col].astype("string").str.strip() == str(cpa_label).strip()
    return sorted(gdf.loc[mask, "__label__"].dropna().astype(str).unique().tolist())


def cia_options_by_btl(btl_label: Optional[str]) -> list[str]:
    """Rótulos de Companhia filtrados pelo Batalhão pai.

    ``btl_label`` None/vazio → retorna todas as companhias.
    """
    layers = load_pmesp_layers()
    gdf = layers.get("Companhia PMESP")
    if gdf is None:
        return []
    if not btl_label:
        return sorted(gdf["__label__"].dropna().astype(str).unique().tolist())
    parent_col = next(
        (c for c in ("btl", "BTL", "btl_CIA") if c in gdf.columns), None,
    )
    if parent_col is None:
        return sorted(gdf["__label__"].dropna().astype(str).unique().tolist())
    mask = gdf[parent_col].astype("string").str.strip() == str(btl_label).strip()
    return sorted(gdf.loc[mask, "__label__"].dropna().astype(str).unique().tolist())


def _add_pmesp_layers(fmap: folium.Map, with_labels: bool = False) -> None:
    """Desenha as 3 camadas PMESP na ordem correta de empilhamento +
    uma camada ÚNICA de rótulos (desligada por default, controlável pelo
    LayerControl do próprio mapa).

    Ordem de inserção determina pintura no leaflet: CIA primeiro → fundo,
    BTL depois → meio, Comando por último → topo. Os rótulos ficam em um
    FeatureGroup separado ("🔤 Rótulos PMESP"), ``show=with_labels``, para
    que o usuário possa ligar/desligar pelo próprio controle de camadas do
    mapa — sem necessidade de rerun no Streamlit.
    """
    layers = load_pmesp_layers()
    # 1) Linhas PMESP (3 FeatureGroups independentes, empilhados CIA→BTL→CPA)
    for recorte in STACKING_ORDER:
        gdf = layers.get(recorte)
        if gdf is None:
            continue
        style = LAYER_STYLE[recorte]
        fg = folium.FeatureGroup(name=f"🗺️ {recorte}", show=True)
        folium.GeoJson(
            data=gdf.to_json(),
            name=recorte,
            style_function=lambda feat, _s=style: {
                "color": _s["color"],
                "weight": _s["weight"],
                "opacity": _s["opacity"],
                "fillOpacity": 0.0,
                "fill": False,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=["__label__"], aliases=[recorte], sticky=True,
            ),
        ).add_to(fg)
        fg.add_to(fmap)

    # 2) Rótulos — TODOS em UM só FeatureGroup toggleable pelo LayerControl
    labels_fg = folium.FeatureGroup(name="🔤 Rótulos PMESP", show=with_labels)
    for recorte in STACKING_ORDER:
        gdf = layers.get(recorte)
        if gdf is None:
            continue
        style = LAYER_STYLE[recorte]
        css = _halo_label_css(style["color"], size_px={
            "Comando (CPA)": 12, "Batalhão PMESP": 11, "Companhia PMESP": 9,
        }[recorte])
        for _, row in gdf.iterrows():
            if pd.isna(row["__lat__"]) or pd.isna(row["__lon__"]):
                continue
            folium.map.Marker(
                location=(float(row["__lat__"]), float(row["__lon__"])),
                icon=DivIcon(
                    icon_size=(1, 1),
                    icon_anchor=(0, 0),
                    html=f"<div style='{css}'>{row['__label__']}</div>",
                ),
            ).add_to(labels_fg)
    labels_fg.add_to(fmap)


# =========================================================================
# Camadas de DADOS
# =========================================================================

def _add_choropleth(fmap: folium.Map, gdf, value_col: str, key_col: str,
                    label_col: Optional[str] = None) -> tuple[float, float]:
    """Adiciona o coroplético. Retorna ``(vmin, vmax)`` para a legenda
    unificada — a rampa de cores nativa do branca NÃO é inserida no mapa;
    quem desenha a legenda é ``legenda_unificada_html()`` (task #32).
    """
    import branca.colormap as cm

    vals = gdf[value_col].fillna(0).astype(float)
    vmin, vmax = float(vals.min()), float(vals.max())
    if vmax <= vmin:
        vmax = vmin + 1.0
    palette = cm.linear.YlOrRd_09.scale(vmin, vmax)

    def _style(feat):
        v = feat["properties"].get(value_col, 0) or 0
        return {
            "fillColor": palette(v),
            "color": "#555555",
            "weight": 0.4,
            "fillOpacity": 0.75,
        }

    # Aliases legíveis — o campo cru (ex.: "DpGeoDes", "DpGeoCod") vira um
    # rótulo amigável ("Delegacia", "Código do DP") no tooltip. Cai pro
    # próprio nome da coluna quando não houver mapeamento.
    PRETTY = {
        "DpGeoDes":  "Delegacia",
        "DpGeoCod":  "Código do DP",
        "NM_MUN":    "Município",
        "CD_MUN":    "Código IBGE",
        "sc_cod":    "Setor Censitário",
        "OPM":       "Batalhão",
        "OPMCOD":    "Companhia (OPMCOD)",
        "cmdo_label":"Comando",
        "N":         "Ocorrências",
    }

    def _alias(col: str) -> str:
        return PRETTY.get(col, col)

    tooltip_fields = [key_col, value_col]
    tooltip_aliases = [_alias(key_col), _alias(value_col)]
    if label_col and label_col in gdf.columns:
        tooltip_fields.insert(0, label_col)
        tooltip_aliases.insert(0, _alias(label_col))

    folium.GeoJson(
        data=gdf.to_json(),
        name="📊 Coroplético",
        style_function=_style,
        tooltip=folium.GeoJsonTooltip(
            fields=tooltip_fields, aliases=tooltip_aliases, sticky=True,
            localize=True,
        ),
    ).add_to(fmap)
    # Não adiciona a legenda padrão do branca — a legenda unificada é renderizada
    # abaixo do mapa (HTML customizado via ``legenda_unificada_html``).
    return vmin, vmax


def _points_color_map(categories: Iterable) -> dict[str, str]:
    PALETTE = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
        "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5",
        "#c49c94", "#f7b6d2", "#c7c7c7", "#dbdb8d", "#9edae5",
    ]
    cats = sorted({
        c for c in categories
        if c is not None and not (isinstance(c, float) and np.isnan(c))
    })
    return {c: PALETTE[i % len(PALETTE)] for i, c in enumerate(cats)}


def _add_points(fmap: folium.Map, pts: pd.DataFrame,
                color_col: str = "NATUREZA_APURADA",
                radius: int = 3,
                min_visible_zoom: int = 6,
                periodo_label: Optional[str] = None) -> dict[str, str]:
    """Pontos individuais com popup enriquecido e zoom-gate.

    Radius=3 (pedido do cliente). Sem MarkerCluster: camada inteira fica
    oculta abaixo de `min_visible_zoom` via JS no `zoomend`. Acima desse
    limiar os pontos aparecem individualmente.

    Popup mostra: Indicador (natureza), Período (string passada pela Home)
    e Data da ocorrência (quando disponível).
    """
    colors = _points_color_map(pts[color_col].dropna().unique())

    fg = folium.FeatureGroup(name="📍 Pontos (ocorrências)", show=True)
    fg.add_to(fmap)

    has_data = "DATA_OCORRENCIA_BO" in pts.columns
    has_mun = "NOME_MUNICIPIO" in pts.columns

    for _, r in pts.iterrows():
        cat = r.get(color_col)
        c = colors.get(cat, "#1f77b4") if pd.notna(cat) else "#1f77b4"

        popup_rows = [f"<b>Indicador:</b> {cat if pd.notna(cat) else '—'}"]
        if periodo_label:
            popup_rows.append(f"<b>Período filtrado:</b> {periodo_label}")
        if has_data:
            d = r.get("DATA_OCORRENCIA_BO")
            if pd.notna(d):
                try:
                    d = pd.to_datetime(d).strftime("%d/%m/%Y")
                except Exception:
                    pass
                popup_rows.append(f"<b>Data da ocorrência:</b> {d}")
        if has_mun:
            mun = r.get("NOME_MUNICIPIO")
            if pd.notna(mun):
                popup_rows.append(f"<b>Município:</b> {mun}")
        popup_html = (
            "<div style='font-size:.85rem; line-height:1.35;'>"
            + "<br>".join(popup_rows) + "</div>"
        )
        folium.CircleMarker(
            location=(float(r["LATITUDE"]), float(r["LONGITUDE"])),
            radius=radius,
            color=c,
            weight=1,
            fill=True,
            fill_color=c,
            fill_opacity=0.85,
            popup=folium.Popup(popup_html, max_width=320),
        ).add_to(fg)

    # Zoom-gate: abaixo de min_visible_zoom, remove o FG do mapa; acima, adiciona.
    _inject_zoom_gate(fmap, fg, min_zoom=min_visible_zoom)

    return colors


class _ZoomGate(MacroElement):
    """MacroElement: injeta JS DENTRO do bloco <script> do folium, após as
    declarações de ``var <map_name>`` e ``var <layer_name>`` — assim as refs
    JS apontam direto para as variáveis locais do folium (não ``window.X``,
    que é onde o approach antigo falhava: o script era emitido no <html>
    antes do <script> do folium declarar as vars, então ``window[name]``
    sempre ficava undefined e os pontos nunca apareciam).
    """

    _template = Template(u"""
        {% macro script(this, kwargs) %}
        (function() {
            var m = {{this._parent.get_name()}};
            var l = {{this._layer_name}};
            function refresh() {
                var z = m.getZoom();
                if (z >= {{this._min_zoom}}) {
                    if (!m.hasLayer(l)) l.addTo(m);
                } else {
                    if (m.hasLayer(l)) m.removeLayer(l);
                }
            }
            m.on('zoomend', refresh);
            refresh();
        })();
        {% endmacro %}
    """)

    def __init__(self, layer, min_zoom: int):
        super().__init__()
        self._name = "ZoomGate"
        self._layer_name = layer.get_name()
        self._min_zoom = int(min_zoom)


def _inject_zoom_gate(fmap: folium.Map, layer, min_zoom: int) -> None:
    """Liga/desliga ``layer`` conforme o zoom cruza ``min_zoom``.

    O MacroElement é adicionado ao mapa, então seu ``script()`` é renderizado
    no mesmo <script> onde ``var <map> = L.map(...)`` e ``var <fg> = ...``
    já estão declarados. Isso garante que ``m`` e ``l`` existam quando o
    código rodar — corrigindo o bug que mantinha os pontos invisíveis.
    """
    gate = _ZoomGate(layer, min_zoom=min_zoom)
    gate.add_to(fmap)


def _add_hotspot(fmap: folium.Map, pts: pd.DataFrame) -> None:
    """Mapa de calor com parâmetros calibrados pra VISIBILIDADE EM ZOOM BAIXO.

    ``maxZoom`` alto mantém o calor aceso em zoom estadual; ``radius`` e
    ``blur`` mais agressivos espalham o sinal no nível 6-8.
    """
    data = pts[["LATITUDE", "LONGITUDE"]].dropna().to_numpy().tolist()
    if not data:
        return
    HeatMap(
        data=data,
        name="🔥 Hotspot",
        radius=28,          # ↑ (era 14): mancha maior pra ler em zoom baixo
        blur=35,            # ↑ (era 20): suaviza, mantém o heat contínuo
        min_opacity=0.55,   # ↑ (era 0.3): garante contraste sobre o basemap claro
        max_zoom=13,        # ↑ faz o heat "intensificar" até zoom 13 antes de saturar
        gradient={
            0.2: "#2c7bb6",
            0.4: "#abd9e9",
            0.6: "#ffffbf",
            0.8: "#fdae61",
            1.0: "#d7191c",
        },
    ).add_to(fmap)


# =========================================================================
# Orquestrador: build_map()
# =========================================================================
@dataclass
class PointsData:
    df: pd.DataFrame                 # LATITUDE, LONGITUDE, NATUREZA_APURADA
    color_col: str = "NATUREZA_APURADA"
    periodo_label: Optional[str] = None   # string para exibir no popup


@dataclass
class ChoroplethData:
    gdf: "object"                    # GeoDataFrame já mergeado
    value_col: str
    key_col: str
    label_col: Optional[str] = None


def build_map(
    modo: str = "choropleth",
    pts_data: Optional[PointsData] = None,
    choro_data: Optional[ChoroplethData] = None,
    center: tuple[float, float] = (-22.4, -48.5),
    zoom: int = 7,
    with_pmesp_labels: bool = True,
    endereco_marker: Optional[tuple[float, float, str]] = None,
    points_min_zoom: int = 6,
    fit_bounds: Optional[tuple[float, float, float, float]] = None,
    with_draw_tools: bool = False,
) -> folium.Map:
    """Constrói o mapa único usado na Home.

    Args:
      modo: 'choropleth' | 'pontos' | 'hotspot'.
      pts_data: payload pros modos 'pontos' / 'hotspot'.
      choro_data: payload pro modo 'choropleth'.
      center, zoom: estado inicial do viewport (persistido via session_state).
      with_pmesp_labels: ativa rótulos halo-branco em cada polígono PMESP.
      endereco_marker: (lat, lon, nome) pra desenhar o pino do endereço buscado.
      points_min_zoom: zoom mínimo pra exibir pontos individualmente (default 6).
      fit_bounds: (south, west, north, east) — quando fornecido, o leaflet
        ajusta o viewport ao retângulo envolvente do polígono, sobrepondo
        ``center``/``zoom`` iniciais. Útil pro "ir para" com zoom proporcional.
    """
    fmap = folium.Map(
        location=list(center),
        zoom_start=zoom,
        tiles="CartoDB Positron",
        scrollWheelZoom=True,
        control_scale=True,
        prefer_canvas=True,
    )

    # Intervalo do coroplético (quando aplicável) — usado pela legenda unificada.
    fmap._choro_range = None  # type: ignore[attr-defined]

    # --- Sem camadas PMESP (pedido do cliente, abr/2026) -------------------
    # Os limites administrativos CPA/BTL/CIA foram removidos do mapa. O
    # cliente quer ver apenas Delegacia (DP) e Setor Censitário, que são
    # renderizadas sob demanda via o modo coroplético (a escolha vem da
    # sidebar, em ``f.recorte``). Rótulos halo-branco também foram suprimidos.
    # As helpers (load_pmesp_layers, pmesp_bounds, btl_options_by_cpa...)
    # seguem no módulo pra compat com código antigo, só deixaram de ser
    # chamadas aqui.

    # --- Dados (uma camada exclusiva, AGORA por cima do PMESP) ---
    if modo == "choropleth" and choro_data is not None:
        vmin, vmax = _add_choropleth(
            fmap, gdf=choro_data.gdf,
            value_col=choro_data.value_col, key_col=choro_data.key_col,
            label_col=choro_data.label_col,
        )
        fmap._choro_range = (vmin, vmax, choro_data.value_col)  # type: ignore[attr-defined]
    elif modo == "pontos" and pts_data is not None and not pts_data.df.empty:
        _add_points(
            fmap, pts_data.df, color_col=pts_data.color_col, radius=3,
            min_visible_zoom=points_min_zoom, periodo_label=pts_data.periodo_label,
        )
    elif modo == "hotspot" and pts_data is not None and not pts_data.df.empty:
        _add_hotspot(fmap, pts_data.df)

    # --- Pin do endereço pesquisado ---
    if endereco_marker:
        lat, lon, nome = endereco_marker
        folium.Marker(
            location=(lat, lon),
            tooltip=nome,
            icon=folium.Icon(color="green", icon="search"),
        ).add_to(fmap)

    # --- Ferramenta laço (polígono / retângulo / círculo) --------------
    # Plugin Leaflet.Draw: permite ao usuário desenhar uma forma livre no
    # mapa. A geometria sai em ``ret['last_active_drawing']`` pelo st_folium
    # e é usada em downstream pra filtro point-in-polygon (shapely).
    if with_draw_tools:
        Draw(
            export=False,
            position="topleft",
            draw_options={
                "polyline": False,           # linha não seleciona área
                "circlemarker": False,        # sem circle markers (sem raio)
                "polygon": {"allowIntersection": False, "showArea": True},
                "rectangle": {"shapeOptions": {"color": "#c8102e"}},
                "circle": {"shapeOptions": {"color": "#c8102e"}},
                "marker": False,
            },
            edit_options={"edit": True, "remove": True},
        ).add_to(fmap)

    folium.LayerControl(collapsed=False, position="topright").add_to(fmap)

    # --- fit_bounds (opcional) — enquadra polígono/feature de interesse ---
    # Deve vir por ÚLTIMO: o leaflet aplica o enquadramento depois de todos
    # os layers já terem sido adicionados.
    if fit_bounds is not None:
        south, west, north, east = fit_bounds
        try:
            fmap.fit_bounds([[float(south), float(west)], [float(north), float(east)]])
        except Exception:
            pass

    return fmap


def points_in_drawing(
    pts: pd.DataFrame,
    drawing: Optional[dict],
    lat_col: str = "LATITUDE",
    lon_col: str = "LONGITUDE",
) -> pd.DataFrame:
    """Filtra ``pts`` pras linhas cujas coordenadas caem dentro da forma
    desenhada no mapa (polígono, retângulo ou círculo).

    ``drawing`` é um GeoJSON Feature retornado por st_folium em
    ``ret["last_active_drawing"]``. A geometria pode ser:

    - ``Polygon`` / ``Rectangle`` → convertemos direto em ``shapely.Polygon``.
    - ``Circle`` (no Leaflet.Draw vira uma Feature com Point na geometria e
      raio em metros nas properties) → expandimos pra um círculo aproximado
      via buffer em coordenadas locais (ver nota abaixo).

    Retorna ``pts`` vazio se a forma for inválida, ou o subset em ordem
    original. Fica silencioso em exceções (no máximo devolve o df inteiro
    sem filtro) pra não quebrar a Home quando o laço for malformado.
    """
    if drawing is None or pts is None or pts.empty:
        return pts.iloc[0:0] if pts is not None else pd.DataFrame()

    try:
        from shapely.geometry import shape, Point
        from shapely.geometry.polygon import Polygon
    except Exception:
        # shapely indisponível (não deveria, faz parte do stack geo) —
        # devolve vazio pra o chamador exibir um aviso.
        return pts.iloc[0:0]

    geom_dict = drawing.get("geometry") if isinstance(drawing, dict) else None
    props = drawing.get("properties") if isinstance(drawing, dict) else None
    if not geom_dict:
        return pts.iloc[0:0]

    try:
        # Caso especial: leaflet-draw representa Circle como Feature Point +
        # property "radius" (metros). Shapely não tem círculo nativo — usamos
        # comparação por distância em graus equivalentes.
        if (geom_dict.get("type") == "Point"
                and isinstance(props, dict) and "radius" in props):
            lon_c, lat_c = geom_dict["coordinates"]
            r_m = float(props["radius"])
            # Converte raio em METROS → GRAUS aproximados (latitude de SP:
            # 1° latitude ≈ 111.32 km; longitude varia com cos(lat)).
            import math
            r_lat = r_m / 111_320.0
            r_lon = r_m / (111_320.0 * math.cos(math.radians(float(lat_c))))
            # Filtro elíptico (aproximação decente para raios ≤ dezenas de km).
            dlat = pts[lat_col].astype(float) - float(lat_c)
            dlon = pts[lon_col].astype(float) - float(lon_c)
            mask = ((dlat / r_lat) ** 2 + (dlon / r_lon) ** 2) <= 1.0
            return pts.loc[mask].copy()

        poly = shape(geom_dict)
        if not isinstance(poly, Polygon):
            return pts.iloc[0:0]

        # vectorizado via prepared geometry pra acelerar pra 50k pontos
        from shapely.prepared import prep
        prepared = prep(poly)
        mask = pts.apply(
            lambda r: prepared.contains(Point(
                float(r[lon_col]), float(r[lat_col]),
            )),
            axis=1,
        )
        return pts.loc[mask].copy()
    except Exception:
        return pts.iloc[0:0]


def legenda_pontos_html(colors: dict[str, str]) -> str:
    if not colors:
        return ""
    items = "".join(
        f"<span style='display:inline-flex; align-items:center; margin:2px 8px 2px 0;'>"
        f"<span style='display:inline-block; width:10px; height:10px; "
        f"background:{c}; border-radius:50%; margin-right:4px;'></span>"
        f"<span style='font-size:.82rem;'>{k}</span></span>"
        for k, c in colors.items()
    )
    return f"<div style='line-height:1.6;'>{items}</div>"


def legenda_unificada_html(
    choro_range: Optional[tuple[float, float, str]] = None,
    points_colors: Optional[dict[str, str]] = None,
) -> str:
    """Legenda unificada (renderizada como bloco HTML abaixo do mapa).

    Inclui:
      • Indicadores de linha das 3 camadas PMESP (sempre visíveis).
      • Rampa de cores do coroplético INLINE, com rótulos em números **inteiros**
        nos extremos e no meio — nada de decimais.
      • Cores dos pontos (quando em modo 'pontos'), quando ``points_colors``
        estiver preenchido.
    """
    blocks: list[str] = []

    # (1) Linhas PMESP foram removidas da legenda — o mapa não desenha mais
    # os limites CPA/BTL/CIA (pedido do cliente, abr/2026). Mantemos a
    # legenda focada no dado efetivamente exibido (coroplético/pontos).

    # (2) Rampa do coroplético com valores INTEIROS
    if choro_range is not None:
        vmin, vmax, value_col = choro_range
        vmin_i = int(round(float(vmin)))
        vmax_i = int(round(float(vmax)))
        if vmax_i <= vmin_i:
            vmax_i = vmin_i + 1
        vmid_i = int(round((vmin_i + vmax_i) / 2))
        vq1_i = int(round(vmin_i + (vmax_i - vmin_i) * 0.25))
        vq3_i = int(round(vmin_i + (vmax_i - vmin_i) * 0.75))
        gradient_css = (
            "linear-gradient(to right, "
            "#ffffcc 0%, #ffeda0 12.5%, #fed976 25%, "
            "#feb24c 37.5%, #fd8d3c 50%, #fc4e2a 62.5%, "
            "#e31a1c 75%, #bd0026 87.5%, #800026 100%)"
        )

        def fmt_i(n: int) -> str:
            return f"{n:,}".replace(",", ".")

        blocks.append(
            f"<div style='margin-top:10px;'>"
            f"  <div style='font-size:.82rem; font-weight:600; margin-bottom:4px;'>"
            f"    📊 Coroplético · <span style='color:#0C2B4E'>{value_col}</span>"
            f"  </div>"
            f"  <div style='background:{gradient_css}; height:14px; "
            f"       border-radius:3px; border:1px solid #ccc;'></div>"
            f"  <div style='display:flex; justify-content:space-between; "
            f"       font-size:.75rem; color:#333; margin-top:2px;'>"
            f"    <span>{fmt_i(vmin_i)}</span>"
            f"    <span>{fmt_i(vq1_i)}</span>"
            f"    <span>{fmt_i(vmid_i)}</span>"
            f"    <span>{fmt_i(vq3_i)}</span>"
            f"    <span>{fmt_i(vmax_i)}</span>"
            f"  </div>"
            f"</div>"
        )

    # (3) Cores dos pontos (quando ativo)
    if points_colors:
        items = "".join(
            f"<span style='display:inline-flex; align-items:center; margin:2px 10px 2px 0;'>"
            f"<span style='display:inline-block; width:10px; height:10px; "
            f"background:{c}; border-radius:50%; margin-right:5px;'></span>"
            f"<span style='font-size:.8rem;'>{k}</span></span>"
            for k, c in points_colors.items()
        )
        blocks.append(
            "<div style='margin-top:10px;'>"
            "  <div style='font-size:.82rem; font-weight:600; margin-bottom:4px;'>"
            "    📍 Pontos (cores por natureza, tamanho 3)"
            "  </div>"
            f"  <div style='line-height:1.6;'>{items}</div>"
            "</div>"
        )

    body = "".join(blocks)
    return (
        "<div style='border:1px solid #e5e7eb; border-radius:6px; "
        "padding:10px 14px; background:#fafbfc;'>"
        f"  <div style='font-size:.9rem; font-weight:700; color:#0C2B4E; "
        f"       margin-bottom:6px;'>Legenda</div>"
        f"  {body}"
        "</div>"
    )
