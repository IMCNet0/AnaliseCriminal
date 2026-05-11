"""Home — KPIs + mapa interativo (coroplético / pontos / hotspot).

Regras desta rodada (abr/2026):
  • Mapa SEM limites administrativos PMESP — apenas Delegacia (DP) e
    Setor Censitário aparecem, via coroplético, quando o usuário escolhe
    o recorte na sidebar. Rótulos halo-branco foram suprimidos.
  • Filtros (período, naturezas, recorte) persistem ao alternar entre
    páginas — controlados por ``lib.filters`` via session_state.
  • Default de período = último mês com dado disponível (plant no first-run).
  • Default do mapa = **coroplético por Delegacia** com a natureza top-1
    pré-selecionada (pedido cliente abr/26 #3). Camada temática continua
    dependendo de ≥1 natureza; se o usuário limpar o multiselect, o mapa
    volta pra base sem tema.
  • Busca de endereço com autocomplete inline via datalist HTML.
"""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

from lib.branding import apply_brand, header
from lib.filters import sidebar_filters, sidebar_footer
from lib import data, geo as geolib
from lib.map_builder import (
    build_map, PointsData, ChoroplethData,
    geocode_many,
    legenda_unificada_html, _points_color_map,
    points_in_drawing,
)


apply_brand("Home · InsightGeoLab AI")
header("Portal de Análise Criminal · SP-Capital",
       "Visão espacial dos indicadores criminais da Cidade de São Paulo")

f = sidebar_filters()

# Reset do mapa na sidebar (zoom/centro). Não mexe nos filtros globais
# (esses persistem entre páginas por design).
reset_map = st.sidebar.button("↺ Reset do mapa", use_container_width=True)

sidebar_footer()


# =========================================================================
# 1) KPIs
# =========================================================================
# Quando o usuário escolhe uma DP na sidebar, `serie_contextual` troca
# transparentemente a série estadual por por_dp filtrado → os KPIs,
# gráficos e gauges passam a refletir somente aquela delegacia.
serie = data.serie_contextual(f.dp_cod)
if serie.empty:
    if f.dp_cod:
        st.info(
            f"Sem dados para a delegacia **{f.dp_des}** no agregado "
            f"`por_dp.parquet`. Verifique se o pipeline foi rodado para o período."
        )
    else:
        st.info(
            "Ainda não há agregados em `data/aggregates/`. "
            "Rode `python pipeline/run_all.py` depois de colocar os .xlsx em `data/raw/ssp/`."
        )
    st.stop()

mask_serie = f.mask_date(serie) & f.mask_natureza(serie)
serie_f = serie.loc[mask_serie]

# Aviso visual quando o DP está ativo — reforça que toda a página está
# escopada a essa delegacia.
if f.dp_cod:
    st.info(
        f"🏛️ **Escopo ativo:** Delegacia `{f.dp_des}` — todos os números "
        f"abaixo (KPIs, mapa, séries) referem-se apenas a essa DP. "
        f"Para voltar à visão estadual, selecione **Todos os DPs** na sidebar.",
        icon="🏛️",
    )

total_periodo = int(serie_f["N"].sum())
ultimo_ano = int(serie_f["ANO"].max()) if not serie_f.empty else f.data_fim.year
total_ultimo_ano = int(serie_f.loc[serie_f["ANO"] == ultimo_ano, "N"].sum())
anos_unicos = sorted(serie_f["ANO"].unique())
if len(anos_unicos) >= 2:
    prev = int(serie_f.loc[serie_f["ANO"] == anos_unicos[-2], "N"].sum())
    delta_yoy = (total_ultimo_ano - prev) / prev * 100 if prev else 0.0
else:
    delta_yoy = 0.0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total no período", f"{total_periodo:,}".replace(",", "."))
k2.metric(
    f"Total em {ultimo_ano}", f"{total_ultimo_ano:,}".replace(",", "."),
    f"{delta_yoy:+.1f}% vs. ano anterior",
    delta_color="inverse",
)
k3.metric(
    "Naturezas incluídas",
    f"{serie_f['NATUREZA_APURADA'].nunique():,}".replace(",", "."),
)
k4.metric(
    "Meses cobertos",
    f"{serie_f.groupby(['ANO','MES']).ngroups:,}".replace(",", "."),
)

st.divider()


# =========================================================================
# 2) Estado do mapa (persistido entre reruns)
# =========================================================================
# Default ajustado para a Cidade de São Paulo (rodada abr/26 #4): centro ≈
# Praça da Sé / Marco zero, zoom 11 enquadra o município inteiro com folga
# pra ver bairros ao redor. Antes era centro do estado de SP (zoom 7).
DEFAULT_CENTER = (-23.5505, -46.6333)   # SP-Capital — Praça da Sé
DEFAULT_ZOOM = 11

if "map_center" not in st.session_state:
    st.session_state.map_center = DEFAULT_CENTER
if "map_zoom" not in st.session_state:
    st.session_state.map_zoom = DEFAULT_ZOOM

if reset_map:
    st.session_state.map_center = DEFAULT_CENTER
    st.session_state.map_zoom = DEFAULT_ZOOM
    st.session_state["_endereco_marker"] = None
    st.rerun()


# =========================================================================
# 3) Controles do corpo da página — modo + busca de endereço
# =========================================================================
st.subheader("Mapa interativo")

ctrl_a, ctrl_b = st.columns([1.2, 3.0])

modo_label = ctrl_a.radio(
    "Visualização",
    ["Coroplético", "Pontos", "Hotspot"],
    horizontal=True,
    index=0,  # default = Coroplético (por Delegacia, via recorte da sidebar)
    help="Escolha uma forma de ver os dados. O default é **Coroplético** "
         "no recorte de **Delegacia** selecionado na sidebar. "
         "A camada temática só aparece quando há ≥1 natureza selecionada.",
)
MODE = {"Coroplético": "choropleth", "Pontos": "pontos", "Hotspot": "hotspot"}[modo_label]

# -------------------------------------------------------------------------
# Busca de endereço — autocomplete real via `streamlit-searchbox`
# -------------------------------------------------------------------------
# Engine substituído na rodada abr/2026 #3: o approach antigo (datalist
# HTML + botão Centralizar) não renderiza de forma confiável dentro do
# iframe do Streamlit — o browser renderiza a datalist no DOM pai e os
# atributos se perdem em reruns. O componente `streamlit-searchbox`
# conversa nativamente com o Streamlit, faz debounce, e devolve o item
# selecionado direto pelo callback — UX de autocomplete "de verdade".
#
# Fallback silencioso: se a lib não estiver instalada (dev fresh), cai
# num text_input + botão (comportamento antigo), pra não quebrar o app.
try:
    from streamlit_searchbox import st_searchbox
    _HAS_SEARCHBOX = True
except ImportError:
    _HAS_SEARCHBOX = False


def _search_enderecos(q: str) -> list[tuple[str, dict]]:
    """Callback do searchbox — recebe o texto em digitação e devolve
    ``(label, payload)`` para cada sugestão. ``label`` aparece no dropdown;
    ``payload`` (dict com lat/lon/display_name) é o que volta no retorno."""
    if not q or len(q.strip()) < 3:
        return []
    hits = geocode_many(q, limit=7)
    return [(h.get("display_name", ""), h) for h in hits if h]


with ctrl_b:
    if _HAS_SEARCHBOX:
        chosen = st_searchbox(
            _search_enderecos,
            placeholder="Endereço dentro da Cidade de São Paulo (ex.: Av. Paulista 1578)",
            label="🔍 Buscar endereço (SP-Capital)",
            key="endereco_searchbox",
            clear_on_submit=False,
            edit_after_submit="current",
        )
        if chosen and isinstance(chosen, dict) and "lat" in chosen:
            st.session_state.map_center = (float(chosen["lat"]), float(chosen["lon"]))
            st.session_state.map_zoom = 15
            st.session_state["_endereco_marker"] = (
                float(chosen["lat"]), float(chosen["lon"]),
                str(chosen.get("display_name", "")),
            )
    else:
        # Fallback — apenas pra dev local sem a lib instalada.
        st.warning(
            "⚠️ `streamlit-searchbox` não instalado — rodando com busca básica. "
            "`pip install streamlit-searchbox` pra ativar o autocomplete."
        )
        endereco_q = st.text_input(
            "Buscar endereço",
            placeholder="Ex.: Av. Paulista 1578, São Paulo",
            key="endereco_q",
        )
        if st.button("🔍 Centralizar", use_container_width=False) and endereco_q:
            hits = geocode_many(endereco_q, limit=1)
            if hits:
                h = hits[0]
                st.session_state.map_center = (h["lat"], h["lon"])
                st.session_state.map_zoom = 15
                st.session_state["_endereco_marker"] = (
                    h["lat"], h["lon"], h["display_name"],
                )
                st.rerun()

    endereco_marker = st.session_state.get("_endereco_marker")


# ── Controles do Hotspot (visíveis apenas quando o modo Hotspot está ativo) ──
if MODE == "hotspot":
    with st.expander("⚙️ Parâmetros do Hotspot", expanded=True):
        hs_c1, hs_c2, hs_c3, hs_c4 = st.columns(4)
        hs_radius = hs_c1.slider(
            "Raio (px)", min_value=5, max_value=60, value=28, step=1,
            key="hs_radius",
            help="Raio de influência de cada ponto em pixels. "
                 "Maior → manchas mais largas.",
        )
        hs_blur = hs_c2.slider(
            "Blur (px)", min_value=5, max_value=60, value=35, step=1,
            key="hs_blur",
            help="Grau de suavização. Maior → transições mais suaves entre quente e frio.",
        )
        hs_min_opacity = hs_c3.slider(
            "Opacidade mín.", min_value=0.10, max_value=1.0, value=0.55, step=0.05,
            key="hs_min_opacity",
            help="Opacidade mínima da camada. "
                 "Maior → áreas com poucas ocorrências ficam mais visíveis.",
        )
        hs_max_zoom = hs_c4.slider(
            "Zoom de saturação", min_value=8, max_value=18, value=13, step=1,
            key="hs_max_zoom",
            help="Nível de zoom a partir do qual o calor para de intensificar. "
                 "Menor → satura mais cedo (zoom baixo); maior → mantém gradiente até zoom alto.",
        )
else:
    hs_radius = int(st.session_state.get("hs_radius", 28))
    hs_blur = int(st.session_state.get("hs_blur", 35))
    hs_min_opacity = float(st.session_state.get("hs_min_opacity", 0.55))
    hs_max_zoom = int(st.session_state.get("hs_max_zoom", 13))


# =========================================================================
# 4) Pipeline de dados — só se houver natureza definida na sidebar
# =========================================================================
pts_data = None
choro_data = None
periodo_label = None

tem_natureza = bool(f.naturezas)

if not tem_natureza:
    st.info(
        "ℹ️ Selecione **uma ou mais naturezas** na aba lateral para ativar "
        "a camada temática. Enquanto isso, o mapa exibe apenas a base."
    )

if tem_natureza and MODE in ("pontos", "hotspot"):
    anos_range = list(range(int(f.ano_ini), int(f.ano_fim) + 1))
    frames: list[pd.DataFrame] = []
    for a in anos_range:
        m_start = f.data_ini.month if a == f.ano_ini else 1
        m_end = f.data_fim.month if a == f.ano_fim else 12
        for m in range(m_start, m_end + 1):
            for nat in f.naturezas:
                df_am = data.pontos(int(a), int(m), nat, dp_cod=f.dp_cod)
                if not df_am.empty:
                    frames.append(df_am)
    pts = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not pts.empty:
        pts = pts.drop_duplicates()

    periodo_label = (
        f"{f.data_ini.strftime('%d/%m/%Y')} a {f.data_fim.strftime('%d/%m/%Y')}"
    )

    pts_data = PointsData(df=pts, periodo_label=periodo_label)

    # Total REAL (sem amostragem) vindo dos agregados — pra explicar o gap
    # entre o que o mapa mostra (amostra) e a contagem oficial SSP.
    total_real = int(serie_f["N"].sum())

    info_col = st.columns(1)[0]
    if not pts.empty:
        amostra_pct = (len(pts) / total_real * 100.0) if total_real else 0.0
        if MODE == "pontos":
            info_col.caption(
                f"📍 {len(pts):,} ponto(s) exibido(s) em {periodo_label} — "
                f"**amostra** representativa de **{total_real:,}** ocorrências "
                f"oficiais (≈ {amostra_pct:.1f}%). "
                f"Pontos visíveis a partir do zoom 6."
            )
        else:
            info_col.caption(
                f"🔥 {len(pts):,} ponto(s) compondo o calor — **amostra** de "
                f"{total_real:,} ocorrências oficiais no período. "
                f"Padrão espacial é preservado pela amostragem uniforme "
                f"(seed fixa)."
            )
        info_col.info(
            "ℹ️ **Por que amostra?** A base completa tem ~600 MB e só cabe na "
            "máquina local. Para rodar no Streamlit Cloud, usamos uma amostra "
            "aleatória (até 5.000 pontos/ANO×MES · seed 42 · "
            "`pipeline/build_sample.py`). **Totais e KPIs acima vêm dos "
            "agregados — são os números reais da SSP-SP**. Use o recorte "
            "coroplético (Delegacia / Setor) quando precisar de contagem "
            "exata espacialmente.",
            icon="ℹ️",
        )
    else:
        info_col.info(
            f"Sem pontos com coordenadas válidas em {periodo_label} "
            f"para as naturezas selecionadas."
        )

if tem_natureza and MODE == "choropleth":
    recorte_choro = f.recorte  # só DP ou Setor (filtros.RECORTES enxuto)
    loader, data_key = data.RECORTE_LOADER[recorte_choro]
    df_rec = loader()
    if df_rec.empty:
        st.warning(
            f"Agregado de **{recorte_choro}** não encontrado. "
            f"Rode `python pipeline/run_all.py` para regerá-lo."
        )
    else:
        # mask_dp é no-op no agregado de Setor (sem DpGeoCod); no de DP,
        # restringe o merge a uma única linha-DP por natureza/mês.
        mask_rec = f.mask_date(df_rec) & f.mask_natureza(df_rec) & f.mask_dp(df_rec)
        agg_cols = [c for c in df_rec.columns if c in {
            data_key, "NM_MUN", "regiao", "DpGeoDes", "CD_MUN", "sc_cod",
        }]
        agg = (
            df_rec.loc[mask_rec]
            .groupby([data_key] + [c for c in agg_cols if c != data_key],
                     as_index=False, observed=True)["N"].sum()
        )

        gdf, geo_key = geolib.load_layer(recorte_choro)
        if gdf is None:
            st.warning(f"Camada `{recorte_choro}` não encontrada em `data/geo/`.")
        else:
            def _norm_key(s: pd.Series) -> pd.Series:
                nums = pd.to_numeric(s, errors="coerce")
                valid = nums.dropna()
                if not valid.empty and (valid == valid.astype("int64")).all():
                    return nums.astype("Int64").astype("string")
                return s.astype("string").str.strip().str.upper()

            gdf = gdf.copy()
            agg_local = agg.copy()
            gdf[geo_key] = _norm_key(gdf[geo_key])
            agg_local[data_key] = _norm_key(agg_local[data_key])

            merged = gdf.merge(agg_local, left_on=geo_key, right_on=data_key, how="left")
            merged["N"] = merged["N"].fillna(0)

            label_col = next(
                (c for c in ["DpGeoDes", "NM_MUN", "__label__"]
                 if c in merged.columns),
                None,
            )
            choro_data = ChoroplethData(
                gdf=merged, value_col="N", key_col=geo_key, label_col=label_col,
            )

            n_com_valor = int((merged["N"] > 0).sum())
            if n_com_valor == 0:
                st.warning(
                    f"⚠️ Nenhum polígono de **{recorte_choro}** recebeu valor > 0 "
                    f"para o filtro atual. Verifique período/natureza."
                )
            elif recorte_choro == "Setor Censitário":
                st.caption(
                    f"ℹ️ Setor Censitário é a camada mais granular (~250k polígonos). "
                    f"Em zoom baixo fica denso; aproxime para ler com conforto."
                )

            # Quando o usuário fixou uma DP, enquadramos o mapa nela.
            # Guardamos no session_state pra o build_map abaixo consumir.
            if f.dp_cod and recorte_choro == "Delegacia (DP)":
                try:
                    sel_poly = merged.loc[merged[geo_key].astype(str).str.strip()
                                          == str(f.dp_cod).strip()]
                    if not sel_poly.empty:
                        minx, miny, maxx, maxy = sel_poly.geometry.total_bounds
                        if not any(pd.isna(v) for v in (minx, miny, maxx, maxy)):
                            st.session_state["_dp_fit_bounds"] = (
                                float(miny), float(minx), float(maxy), float(maxx),
                            )
                except Exception:
                    pass


# =========================================================================
# 5) Renderização do mapa
# =========================================================================
# fit_bounds: quando a DP está fixada, enquadra o polígono dela
# (definido no bloco do coroplético acima); senão deixa o zoom/center livres.
dp_bounds = st.session_state.pop("_dp_fit_bounds", None) if f.dp_cod else None

fmap = build_map(
    modo=MODE,
    pts_data=pts_data,
    choro_data=choro_data,
    center=st.session_state.map_center,
    zoom=st.session_state.map_zoom,
    with_pmesp_labels=False,
    endereco_marker=endereco_marker,
    points_min_zoom=6,
    fit_bounds=dp_bounds,
    with_draw_tools=True,
    hotspot_radius=hs_radius,
    hotspot_blur=hs_blur,
    hotspot_min_opacity=hs_min_opacity,
    hotspot_max_zoom=hs_max_zoom,
)

ret = st_folium(
    fmap,
    use_container_width=True,
    height=640,
    returned_objects=["zoom", "center", "last_active_drawing", "all_drawings"],
    key="home_map",
)
if ret:
    if ret.get("zoom") is not None:
        st.session_state.map_zoom = int(ret["zoom"])
    c = ret.get("center")
    if c and "lat" in c and "lng" in c:
        st.session_state.map_center = (float(c["lat"]), float(c["lng"]))

# ---------------------------------------------------------------------------
# Ferramenta laço — análise de ocorrências dentro da forma desenhada
# ---------------------------------------------------------------------------
# O usuário pode desenhar no mapa (polígono/retângulo/círculo) via os
# ícones no canto superior esquerdo. Capturamos a geometria no retorno do
# st_folium e fazemos point-in-polygon via shapely sobre o DataFrame de
# pontos já carregado pra o período atual.
drawing = (ret or {}).get("last_active_drawing")
if drawing and pts_data is not None and not pts_data.df.empty:
    subset = points_in_drawing(pts_data.df, drawing)
    st.subheader("🔗 Seleção por laço")
    if subset.empty:
        st.warning(
            "Nenhum ponto caiu dentro da forma desenhada. "
            "Tente ampliar a área ou redesenhar."
        )
    else:
        k1, k2, k3 = st.columns(3)
        k1.metric("Pontos na seleção", f"{len(subset):,}".replace(",", "."))
        k2.metric(
            "Naturezas distintas",
            f"{subset['NATUREZA_APURADA'].nunique():,}".replace(",", "."),
        )
        if "NOME_MUNICIPIO" in subset.columns:
            k3.metric(
                "Municípios",
                f"{subset['NOME_MUNICIPIO'].nunique():,}".replace(",", "."),
            )

        # Ranking rápido de naturezas dentro da seleção
        rank_sel = (
            subset.groupby("NATUREZA_APURADA", dropna=True)
            .size().sort_values(ascending=False).head(10)
            .rename("Ocorrências (amostra)").reset_index()
        )
        st.dataframe(rank_sel, use_container_width=True)

        # Export da seleção pra CSV
        cols_keep = [c for c in [
            "DATA_OCORRENCIA_BO", "NATUREZA_APURADA", "NOME_MUNICIPIO",
            "LATITUDE", "LONGITUDE",
        ] if c in subset.columns]
        csv = subset[cols_keep].to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇ Baixar seleção (CSV)",
            data=csv,
            file_name="selecao_laco.csv",
            mime="text/csv",
            use_container_width=False,
        )
        st.caption(
            "ℹ️ A contagem acima é sobre a **amostra exibida** no mapa. "
            "Para números exatos da SSP dentro de um recorte administrativo, "
            "use o coroplético por DP / Setor Censitário."
        )

# -------------------------------------------------------------------------
# Legenda unificada abaixo do mapa
# -------------------------------------------------------------------------
points_colors = None
if MODE == "pontos" and pts_data is not None and not pts_data.df.empty:
    points_colors = _points_color_map(
        pts_data.df["NATUREZA_APURADA"].dropna().unique()
    )

choro_range = getattr(fmap, "_choro_range", None)
st.markdown(
    legenda_unificada_html(choro_range=choro_range, points_colors=points_colors),
    unsafe_allow_html=True,
)

st.caption(
    "📍 Use **scroll do mouse** para zoom e arraste para pan. "
    "Use os ícones **▢ polígono · ▭ retângulo · ⊙ círculo** no canto "
    "superior esquerdo para selecionar ocorrências dentro de uma área. "
    "O recorte do coroplético (Delegacia ou Setor Censitário) vem da "
    "aba lateral."
)
