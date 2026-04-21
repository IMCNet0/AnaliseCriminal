"""Mapa coroplético + pontos (drill-down), respeitando o recorte geográfico."""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import json as _json
import pandas as pd
import streamlit as st
import plotly.express as px

from lib.branding import apply_brand, header
from lib.filters import sidebar_filters
from lib import data, geo as geolib
from lib.downloads import download_buttons

apply_brand("Mapa · InsightGeoLab AI")
header("Distribuição espacial das ocorrências")

f = sidebar_filters()

loader, data_key = data.RECORTE_LOADER[f.recorte]
df = loader()
if df.empty:
    st.info("Agregado ainda não gerado. Rode `python pipeline/run_all.py`.")
    st.stop()

mask = f.mask_date(df) & f.mask_natureza(df)
agg_cols = [c for c in df.columns if c in {
    data_key, "NM_MUN", "cmdo_BTL", "btl_CIA", "populacao_CIA",
    "regiao", "DpGeoDes", "CD_MUN", "favela_2022",
}]
agg = (
    df.loc[mask]
    .groupby([data_key] + [c for c in agg_cols if c != data_key],
             as_index=False, observed=True)["N"].sum()
)

# Taxa per 100k (quando houver população)
if "populacao_CIA" in agg.columns and agg["populacao_CIA"].notna().any():
    agg["taxa_100k"] = (agg["N"] / agg["populacao_CIA"].where(agg["populacao_CIA"] > 0)) * 100_000

gdf, geo_key = geolib.load_layer(f.recorte)
if gdf is None:
    st.warning(f"Camada `{f.recorte}` não encontrada em `data/geo/`.")
else:
    metric = "taxa_100k" if "taxa_100k" in agg.columns else "N"

    # Normaliza chaves dos dois lados antes do merge. Armadilhas comuns aqui:
    #  - Um lado string ("130409"), outro float ("10007.0") → "0.0" sufixo
    #    impede o match. Acontece porque o agregado manteve NaN-aware float.
    #  - Whitespace/case em labels tipo "1ºBPM/M" vs "1º BPM/M".
    # Se os dois lados são numéricos inteiros, converte pra Int64 e depois
    # string (remove o ".0"); senão cai em string + strip + upper.
    def _norm_key(s: pd.Series) -> pd.Series:
        nums = pd.to_numeric(s, errors="coerce")
        valid = nums.dropna()
        if not valid.empty and (valid == valid.astype("int64")).all():
            # Tudo inteiro (possivelmente armazenado como float) → Int64 pra
            # string sem o ".0"
            return nums.astype("Int64").astype("string")
        return s.astype("string").str.strip().str.upper()

    gdf = gdf.copy()
    agg_local = agg.copy()
    gdf[geo_key] = _norm_key(gdf[geo_key])
    agg_local[data_key] = _norm_key(agg_local[data_key])

    merged = gdf.merge(agg_local, left_on=geo_key, right_on=data_key, how="left")
    merged[metric] = merged[metric].fillna(0)

    # Diagnóstico do join: quantos polígonos receberam valor > 0?
    n_total = len(merged)
    n_com_valor = int((merged[metric] > 0).sum())
    if n_com_valor == 0:
        set_geo = set(gdf[geo_key].dropna().astype(str).unique())
        set_agg = set(agg_local[data_key].dropna().astype(str).unique())
        overlap = set_geo & set_agg
        st.warning(
            f"⚠️ Nenhum polígono do recorte **{f.recorte}** recebeu valor > 0 após o join. "
            f"Chave geo=`{geo_key}` ↔ agregado=`{data_key}`. "
            f"Geo tem {len(set_geo):,} chaves únicas, agregado tem {len(set_agg):,}, "
            f"**overlap = {len(overlap):,}**. "
            f"Exemplos geo: {sorted(set_geo)[:3]} · exemplos agregado: {sorted(set_agg)[:3]}"
        )

    merged = merged.reset_index(drop=True)
    merged["_id"] = merged.index.astype(str)

    geojson = _json.loads(merged.set_index("_id").to_json())
    fig = px.choropleth_mapbox(
        merged, geojson=geojson, locations="_id",
        color=metric,
        hover_data={data_key: True, metric: ":,.2f", "_id": False},
        center={"lat": -22.5, "lon": -48.5}, zoom=5.5,
        mapbox_style="carto-positron",
        color_continuous_scale="YlOrRd", opacity=0.75,
    )
    fig.update_layout(height=620, margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        f"Coroplético de **{metric}** · recorte: **{f.recorte}** · "
        f"{n_com_valor:,} de {n_total:,} polígonos com valor > 0"
    )

st.subheader("Tabela do recorte")
st.dataframe(agg.sort_values("N", ascending=False), use_container_width=True, height=380)

st.divider()
st.subheader("Exportar")
download_buttons(
    agg, basename=f"mapa_{f.recorte.lower().replace(' ', '_')}",
    meta={"recorte": f.recorte, "ano_inicio": f.ano_ini, "ano_fim": f.ano_fim,
          "naturezas": ", ".join(f.naturezas) if f.naturezas else "todas"},
)

with st.expander("🔍 Drill-down: pontos individuais de um mês", expanded=False):
    st.caption("Carrega pontos de um mês específico. Amostrado se exceder 50k.")
    c1, c2 = st.columns(2)
    anos = data.anos_disponiveis()
    ano = c1.selectbox("Ano", anos, index=len(anos) - 1 if anos else 0)
    mes = c2.selectbox("Mês", list(range(1, 13)), index=0)
    natureza_one = st.selectbox("Natureza (opcional)",
                                ["(todas)"] + data.naturezas_disponiveis())
    if st.button("Carregar pontos", type="primary"):
        pts = data.pontos(int(ano), int(mes),
                          None if natureza_one == "(todas)" else natureza_one)
        if pts.empty:
            st.info("Nenhum ponto válido nessa partição.")
        else:
            st.map(pts.rename(columns={"LATITUDE": "lat",
                                       "LONGITUDE": "lon"})[["lat", "lon"]])
            st.caption(f"{len(pts):,} pontos plotados.")
