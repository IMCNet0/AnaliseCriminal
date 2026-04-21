"""Home — visão executiva com KPIs e série estadual."""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd
import streamlit as st
import plotly.express as px

from lib.branding import apply_brand, header
from lib import data
from lib.filters import sidebar_filters
from lib.downloads import download_buttons

brand = apply_brand("Home · Portal de Análise Criminal")
header("Visão executiva dos indicadores criminais do estado de SP")

f = sidebar_filters()

serie = data.serie_estado()
if serie.empty:
    st.info(
        "Ainda não há agregados em `data/aggregates/`. "
        "Rode `python pipeline/run_all.py` depois de colocar os .xlsx em `data/raw/ssp/`."
    )
    st.stop()

# Filtros globais
mask = f.mask_date(serie) & f.mask_natureza(serie)
serie_f = serie.loc[mask]

# --- KPIs ---
total_periodo = int(serie_f["N"].sum())
ultimo_ano = int(serie_f["ANO"].max())
total_ultimo_ano = int(serie_f.loc[serie_f["ANO"] == ultimo_ano, "N"].sum())
anos_unicos = sorted(serie_f["ANO"].unique())
if len(anos_unicos) >= 2:
    prev = int(serie_f.loc[serie_f["ANO"] == anos_unicos[-2], "N"].sum())
    delta_yoy = (total_ultimo_ano - prev) / prev * 100 if prev else 0.0
else:
    delta_yoy = 0.0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total no período", f"{total_periodo:,}".replace(",", "."))
k2.metric(f"Total em {ultimo_ano}", f"{total_ultimo_ano:,}".replace(",", "."),
          f"{delta_yoy:+.1f}% vs. ano anterior")
k3.metric("Naturezas incluídas",
          f"{serie_f['NATUREZA_APURADA'].nunique():,}".replace(",", "."))
k4.metric("Meses cobertos", f"{serie_f.groupby(['ANO','MES']).ngroups:,}".replace(",", "."))

st.divider()

# --- Série mensal estadual ---
st.subheader("Evolução mensal no estado de SP")
serie_mes = (
    serie_f.groupby("DATA", as_index=False)["N"].sum().sort_values("DATA")
)
fig = px.line(serie_mes, x="DATA", y="N", markers=False)
fig.update_layout(height=420, margin=dict(l=10, r=10, t=10, b=10),
                  xaxis_title=None, yaxis_title="Ocorrências")
st.plotly_chart(fig, use_container_width=True)

# --- Top naturezas no período ---
st.subheader("Naturezas mais registradas no período selecionado")
top = (
    serie_f.groupby("NATUREZA_APURADA", as_index=False)["N"].sum()
    .sort_values("N", ascending=False).head(20)
)
fig2 = px.bar(top, x="N", y="NATUREZA_APURADA", orientation="h")
fig2.update_layout(height=520, yaxis=dict(autorange="reversed"),
                   margin=dict(l=10, r=10, t=10, b=10),
                   xaxis_title="Ocorrências", yaxis_title=None)
st.plotly_chart(fig2, use_container_width=True)

st.divider()
st.subheader("Exportar dados filtrados")
download_buttons(
    serie_f.drop(columns=["DATA"]),
    basename="serie_estadual",
    meta={
        "ano_inicio": f.ano_ini, "ano_fim": f.ano_fim,
        "naturezas": ", ".join(f.naturezas) if f.naturezas else "todas",
        "fonte": "SSP-SP",
    },
)
