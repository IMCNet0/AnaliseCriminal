"""Séries temporais: evolução mensal + decomposição STL + previsão (ARIMA/Prophet)."""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from lib.branding import apply_brand, header
from lib.filters import sidebar_filters
from lib import data, stats
from lib.downloads import download_buttons

apply_brand("Séries Temporais · Portal de Análise Criminal")
header("Evolução, sazonalidade e previsão")

f = sidebar_filters()

serie = data.serie_estado()
if serie.empty:
    st.info("Rode `python pipeline/run_all.py` para gerar os agregados.")
    st.stop()

mask = f.mask_date(serie) & f.mask_natureza(serie)
ts = (
    serie.loc[mask].groupby("DATA")["N"].sum()
    .sort_index()
    .rename("ocorrencias")
)
ts.index.name = "DATA"

c1, c2 = st.columns([3, 2])
with c1:
    fig = go.Figure()
    fig.add_scatter(x=ts.index, y=ts.values, mode="lines", name="Observado")
    fig.update_layout(height=420, margin=dict(l=10, r=10, t=10, b=10), yaxis_title="Ocorrências")
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.markdown("#### Estatísticas da série")
    st.metric("Média mensal", f"{ts.mean():,.0f}".replace(",", "."))
    st.metric("Desvio-padrão", f"{ts.std():,.0f}".replace(",", "."))
    st.metric("Máximo mensal", f"{ts.max():,.0f}".replace(",", "."))
    st.metric("Mínimo mensal", f"{ts.min():,.0f}".replace(",", "."))

st.divider()
st.subheader("Decomposição STL (tendência + sazonalidade + resíduo)")
if len(ts) >= 24:
    try:
        decomp = stats.stl_decompose(ts, period=12, robust=True)
        fig2 = go.Figure()
        for col, color in [("observed", "#1f4e79"), ("trend", "#c8102e"),
                           ("seasonal", "#2ca02c"), ("resid", "#7f7f7f")]:
            fig2.add_scatter(x=decomp.index, y=decomp[col], name=col, mode="lines")
        fig2.update_layout(height=420, margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig2, use_container_width=True)
    except Exception as e:
        st.warning(f"STL falhou: {e}")
else:
    st.info("Pelo menos 24 meses são necessários para decomposição STL (período=12).")

st.divider()
st.subheader("Previsão")
c1, c2, c3 = st.columns(3)
metodo = c1.selectbox("Método", ["Prophet", "SARIMA"])
horizon = c2.slider("Horizonte (meses)", 3, 24, 12)
run = c3.button("Gerar previsão", use_container_width=True)

if run and len(ts) >= 24:
    try:
        if metodo == "Prophet":
            fc = stats.forecast_prophet(ts, horizon=horizon)
        else:
            fc = stats.forecast_arima(ts, horizon=horizon)
        fig3 = go.Figure()
        fig3.add_scatter(x=ts.index, y=ts.values, name="Observado", line=dict(color="#1f4e79"))
        fig3.add_scatter(x=fc.index, y=fc["yhat"], name="Previsão", line=dict(color="#c8102e"))
        fig3.add_scatter(x=fc.index, y=fc["yhat_upper"], name="IC 95% superior",
                         line=dict(dash="dot", color="#c8102e"))
        fig3.add_scatter(x=fc.index, y=fc["yhat_lower"], name="IC 95% inferior",
                         line=dict(dash="dot", color="#c8102e"))
        fig3.update_layout(height=420, margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig3, use_container_width=True)

        download_buttons(fc.reset_index(), basename=f"previsao_{metodo.lower()}",
                         meta={"metodo": metodo, "horizonte": horizon})
    except Exception as e:
        st.error(f"Previsão falhou: {e}")
elif run:
    st.info("Série muito curta para previsão (< 24 meses).")
