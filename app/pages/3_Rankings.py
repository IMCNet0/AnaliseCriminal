"""Rankings e comparativos entre unidades geográficas."""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import streamlit as st
import plotly.express as px

from lib.branding import apply_brand, header
from lib.filters import sidebar_filters
from lib import data
from lib.downloads import download_buttons

apply_brand("Rankings · InsightGeoLab AI")
header("Rankings e comparativos")

f = sidebar_filters()

loader, data_key = data.RECORTE_LOADER[f.recorte]
df = loader()
if df.empty:
    st.info("Agregados ainda não gerados. Rode `python pipeline/run_all.py`.")
    st.stop()

# Rótulo humano quando disponível
label_col = {
    "Município":        "NM_MUN",
    "Comando (CPA)":    "CMDO",
    "Delegacia (DP)":   "DpGeoDes",
    "Batalhão PMESP":   "OPM_BTL",
    # LABEL_CIA = "OPM_CIA / btl_CIA" é criado em por_companhia() — legível
    # pro usuário (ex.: "3ªCIA / 7ºBPM/M") enquanto OPMCOD_CIA fica como chave.
    "Companhia PMESP":  "LABEL_CIA",
    "Setor Censitário": "sc_cod",
}[f.recorte]
if label_col not in df.columns:
    label_col = data_key

mask = f.mask_date(df) & f.mask_natureza(df)
top_n = st.slider("Top N", 5, 50, 15)

ranking = (
    df.loc[mask].groupby([data_key, label_col], as_index=False, observed=True)["N"].sum()
    .sort_values("N", ascending=False).head(top_n)
)

fig = px.bar(
    ranking, x="N", y=label_col, orientation="h", text="N",
    color="N", color_continuous_scale="Blues",
)
fig.update_layout(height=620, yaxis=dict(autorange="reversed"),
                  margin=dict(l=10, r=10, t=10, b=10),
                  xaxis_title="Ocorrências", yaxis_title=None,
                  coloraxis_showscale=False)
fig.update_traces(textposition="outside")
st.plotly_chart(fig, use_container_width=True)

st.subheader("Tabela detalhada")
st.dataframe(ranking, use_container_width=True)

download_buttons(
    ranking, basename=f"ranking_{f.recorte.lower().replace(' ', '_')}",
    meta={"recorte": f.recorte, "top_n": top_n,
          "ano_inicio": f.ano_ini, "ano_fim": f.ano_fim,
          "naturezas": ", ".join(f.naturezas) if f.naturezas else "todas"},
)
