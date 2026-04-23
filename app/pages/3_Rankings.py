"""Rankings e comparativos entre unidades geográficas.

Escopo (abr/2026): apenas dois recortes — **Delegacia (DP)** e
**Setor Censitário**. Os recortes PMESP (Comando/Batalhão/Companhia) e
Município foram retirados da visão desta rodada a pedido do cliente para
simplificar a navegação; continuam disponíveis nos agregados caso sejam
reintroduzidos futuramente.

- Barras EMPILHADAS: cada natureza selecionada vira uma cor distinta dentro
  da barra da unidade — assim o usuário enxerga a composição por indicador
  criminal em um único ranking.
- Setor Censitário é um recorte de ~250k polígonos: o Top-N padrão limita
  a visualização às unidades com maior volume.
"""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import streamlit as st
import plotly.express as px

from lib.branding import apply_brand, header
from lib.filters import sidebar_filters, sidebar_footer
from lib import data
from lib.downloads import download_buttons

apply_brand("Rankings · InsightGeoLab AI")
header("Rankings", "Rankings e comparativos por recorte geográfico")

f = sidebar_filters()
sidebar_footer()

# ---------------------------------------------------------------------------
# Recorte local: apenas DP e Setor Censitário (rodada abr/2026).
# Respeita o recorte da sidebar quando compatível; senão cai no default DP.
# ---------------------------------------------------------------------------
RECORTES_RANKING = ["Delegacia (DP)", "Setor Censitário"]
if f.recorte in RECORTES_RANKING:
    _default_idx = RECORTES_RANKING.index(f.recorte)
else:
    _default_idx = 0

recorte_rank = st.selectbox(
    "Recorte do ranking", RECORTES_RANKING, index=_default_idx,
    help="Apenas Delegacia (DP) e Setor Censitário estão disponíveis nesta visão.",
)

loader, data_key = data.RECORTE_LOADER[recorte_rank]
df = loader()
if df.empty:
    st.info("Agregados ainda não gerados. Rode `python pipeline/run_all.py`.")
    st.stop()


# ---------------------------------------------------------------------------
# Label legível por recorte (DP e Setor Censitário).
# ---------------------------------------------------------------------------
def _build_label(df_: pd.DataFrame, recorte: str) -> pd.Series:
    if recorte == "Delegacia (DP)":
        # DpGeoDes traz o nome (ex.: "1º DP - CENTRO"); DpGeoCod é fallback.
        col = "DpGeoDes" if "DpGeoDes" in df_.columns else "DpGeoCod"
        return df_[col].astype("string").fillna("?")
    if recorte == "Setor Censitário":
        return df_["sc_cod"].astype("string").fillna("?")
    # Fallback: usa a própria chave de dados
    return df_[data_key].astype("string").fillna("?")


df = df.copy()
df["__label__"] = _build_label(df, recorte_rank)

mask = f.mask_date(df) & f.mask_natureza(df)
top_n = st.slider("Top N", 5, 50, 15)


# ---------------------------------------------------------------------------
# Agregação por UNIDADE × NATUREZA (duas dimensões → barras empilhadas)
# ---------------------------------------------------------------------------
ranking = (
    df.loc[mask]
    .groupby([data_key, "__label__", "NATUREZA_APURADA"],
             as_index=False, observed=True)["N"].sum()
)

# Pega Top-N unidades pelo total agregado (somando naturezas) — depois
# restringe o ranking às naturezas dessas unidades.
totals = (
    ranking.groupby([data_key, "__label__"], as_index=False, observed=True)["N"]
    .sum().sort_values("N", ascending=False).head(top_n)
)
keep_keys = set(totals[data_key].tolist())
ranking_top = ranking[ranking[data_key].isin(keep_keys)].copy()
# Mantém a ordem (decrescente por total) no eixo Y.
order = totals["__label__"].tolist()
ranking_top["__label__"] = pd.Categorical(
    ranking_top["__label__"], categories=order, ordered=True,
)
ranking_top = ranking_top.sort_values(["__label__", "NATUREZA_APURADA"])


# ---------------------------------------------------------------------------
# Gráfico: barras HORIZONTAIS EMPILHADAS coloridas por NATUREZA_APURADA.
# ---------------------------------------------------------------------------
fig = px.bar(
    ranking_top,
    x="N", y="__label__",
    color="NATUREZA_APURADA",
    orientation="h",
    barmode="stack",
    color_discrete_sequence=px.colors.qualitative.Set2,
    labels={"__label__": recorte_rank, "N": "Ocorrências",
            "NATUREZA_APURADA": "Indicador criminal"},
)
fig.update_layout(
    height=max(380, 28 * len(order) + 80),
    yaxis=dict(autorange="reversed"),
    margin=dict(l=10, r=10, t=10, b=10),
    xaxis_title="Ocorrências", yaxis_title=None,
    legend=dict(orientation="h", y=-0.15, x=0),
)
st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Tabela detalhada + export
# ---------------------------------------------------------------------------
st.subheader("Tabela detalhada")
# Pivot pra o usuário ler "unidade × natureza" cruzado, quando >1 natureza
if ranking_top["NATUREZA_APURADA"].nunique() > 1:
    pivot = (
        ranking_top.pivot_table(
            index="__label__", columns="NATUREZA_APURADA",
            values="N", aggfunc="sum", fill_value=0, observed=True,
        )
        .reindex(order)
        .assign(Total=lambda d: d.sum(axis=1))
        .sort_values("Total", ascending=False)
    )
    st.dataframe(pivot, use_container_width=True)
else:
    show = ranking_top[["__label__", "NATUREZA_APURADA", "N"]].rename(
        columns={"__label__": recorte_rank}
    )
    st.dataframe(show, use_container_width=True)

download_buttons(
    ranking_top.rename(columns={"__label__": recorte_rank}),
    basename=f"ranking_{recorte_rank.lower().replace(' ', '_').replace('(', '').replace(')', '')}",
    meta={
        "recorte": recorte_rank, "top_n": top_n,
        "data_inicio": str(f.data_ini), "data_fim": str(f.data_fim),
        "naturezas": ", ".join(f.naturezas) if f.naturezas else "todas",
    },
)
