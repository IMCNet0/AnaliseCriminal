"""Séries temporais: evolução mensal (top 5 indicadores) + STL + previsão + matriz hora×dia.

Pedidos desta rodada (abr/2026):
  • Primeiro gráfico: **uma linha para cada um dos 5 maiores indicadores** no
    período filtrado (substitui a linha única "Observado" anterior).
  • Nova matriz temática: **Dia da Semana × Faixa Hora** (Manhã/Tarde/Noite),
    com filtro por Descrição do Período (DESC_PERIODO da SSP). Requer o
    agregado ``matriz_hora_dia.parquet`` — rode
    ``python pipeline/aggregate_hora_dia.py`` depois do run_all.py.
"""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from lib.branding import apply_brand, header
from lib.filters import sidebar_filters, sidebar_footer
from lib import data, stats
from lib.downloads import download_buttons

apply_brand("Séries Temporais · Portal de Análise Criminal")
header("Séries Temporais", "Evolução, sazonalidade, previsão e matriz hora×dia")

f = sidebar_filters()
sidebar_footer()

serie = data.serie_estado()
if serie.empty:
    st.info("Rode `python pipeline/run_all.py` para gerar os agregados.")
    st.stop()

mask = f.mask_date(serie) & f.mask_natureza(serie)
serie_f = serie.loc[mask]


# =========================================================================
# 1) Evolução mensal — UMA LINHA POR INDICADOR (top 5)
# =========================================================================
# Se o usuário filtrou naturezas, usamos até 5 dentre as selecionadas;
# senão, top 5 do próprio período.
if f.naturezas:
    # Rank dentre as escolhidas, limitado a 5.
    rank = (
        serie_f.groupby("NATUREZA_APURADA")["N"].sum()
        .sort_values(ascending=False).head(5)
    )
else:
    rank = (
        serie_f.groupby("NATUREZA_APURADA")["N"].sum()
        .sort_values(ascending=False).head(5)
    )
top5 = rank.index.tolist()

c1, c2 = st.columns([3, 2])

with c1:
    st.subheader("Evolução mensal — top 5 indicadores")
    if not top5:
        st.info("Sem dados no período filtrado.")
        ts_total = pd.Series(dtype="float64")
    else:
        serie_top = (
            serie_f[serie_f["NATUREZA_APURADA"].isin(top5)]
            .groupby(["DATA", "NATUREZA_APURADA"], observed=True)["N"]
            .sum().reset_index().sort_values("DATA")
        )
        # Força a ordem das categorias no eixo/legenda pela posição no rank
        # (1º = maior total) — assim as cores mapeiam coerentemente.
        serie_top["NATUREZA_APURADA"] = pd.Categorical(
            serie_top["NATUREZA_APURADA"], categories=top5, ordered=True,
        )
        fig = px.line(
            serie_top, x="DATA", y="N", color="NATUREZA_APURADA",
            color_discrete_sequence=px.colors.qualitative.Set2,
            labels={"DATA": "Mês", "N": "Ocorrências",
                    "NATUREZA_APURADA": "Indicador"},
            markers=True,
        )
        fig.update_layout(
            height=420, margin=dict(l=10, r=10, t=10, b=10),
            yaxis_title="Ocorrências", xaxis_title=None,
            legend=dict(orientation="h", y=-0.25, x=0,
                        title=dict(text="<b>Indicador</b>")),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Série agregada (soma dos 5) → alimenta STL + previsão abaixo.
        ts_total = (
            serie_top.groupby("DATA")["N"].sum().sort_index().rename("ocorrencias")
        )
        ts_total.index.name = "DATA"

with c2:
    st.markdown("#### Estatísticas (soma dos top 5)")
    ts = ts_total if 'ts_total' in locals() else pd.Series(dtype="float64")
    if ts.empty:
        st.caption("—")
    else:
        st.metric("Média mensal", f"{ts.mean():,.0f}".replace(",", "."))
        st.metric("Desvio-padrão", f"{ts.std():,.0f}".replace(",", "."))
        st.metric("Máximo mensal", f"{ts.max():,.0f}".replace(",", "."))
        st.metric("Mínimo mensal", f"{ts.min():,.0f}".replace(",", "."))


# =========================================================================
# 2) Decomposição STL (soma dos top 5) — 2 casas decimais
# =========================================================================
st.divider()
st.subheader("Decomposição STL (tendência + sazonalidade + resíduo)")
if 'ts' not in locals() or ts.empty:
    st.info("Sem série para decompor.")
elif len(ts) >= 24:
    try:
        decomp = stats.stl_decompose(ts, period=12, robust=True)
        decomp = decomp.round(2)
        fig2 = go.Figure()
        for col, color in [("observed", "#1f4e79"), ("trend", "#c8102e"),
                           ("seasonal", "#2ca02c"), ("resid", "#7f7f7f")]:
            fig2.add_scatter(
                x=decomp.index, y=decomp[col], name=col, mode="lines",
                line=dict(color=color),
                hovertemplate="%{x|%b/%Y}<br>" + col + ": %{y:.2f}<extra></extra>",
            )
        fig2.update_layout(
            height=420, margin=dict(l=10, r=10, t=10, b=10),
            yaxis=dict(tickformat=".2f"),
        )
        st.plotly_chart(fig2, use_container_width=True)
    except Exception as e:
        st.warning(f"STL falhou: {e}")
else:
    st.info("Pelo menos 24 meses são necessários para decomposição STL (período=12).")


# =========================================================================
# 3) Previsão (SARIMA ou Prophet)
# =========================================================================
st.divider()
st.subheader("Previsão")
_metodos = (["Prophet", "SARIMA"] if stats.prophet_available() else ["SARIMA"])
c1, c2, c3 = st.columns(3)
metodo = c1.selectbox("Método", _metodos)
horizon = c2.slider("Horizonte (meses)", 3, 24, 12)
run = c3.button("Gerar previsão", use_container_width=True)
if "Prophet" not in _metodos:
    st.caption("_Prophet indisponível em produção (economia de memória). SARIMA oferece precisão comparável pra série mensal._")

if run and 'ts' in locals() and len(ts) >= 24:
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


# =========================================================================
# 4) Matriz temática — Dia da Semana × Faixa Hora (× Descrição do Período)
# =========================================================================
# Lê o agregado ``matriz_hora_dia.parquet`` gerado por
# ``pipeline/aggregate_hora_dia.py``. Esse agregado é pequeno
# (~dezenas de KB) e cabe confortavelmente no Streamlit Cloud.
st.divider()
st.subheader("Matriz temática — Dia da Semana × Faixa Hora")
st.caption(
    "Heatmap do volume de ocorrências cruzando o **dia da semana** com a "
    "**faixa do dia** (Madrugada 0-5h · Manhã 6-11h · Tarde 12-17h · Noite 18-23h). "
    "Use o filtro de *Descrição do Período* (categoria da SSP) para fatiar a matriz."
)

mhd = data.matriz_hora_dia()
if mhd.empty:
    st.info(
        "ℹ️ Agregado `matriz_hora_dia.parquet` não encontrado. Rode "
        "`python pipeline/aggregate_hora_dia.py` depois do `run_all.py` para gerá-lo."
    )
else:
    # Aplica filtros globais (data, natureza).
    m_mask = f.mask_date(mhd) & f.mask_natureza(mhd)
    mhd_f = mhd.loc[m_mask].copy()

    # Filtro de DESC_PERIODO específico desta matriz (não afeta outras páginas).
    periodos = sorted(mhd_f["DESC_PERIODO"].dropna().astype(str).unique().tolist())
    sel_periodos = st.multiselect(
        "Descrição do Período (opcional — vazio = todos)",
        periodos, default=[],
        help="Classificação da SSP baseada no depoimento da vítima. Quando vazio, "
             "somamos todas as categorias.",
        key="matriz_sel_periodos",
    )
    if sel_periodos:
        mhd_f = mhd_f[mhd_f["DESC_PERIODO"].isin(sel_periodos)]

    if mhd_f.empty:
        st.warning("Sem dados na matriz após aplicar os filtros.")
    else:
        # Ordens canônicas para os eixos.
        DIAS = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
        FAIXAS = ["Madrugada", "Manhã", "Tarde", "Noite"]

        pivot = (
            mhd_f.groupby(["DIA_SEMANA", "FAIXA_HORA"], observed=True)["N"].sum()
            .reset_index()
            .pivot(index="DIA_SEMANA", columns="FAIXA_HORA", values="N")
            .reindex(index=DIAS, columns=FAIXAS)
            .fillna(0).astype(int)
        )

        fig_hm = px.imshow(
            pivot.values,
            x=list(pivot.columns), y=list(pivot.index),
            color_continuous_scale="YlOrRd",
            aspect="auto", text_auto=",d",
            labels=dict(x="Faixa do dia", y="Dia da semana", color="Ocorrências"),
        )
        fig_hm.update_layout(
            height=420, margin=dict(l=10, r=10, t=10, b=10),
            coloraxis_colorbar=dict(title="Ocorr."),
        )
        fig_hm.update_xaxes(side="top")
        st.plotly_chart(fig_hm, use_container_width=True)

        # Tabela detalhada por DESC_PERIODO (facetada).
        with st.expander("Matriz por Descrição do Período (tabela detalhada)"):
            facet = (
                mhd_f.groupby(["DESC_PERIODO", "DIA_SEMANA", "FAIXA_HORA"],
                              observed=True)["N"].sum().reset_index()
            )
            facet["DIA_SEMANA"] = pd.Categorical(facet["DIA_SEMANA"], DIAS, ordered=True)
            facet["FAIXA_HORA"] = pd.Categorical(facet["FAIXA_HORA"], FAIXAS, ordered=True)
            facet = facet.sort_values(["DESC_PERIODO", "DIA_SEMANA", "FAIXA_HORA"])
            st.dataframe(facet, use_container_width=True)

        download_buttons(
            pivot.reset_index(),
            basename="matriz_hora_dia",
            meta={
                "data_inicio": str(f.data_ini), "data_fim": str(f.data_fim),
                "naturezas": ", ".join(f.naturezas) if f.naturezas else "todas",
                "periodos_desc": ", ".join(sel_periodos) if sel_periodos else "todos",
            },
        )
