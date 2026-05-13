"""Gráficos — gauges YoY + evolução mensal + ranking das naturezas mais frequentes.

Pedidos desta rodada (abr/2026):
  • Gauges YoY por indicador selecionado no topo da página — variação %
    do período atual vs. o mesmo intervalo do ano anterior.
  • Ranking de naturezas com barras ordenadas do MAIOR PARA O MENOR e
    legenda de cores ORDENADA POR ANO (sequencial, não categórica).
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
from lib import data
from lib.downloads import download_buttons

apply_brand("Gráficos · InsightGeoLab AI")
header("Gráficos · SP-Capital",
       "Gauges YoY, evolução mensal e ranking das naturezas (Cidade de São Paulo)")

f = sidebar_filters()
sidebar_footer()

# Quando uma DP está selecionada, serie_contextual substitui a série
# estadual por por_dp filtrado — gauges/evolução/ranking passam a refletir
# apenas aquela delegacia. None = estadual (default).
serie = (
    data.serie_contextual_conduta(f.dp_cod, f.condutas)
    if f.condutas else
    data.serie_contextual(f.dp_cod)
)
if serie.empty:
    if f.condutas:
        st.info("Sem dados para a combinação de Conduta + período selecionados.")
    elif f.dp_cod:
        st.info(
            f"Sem dados para a delegacia **{f.dp_des}**. Tente outra DP ou "
            f"volte pra **Todos os DPs** na sidebar."
        )
    else:
        st.info(
            "Ainda não há agregados em `data/aggregates/`. "
            "Rode `python pipeline/run_all.py` depois de colocar os .xlsx em `data/raw/ssp/`."
        )
    st.stop()

if f.dp_cod:
    st.info(
        f"🏛️ **Escopo ativo:** Delegacia `{f.dp_des}` — os gauges, evolução "
        f"mensal e ranking abaixo são restritos a essa DP.",
        icon="🏛️",
    )

mask = f.mask_date(serie) & f.mask_natureza(serie)
serie_f = serie.loc[mask]


# =========================================================================
# 1) Gauges YoY — variação % vs. mesmo intervalo do ano anterior
# =========================================================================
st.subheader("Variação % vs. mesmo período do ano anterior")

d_prev_ini, d_prev_fim = f.prev_year_window()
periodo_atual_label = (
    f"{f.data_ini.strftime('%d/%m/%Y')} → {f.data_fim.strftime('%d/%m/%Y')}"
)
periodo_prev_label = (
    f"{d_prev_ini.strftime('%d/%m/%Y')} → {d_prev_fim.strftime('%d/%m/%Y')}"
)
st.caption(
    f"Período atual: **{periodo_atual_label}** · "
    f"Comparação: **{periodo_prev_label}**"
)


def _sum_range(df: pd.DataFrame, d_ini, d_fim, natureza: str | None = None) -> int:
    """Soma N em [d_ini, d_fim], opcionalmente filtrando por natureza."""
    if df.empty:
        return 0
    if "DATA" in df.columns:
        s = pd.to_datetime(df["DATA"], errors="coerce")
    else:
        s = pd.to_datetime(
            df["ANO"].astype("Int64").astype("string") + "-"
            + df["MES"].astype("Int64").astype("string").str.zfill(2) + "-01",
            errors="coerce",
        )
    m = s.between(pd.Timestamp(d_ini), pd.Timestamp(d_fim))
    if natureza is not None:
        m &= df["NATUREZA_APURADA"] == natureza
    return int(df.loc[m, "N"].sum())


def _gauge(title: str, atual: int, prev: int) -> go.Figure:
    """Indicador plotly: número atual + delta % vs. prev + arco colorido.

    Regras de cor (análise criminal: queda = verde, alta = vermelho):
      • delta ≤ -10%   → verde forte
      • -10% < δ ≤ 0%  → verde claro
      • 0% < δ ≤ +10%  → laranja
      • > +10%         → vermelho
    Quando prev == 0, escondemos o delta (não dá pra calcular %).
    """
    if prev > 0:
        delta_pct = (atual - prev) / prev * 100.0
    else:
        delta_pct = None

    gauge_val = 0.0 if delta_pct is None else max(-200.0, min(200.0, delta_pct))
    if delta_pct is None:
        bar_color = "#9ca3af"
    elif delta_pct <= -10:
        bar_color = "#15803d"
    elif delta_pct <= 0:
        bar_color = "#65a30d"
    elif delta_pct <= 10:
        bar_color = "#f59e0b"
    else:
        bar_color = "#dc2626"

    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta" if delta_pct is not None else "gauge+number",
        value=atual,
        number={"valueformat": ",.0f"},
        delta=(
            {"reference": prev, "relative": True, "valueformat": ".1%",
             "increasing": {"color": "#dc2626"},
             "decreasing": {"color": "#15803d"}}
            if delta_pct is not None else None
        ),
        title={"text": f"<b>{title}</b>", "font": {"size": 13}},
        gauge={
            "axis": {"range": [-50, 50], "tickformat": ".0f",
                     "ticksuffix": "%", "tickfont": {"size": 9}},
            "bar": {"color": bar_color, "thickness": 0.35},
            "bgcolor": "#f3f4f6",
            "borderwidth": 0,
            "steps": [
                {"range": [-50, -10], "color": "#dcfce7"},
                {"range": [-10,   0], "color": "#fef3c7"},
                {"range": [  0,  10], "color": "#fed7aa"},
                {"range": [ 10,  50], "color": "#fee2e2"},
            ],
            "threshold": {
                "line": {"color": "#0C2B4E", "width": 2.5},
                "thickness": 0.9, "value": gauge_val,
            },
        },
    ))
    fig.update_layout(
        height=220, margin=dict(l=10, r=10, t=40, b=10),
        paper_bgcolor="white",
    )
    return fig


if f.naturezas:
    naturezas_ord = sorted(f.naturezas)
    n_cols = min(3, len(naturezas_ord))
    rows = [naturezas_ord[i:i + n_cols] for i in range(0, len(naturezas_ord), n_cols)]
    for row in rows:
        cols = st.columns(n_cols)
        for i, nat in enumerate(row):
            atual = _sum_range(serie, f.data_ini, f.data_fim, natureza=nat)
            prev  = _sum_range(serie, d_prev_ini, d_prev_fim, natureza=nat)
            cols[i].plotly_chart(
                _gauge(nat, atual, prev), use_container_width=True,
                config={"displayModeBar": False},
            )
else:
    atual_tot = _sum_range(serie, f.data_ini, f.data_fim)
    prev_tot  = _sum_range(serie, d_prev_ini, d_prev_fim)
    c = st.columns([1, 2, 1])[1]
    c.plotly_chart(
        _gauge("TODAS AS NATUREZAS", atual_tot, prev_tot),
        use_container_width=True, config={"displayModeBar": False},
    )
    st.caption(
        "💡 Selecione naturezas específicas na aba lateral para ver gauges "
        "individuais por indicador."
    )

st.divider()


# =========================================================================
# 2) Evolução mensal estadual — barras empilhadas por indicador
# =========================================================================
st.subheader("Evolução mensal na Cidade de São Paulo (empilhado por indicador)")
serie_mes = (
    serie_f.groupby(["DATA", "NATUREZA_APURADA"], as_index=False, observed=True)["N"]
    .sum().sort_values(["DATA", "NATUREZA_APURADA"])
)
fig = px.bar(
    serie_mes, x="DATA", y="N",
    color="NATUREZA_APURADA",
    barmode="stack",
    color_discrete_sequence=px.colors.qualitative.Set2,
    labels={"N": "Ocorrências", "DATA": "Mês",
            "NATUREZA_APURADA": "Indicador criminal"},
)
fig.update_layout(
    height=440, margin=dict(l=10, r=10, t=10, b=10),
    xaxis_title=None, yaxis_title="Ocorrências",
    legend=dict(orientation="h", y=-0.18, x=0),
)
st.plotly_chart(fig, use_container_width=True)


# =========================================================================
# 3) Top 20 naturezas — barras horizontais empilhadas, ORDENADAS do MENOR
#    para o MAIOR (pedido do cliente rodada abr/26 #2); legenda SEQUENCIAL
#    por ANO do período.
# =========================================================================
st.subheader("Naturezas mais registradas no período selecionado")
top_totais = (
    serie_f.groupby("NATUREZA_APURADA", as_index=False)["N"].sum()
    .sort_values("N", ascending=False).head(20)  # Top-20 pelos MAIORES volumes
)
top_ano = (
    serie_f[serie_f["NATUREZA_APURADA"].isin(top_totais["NATUREZA_APURADA"])]
    .groupby(["NATUREZA_APURADA", "ANO"], as_index=False, observed=True)["N"].sum()
)

# (a) Ordem do eixo Y: MENOR → MAIOR (invertido desta rodada). Combinado com
# `autorange="reversed"` mais abaixo, isso coloca a MENOR barra no TOPO da
# figura e a MAIOR no FUNDO — forma de pirâmide crescente do olho pro fim.
order_nat = top_totais["NATUREZA_APURADA"][::-1].tolist()  # reverse: menor 1º
top_ano["NATUREZA_APURADA"] = pd.Categorical(
    top_ano["NATUREZA_APURADA"], categories=order_nat, ordered=True,
)

# (b) ANO como categoria ORDENADA crescente — legenda "2022 → 2026" em ordem.
# ANO vira string pra o plotly tratar como categoria discreta, mas a paleta
# é SEQUENCIAL (Viridis) para o olho captar a progressão temporal.
top_ano["ANO"] = top_ano["ANO"].astype("Int64").astype("string")
anos_ord = sorted(top_ano["ANO"].dropna().unique().tolist())
if len(anos_ord) >= 2:
    stops = [i / (len(anos_ord) - 1) for i in range(len(anos_ord))]
    paleta_ano = px.colors.sample_colorscale("Viridis", stops)
else:
    paleta_ano = ["#440154"]
color_map_ano = dict(zip(anos_ord, paleta_ano))

top_ano = top_ano.sort_values(["NATUREZA_APURADA", "ANO"])

fig2 = px.bar(
    top_ano, x="N", y="NATUREZA_APURADA",
    color="ANO",
    category_orders={"ANO": anos_ord, "NATUREZA_APURADA": order_nat},
    color_discrete_map=color_map_ano,
    orientation="h",
    barmode="stack",
    labels={"N": "Ocorrências", "NATUREZA_APURADA": "Indicador",
            "ANO": "Ano (período)"},
)
fig2.update_layout(
    height=540,
    # autorange="reversed" coloca o primeiro item da categoria (maior total)
    # no TOPO do eixo Y.
    yaxis=dict(autorange="reversed"),
    margin=dict(l=10, r=10, t=10, b=10),
    xaxis_title="Ocorrências", yaxis_title=None,
    legend=dict(
        orientation="h", y=-0.12, x=0,
        title=dict(text="<b>Ano (período)</b>"),
        itemsizing="constant", traceorder="normal",
    ),
)
st.plotly_chart(fig2, use_container_width=True)


# =========================================================================
# 4) Export
# =========================================================================
st.divider()
st.subheader("Exportar dados filtrados")
download_buttons(
    serie_f.drop(columns=["DATA"]),
    basename="serie_estadual",
    meta={
        "data_inicio": str(f.data_ini), "data_fim": str(f.data_fim),
        "naturezas": ", ".join(f.naturezas) if f.naturezas else "todas",
        "fonte": "SSP-SP",
    },
)
