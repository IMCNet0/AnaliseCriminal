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

        n_meses = serie_top["DATA"].nunique()
        if n_meses <= 1:
            # Com 1 único mês, plotly não renderiza linha (precisa de ≥2 pontos
            # no eixo X). Mostramos barras comparativas + aviso sobre ampliar
            # o período. Esse caso é comum agora que o default é "último mês
            # com dado" (rodada abr/2026).
            st.info(
                "📅 O período filtrado contém **apenas 1 mês** — linhas "
                "temporais precisam de pelo menos 2 meses para renderizar. "
                "Mostrando comparativo em barras; amplie o período na "
                "sidebar para ver a evolução como linha."
            )
            fig = px.bar(
                serie_top, x="NATUREZA_APURADA", y="N",
                color="NATUREZA_APURADA",
                color_discrete_sequence=px.colors.qualitative.Set2,
                labels={"NATUREZA_APURADA": "Indicador", "N": "Ocorrências"},
                text="N",
            )
            fig.update_traces(texttemplate="%{text:,}", textposition="outside")
            fig.update_layout(showlegend=False)
        else:
            fig = px.line(
                serie_top, x="DATA", y="N", color="NATUREZA_APURADA",
                color_discrete_sequence=px.colors.qualitative.Set2,
                labels={"DATA": "Mês", "N": "Ocorrências",
                        "NATUREZA_APURADA": "Indicador"},
                markers=True,
            )
            # Garante linhas visíveis mesmo quando uma natureza tem gaps
            # intermitentes. `connectgaps=True` une pontos descontínuos;
            # `mode="lines+markers"` torna explícito o que px.line já deveria
            # fazer por default (bug ocasional com categoricals).
            fig.update_traces(
                mode="lines+markers",
                connectgaps=True,
                line=dict(width=2.5),
                marker=dict(size=7),
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
        # Ordens canônicas (mesmos rótulos gerados pelo pipeline).
        DIAS = [
            "1.DOMINGO", "2.SEGUNDA-FEIRA", "3.TERÇA-FEIRA",
            "4.QUARTA-FEIRA", "5.QUINTA-FEIRA", "6.SEXTA-FEIRA", "7.SÁBADO",
        ]
        FAIXAS = [
            "00:00", "00:01–06:00", "06:01–12:00", "12:01–18:00", "18:01–23:59",
        ]

        # Pivot em contagem bruta (FAIXA em linha, DIA em coluna — espelha o
        # layout pedido pelo cliente, que inverte a matriz anterior).
        pivot_cnt = (
            mhd_f.groupby(["FAIXA_HORA", "DIA_SEMANA"], observed=True)["N"].sum()
            .reset_index()
            .pivot(index="FAIXA_HORA", columns="DIA_SEMANA", values="N")
            .reindex(index=FAIXAS, columns=DIAS)
            .fillna(0).astype(int)
        )

        # Normalização para percentual sobre o GRAND TOTAL da matriz
        # (soma de todas as células) — é o que a imagem de referência traz.
        total = int(pivot_cnt.values.sum())
        if total == 0:
            st.warning("Sem dados na matriz após aplicar os filtros.")
            st.stop()
        pivot_pct = (pivot_cnt / total * 100.0).round(2)

        # Totais marginais (linha e coluna) em %.
        total_por_faixa = pivot_pct.sum(axis=1).round(2)
        total_por_dia   = pivot_pct.sum(axis=0).round(2)

        # Heatmap com valores exibidos como "0,03%".
        # Passamos `text` explicitamente pra formatar com vírgula decimal.
        texto_pct = pivot_pct.map(
            lambda v: f"{v:.2f}%".replace(".", ",")
        ).values
        fig_hm = px.imshow(
            pivot_pct.values,
            x=list(pivot_pct.columns), y=list(pivot_pct.index),
            color_continuous_scale="YlOrRd",
            aspect="auto",
            labels=dict(x="Dia da semana", y="Faixa de hora", color="% do total"),
        )
        fig_hm.update_traces(
            text=texto_pct,
            texttemplate="%{text}",
            hovertemplate=(
                "Dia: %{x}<br>Faixa: %{y}<br>"
                "Participação: %{text}<extra></extra>"
            ),
        )
        fig_hm.update_layout(
            height=420, margin=dict(l=10, r=10, t=10, b=10),
            coloraxis_colorbar=dict(title="%"),
        )
        fig_hm.update_xaxes(side="top", tickangle=0)
        st.plotly_chart(fig_hm, use_container_width=True)

        # Tabela formatada igual ao mock: faixa em linha, dia em coluna,
        # última coluna "Total" por faixa e última linha "Total" por dia.
        with st.expander("Tabela de percentuais (linha + coluna Total)", expanded=True):
            tabela = pivot_pct.copy()
            tabela["Total"] = total_por_faixa
            tabela.loc["Total"] = list(total_por_dia) + [round(total_por_faixa.sum(), 2)]
            # Formata tudo como string "X,XX%"
            tabela_fmt = tabela.map(lambda v: f"{v:.2f}%".replace(".", ","))
            st.dataframe(tabela_fmt, use_container_width=True)

        # Tabela detalhada por DESC_PERIODO (mantida pra análise mais funda).
        with st.expander("Matriz por Descrição do Período (contagens brutas)"):
            facet = (
                mhd_f.groupby(["DESC_PERIODO", "FAIXA_HORA", "DIA_SEMANA"],
                              observed=True)["N"].sum().reset_index()
            )
            facet["DIA_SEMANA"] = pd.Categorical(facet["DIA_SEMANA"], DIAS, ordered=True)
            facet["FAIXA_HORA"] = pd.Categorical(facet["FAIXA_HORA"], FAIXAS, ordered=True)
            facet = facet.sort_values(["DESC_PERIODO", "FAIXA_HORA", "DIA_SEMANA"])
            st.dataframe(facet, use_container_width=True)

        download_buttons(
            pivot_pct.reset_index(),
            basename="matriz_hora_dia_pct",
            meta={
                "data_inicio": str(f.data_ini), "data_fim": str(f.data_fim),
                "naturezas": ", ".join(f.naturezas) if f.naturezas else "todas",
                "periodos_desc": ", ".join(sel_periodos) if sel_periodos else "todos",
                "total_ocorrencias_base": str(total),
            },
        )
