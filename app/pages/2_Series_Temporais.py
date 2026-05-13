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
header("Séries Temporais · SP-Capital",
       "Evolução, sazonalidade, previsão e matrizes temáticas (Cidade de São Paulo)")


# ---------------------------------------------------------------------------
# Helpers reutilizáveis nas matrizes temáticas
# ---------------------------------------------------------------------------
def _render_hora_dia(mhd_sub: pd.DataFrame, chart_key: str) -> pd.DataFrame:
    """Heatmap Dia da Semana × Faixa Hora + tabela de percentuais.

    Retorna pivot_pct (usado externamente para download). Devolve DataFrame
    vazio quando não há dados.
    """
    DIAS = [
        "1.DOMINGO", "2.SEGUNDA-FEIRA", "3.TERÇA-FEIRA",
        "4.QUARTA-FEIRA", "5.QUINTA-FEIRA", "6.SEXTA-FEIRA", "7.SÁBADO",
    ]
    FAIXAS = ["00:00", "00:01–06:00", "06:01–12:00", "12:01–18:00", "18:01–23:59"]
    pivot_cnt = (
        mhd_sub.groupby(["FAIXA_HORA", "DIA_SEMANA"], observed=True)["N"].sum()
        .reset_index()
        .pivot(index="FAIXA_HORA", columns="DIA_SEMANA", values="N")
        .reindex(index=FAIXAS, columns=DIAS).fillna(0).astype(int)
    )
    total = int(pivot_cnt.values.sum())
    if total == 0:
        st.warning("Sem dados para esta natureza após aplicar os filtros.")
        return pd.DataFrame()
    pivot_pct = (pivot_cnt / total * 100.0).round(2)
    total_por_faixa = pivot_pct.sum(axis=1).round(2)
    total_por_dia   = pivot_pct.sum(axis=0).round(2)
    texto_pct = pivot_pct.map(lambda v: f"{v:.2f}%".replace(".", ",")).values
    fig_hm = px.imshow(
        pivot_pct.values,
        x=list(pivot_pct.columns), y=list(pivot_pct.index),
        color_continuous_scale="YlOrRd", aspect="auto",
        labels=dict(x="Dia da semana", y="Faixa de hora", color="% do total"),
    )
    fig_hm.update_traces(
        text=texto_pct, texttemplate="%{text}",
        hovertemplate="Dia: %{x}<br>Faixa: %{y}<br>Participação: %{text}<extra></extra>",
    )
    fig_hm.update_layout(
        height=420, margin=dict(l=10, r=10, t=10, b=10),
        coloraxis_colorbar=dict(title="%"),
    )
    fig_hm.update_xaxes(side="top", tickangle=0)
    st.plotly_chart(fig_hm, use_container_width=True, key=chart_key)
    with st.expander("Tabela de percentuais (linha + coluna Total)", expanded=True):
        tabela = pivot_pct.copy()
        tabela["Total"] = total_por_faixa
        tabela.loc["Total"] = list(total_por_dia) + [round(float(total_por_faixa.sum()), 2)]
        st.dataframe(
            tabela.map(lambda v: f"{v:.2f}%".replace(".", ",")),
            use_container_width=True,
        )
    return pivot_pct


def _render_dia_mes_heatmap(df_sub: pd.DataFrame, chart_key: str) -> None:
    """Heatmap Dia do Mês (1-31) × Faixa Hora + tabela de contagens."""
    FAIXAS = ["00:00", "00:01–06:00", "06:01–12:00", "12:01–18:00", "18:01–23:59"]
    pivot = (
        df_sub.groupby(["DIA_MES", "FAIXA_HORA"], observed=True)["N"].sum()
        .reset_index()
        .pivot(index="DIA_MES", columns="FAIXA_HORA", values="N")
        .reindex(index=range(1, 32), columns=FAIXAS)
        .fillna(0).astype(int)
    )
    total = int(pivot.values.sum())
    if total == 0:
        st.warning("Sem dados para este filtro.")
        return
    fig = px.imshow(
        pivot.values,
        x=FAIXAS,
        y=[str(d) for d in range(1, 32)],
        color_continuous_scale="YlOrRd",
        aspect="auto",
        text_auto=True,
        labels=dict(x="Faixa de hora", y="Dia do mês", color="Ocorrências"),
    )
    fig.update_traces(
        hovertemplate="Dia %{y} · %{x}<br>Ocorrências: %{z}<extra></extra>",
    )
    fig.update_layout(
        height=680,
        margin=dict(l=10, r=10, t=10, b=10),
        coloraxis_colorbar=dict(title="N"),
        yaxis=dict(autorange="reversed", dtick=1),
    )
    fig.update_xaxes(side="top")
    st.plotly_chart(fig, use_container_width=True, key=chart_key)
    with st.expander("Tabela de contagens brutas"):
        pivot.index.name = "Dia"
        st.dataframe(pivot, use_container_width=True)

f = sidebar_filters()
sidebar_footer()

# serie_contextual troca serie_estado por por_dp-filtrado quando uma DP
# está selecionada na sidebar — top 5, STL e previsão passam a ser da DP.
serie = (
    data.serie_contextual_conduta(f.dp_cod, f.condutas)
    if f.condutas else
    data.serie_contextual(f.dp_cod)
)
if serie.empty:
    if f.dp_cod:
        st.info(
            f"Sem dados para **{f.dp_des}**. Tente outra DP ou volte "
            f"pra **Todos os DPs** na sidebar."
        )
    else:
        st.info("Rode `python pipeline/run_all.py` para gerar os agregados.")
    st.stop()

if f.dp_cod:
    st.info(
        f"🏛️ **Escopo ativo:** Delegacia `{f.dp_des}`.",
        icon="🏛️",
    )

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
#    Quando ≥2 naturezas filtradas → uma aba por natureza.
# =========================================================================
st.divider()
st.subheader("Matriz temática — Dia da Semana × Faixa Hora")
st.caption(
    "Heatmap do volume de ocorrências cruzando o **dia da semana** com a "
    "**faixa do dia** (00:01–06:00 · 06:01–12:00 · 12:01–18:00 · 18:01–23:59). "
    "Quando múltiplas naturezas estão selecionadas, cada aba mostra uma natureza. "
    "Use o filtro de *Descrição do Período* (categoria da SSP) para fatiar a matriz."
)

mhd = data.matriz_hora_dia()
if mhd.empty:
    st.info(
        "ℹ️ Agregado `matriz_hora_dia.parquet` não encontrado. Rode "
        "`python pipeline/aggregate_hora_dia.py` depois do `run_all.py` para gerá-lo."
    )
else:
    m_mask = f.mask_date(mhd) & f.mask_natureza(mhd) & f.mask_dp(mhd)
    mhd_f = mhd.loc[m_mask].copy()

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
        # Naturezas presentes no subconjunto filtrado (preserva ordem da seleção).
        nats_mhd = [n for n in (f.naturezas or []) if n in mhd_f["NATUREZA_APURADA"].unique()]

        if len(nats_mhd) >= 2:
            # Uma aba por natureza
            tabs_hd = st.tabs([n[:45] for n in nats_mhd])
            all_pivots: dict[str, pd.DataFrame] = {}
            for tab, nat in zip(tabs_hd, nats_mhd):
                with tab:
                    pv = _render_hora_dia(
                        mhd_f[mhd_f["NATUREZA_APURADA"] == nat],
                        chart_key=f"hd_{nat}",
                    )
                    if not pv.empty:
                        all_pivots[nat] = pv
        else:
            pv = _render_hora_dia(mhd_f, chart_key="hd_all")
            all_pivots = {"total": pv} if not pv.empty else {}

        # Expander com dados brutos por DESC_PERIODO (todas as naturezas)
        with st.expander("Matriz por Descrição do Período (contagens brutas)"):
            _DIAS_ORD = [
                "1.DOMINGO", "2.SEGUNDA-FEIRA", "3.TERÇA-FEIRA",
                "4.QUARTA-FEIRA", "5.QUINTA-FEIRA", "6.SEXTA-FEIRA", "7.SÁBADO",
            ]
            _FAIXAS_ORD = ["00:00", "00:01–06:00", "06:01–12:00", "12:01–18:00", "18:01–23:59"]
            facet = (
                mhd_f.groupby(["DESC_PERIODO", "FAIXA_HORA", "DIA_SEMANA"],
                              observed=True)["N"].sum().reset_index()
            )
            facet["DIA_SEMANA"] = pd.Categorical(facet["DIA_SEMANA"], _DIAS_ORD, ordered=True)
            facet["FAIXA_HORA"] = pd.Categorical(facet["FAIXA_HORA"], _FAIXAS_ORD, ordered=True)
            st.dataframe(
                facet.sort_values(["DESC_PERIODO", "FAIXA_HORA", "DIA_SEMANA"]),
                use_container_width=True,
            )

        if all_pivots:
            download_buttons(
                next(iter(all_pivots.values())).reset_index(),
                basename="matriz_hora_dia_pct",
                meta={
                    "data_inicio": str(f.data_ini), "data_fim": str(f.data_fim),
                    "naturezas": ", ".join(f.naturezas) if f.naturezas else "todas",
                    "periodos_desc": ", ".join(sel_periodos) if sel_periodos else "todos",
                },
            )


# =========================================================================
# 5) Matriz temática — Ocorrências por Dia do Mês  (controle deslizante)
#    Quando ≥2 naturezas filtradas → uma aba por natureza.
# =========================================================================
st.divider()
st.subheader("Matriz temática — Dia do Mês × Faixa Hora")
st.caption(
    "Heatmap cruzando o **dia do mês** (1–31) com a **faixa horária**, "
    "mostrando a contagem bruta de ocorrências em cada célula. "
    "Use o controle deslizante para navegar entre os meses do período filtrado. "
    "Quando múltiplas naturezas estão selecionadas, cada aba mostra uma natureza. "
    "Requer `dia_mes.parquet` — rode `python pipeline/aggregate_dia_mes.py`."
)

dm_raw = data.dia_mes()
if dm_raw.empty:
    st.info(
        "ℹ️ Agregado `dia_mes.parquet` não encontrado. Rode "
        "`python pipeline/aggregate_dia_mes.py` depois do `run_all.py` para gerá-lo."
    )
else:
    dm_mask = f.mask_date(dm_raw) & f.mask_natureza(dm_raw) & f.mask_dp(dm_raw)
    dm_f = dm_raw.loc[dm_mask].copy()

    if dm_f.empty:
        st.warning("Sem dados após aplicar os filtros.")
    else:
        meses_disp = sorted(set(
            zip(dm_f["ANO"].astype(int), dm_f["MES"].astype(int))
        ))
        mes_labels = [f"{m:02d}/{a}" for a, m in meses_disp]

        if len(meses_disp) == 1:
            sel_label = mes_labels[0]
            st.caption(f"Mês: **{sel_label}**")
        else:
            sel_label = st.select_slider(
                "Mês",
                options=mes_labels,
                value=mes_labels[-1],
                key="dm_mes_slider",
            )

        sel_idx   = mes_labels.index(sel_label)
        sel_ano, sel_mes_num = meses_disp[sel_idx]
        dm_mes = dm_f[
            (dm_f["ANO"].astype(int) == sel_ano)
            & (dm_f["MES"].astype(int) == sel_mes_num)
        ]

        nats_dm = [n for n in (f.naturezas or []) if n in dm_mes["NATUREZA_APURADA"].unique()]

        if len(nats_dm) >= 2:
            tabs_dm = st.tabs([n[:45] for n in nats_dm])
            for tab, nat in zip(tabs_dm, nats_dm):
                with tab:
                    _render_dia_mes_heatmap(
                        dm_mes[dm_mes["NATUREZA_APURADA"] == nat],
                        chart_key=f"dm_{nat}_{sel_label}",
                    )
        else:
            _render_dia_mes_heatmap(dm_mes, chart_key=f"dm_all_{sel_label}")
