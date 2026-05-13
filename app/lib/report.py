"""Geração de relatório completo — local only.

Coleta todos os indicadores com os filtros ativos, monta figuras Plotly e
renderiza um HTML autocontido para download. Insights gerados via API
Anthropic (claude-sonnet-4-6) se ANTHROPIC_API_KEY estiver disponível;
caso contrário, texto analítico baseado em regras.
"""
from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

from .filters import GlobalFilters
from . import data as _data


def _get_api_key(name: str) -> str:
    """Lê chave de API do st.secrets (local/Cloud) ou variável de ambiente.

    Prioridade: st.secrets > os.environ. Retorna string vazia se ausente.
    """
    try:
        import streamlit as st
        val = st.secrets.get(name, "")
        if val and val != "COLE_SUA_CHAVE_AQUI":
            return str(val)
    except Exception:
        pass
    return os.environ.get(name, "")


# ---------------------------------------------------------------------------
# Coleta de dados
# ---------------------------------------------------------------------------

def gather(f: GlobalFilters) -> dict:
    """Coleta todos os dados necessários para o relatório com os filtros de f."""
    serie = (
        _data.serie_contextual_conduta(f.dp_cod, f.condutas)
        if f.condutas else
        _data.serie_contextual(f.dp_cod)
    )
    if serie.empty:
        serie_f = pd.DataFrame()
    else:
        mask = f.mask_date(serie) & f.mask_natureza(serie)
        serie_f = serie.loc[mask].copy()

    total_periodo = int(serie_f["N"].sum()) if not serie_f.empty else 0
    ultimo_ano = int(serie_f["ANO"].max()) if not serie_f.empty else f.data_fim.year
    total_ultimo_ano = int(serie_f.loc[serie_f["ANO"] == ultimo_ano, "N"].sum()) if not serie_f.empty else 0
    anos_unicos = sorted(serie_f["ANO"].unique().tolist()) if not serie_f.empty else []

    # delta_yoy calculado após yoy_df (usa prev_year_window — mesmo período ano anterior)
    delta_yoy = 0.0
    total_periodo_anterior = 0

    # Ranking de naturezas
    if not serie_f.empty and "NATUREZA_APURADA" in serie_f.columns:
        nat_counts = (
            serie_f.groupby("NATUREZA_APURADA", observed=True)["N"]
            .sum().sort_values(ascending=False).reset_index()
        )
    else:
        nat_counts = pd.DataFrame(columns=["NATUREZA_APURADA", "N"])

    # Evolução mensal — top 5 naturezas
    top5 = nat_counts.head(5)["NATUREZA_APURADA"].tolist() if not nat_counts.empty else []
    if not serie_f.empty and "DATA" in serie_f.columns and top5:
        mensal = (
            serie_f[serie_f["NATUREZA_APURADA"].isin(top5)]
            .groupby(["DATA", "NATUREZA_APURADA"], observed=True)["N"]
            .sum().reset_index().sort_values("DATA")
        )
    else:
        mensal = pd.DataFrame()

    # Comparação YoY por natureza (top 10)
    d_prev_ini, d_prev_fim = f.prev_year_window()
    if not serie.empty and "DATA" in serie.columns:
        mask_curr = f.mask_date(serie) & f.mask_natureza(serie)
        mask_prev = (
            pd.to_datetime(serie["DATA"]).between(
                pd.Timestamp(d_prev_ini), pd.Timestamp(d_prev_fim)
            ) & f.mask_natureza(serie)
        )
        tot_curr = serie.loc[mask_curr].groupby("NATUREZA_APURADA", observed=True)["N"].sum()
        tot_prev = serie.loc[mask_prev].groupby("NATUREZA_APURADA", observed=True)["N"].sum()
        yoy_df = pd.DataFrame({"atual": tot_curr, "anterior": tot_prev}).fillna(0)
        yoy_df["variacao_pct"] = (
            (yoy_df["atual"] - yoy_df["anterior"])
            / yoy_df["anterior"].replace(0, pd.NA) * 100
        )
        yoy_df = yoy_df.reset_index().sort_values("atual", ascending=False).head(10)
        # delta_yoy correto: período atual vs mesmo período no ano anterior
        curr_sum = int(tot_curr.sum())
        prev_sum = int(tot_prev.sum())
        delta_yoy = (curr_sum - prev_sum) / prev_sum * 100 if prev_sum > 0 else 0.0
        total_periodo_anterior = prev_sum
    else:
        yoy_df = pd.DataFrame()

    # Ranking por DP
    dp_df = _data.por_dp()
    if not dp_df.empty:
        dp_mask = f.mask_date(dp_df) & f.mask_natureza(dp_df)
        dp_f = dp_df.loc[dp_mask].copy()
    else:
        dp_f = pd.DataFrame()

    if not dp_f.empty and "DpGeoDes" in dp_f.columns:
        grp_cols = (["DpGeoCod", "DpGeoDes"] if "DpGeoCod" in dp_f.columns
                    else ["DpGeoDes"])
        dp_rank = (
            dp_f.groupby(grp_cols, observed=True)["N"]
            .sum().sort_values(ascending=False).head(15).reset_index()
        )
    else:
        dp_rank = pd.DataFrame()

    # Todos os DPs (para o mapa coroplético — sem limite de top 15)
    if not dp_f.empty and "DpGeoCod" in dp_f.columns:
        dp_all = (
            dp_f.groupby("DpGeoCod", observed=True)["N"]
            .sum().reset_index()
        )
        dp_all["DpGeoCod"] = _data._norm_dp_cod(dp_all["DpGeoCod"])
    else:
        dp_all = pd.DataFrame()

    # Série mensal total (para STL simplificado)
    serie_total = pd.DataFrame()
    if not serie_f.empty and "DATA" in serie_f.columns:
        serie_total = (
            serie_f.groupby("DATA")["N"].sum().reset_index().sort_values("DATA")
        )

    # Matriz hora × dia
    mhd_raw = _data.matriz_hora_dia()
    if not mhd_raw.empty:
        mhd_mask = f.mask_date(mhd_raw) & f.mask_natureza(mhd_raw)
        if f.dp_cod:
            mhd_mask &= f.mask_dp(mhd_raw)
        mhd_f = mhd_raw.loc[mhd_mask].copy()
    else:
        mhd_f = pd.DataFrame()

    # Matriz dia do mês × faixa hora
    dm_raw = _data.dia_mes()
    if not dm_raw.empty:
        dm_mask = f.mask_date(dm_raw) & f.mask_natureza(dm_raw)
        if f.dp_cod:
            dm_mask &= f.mask_dp(dm_raw)
        dia_mes_f = dm_raw.loc[dm_mask].copy()
    else:
        dia_mes_f = pd.DataFrame()

    # Evolução anual (totais por ano)
    evol_anual = pd.DataFrame()
    if not serie_f.empty:
        evol_anual = (
            serie_f.groupby(["ANO", "NATUREZA_APURADA"], observed=True)["N"]
            .sum().reset_index()
        )

    # Centro/zoom do mapa para o DP selecionado (fallback: SP-Capital)
    dp_map_center = _dp_map_center(f.dp_cod)

    # Pontos para mapas scatter/heatmap
    pontos_f = _gather_pontos(f)

    return {
        "filtros": f,
        "serie_f": serie_f,
        "serie_total": serie_total,
        "total_periodo": total_periodo,
        "total_periodo_anterior": total_periodo_anterior,
        "total_ultimo_ano": total_ultimo_ano,
        "ultimo_ano": ultimo_ano,
        "delta_yoy": delta_yoy,
        "anos_unicos": anos_unicos,
        "nat_counts": nat_counts,
        "mensal": mensal,
        "yoy_df": yoy_df,
        "dp_rank": dp_rank,
        "dp_all": dp_all,
        "pontos_f": pontos_f,
        "dp_map_center": dp_map_center,
        "mhd_f": mhd_f,
        "dia_mes_f": dia_mes_f,
        "evol_anual": evol_anual,
        "d_prev_ini": d_prev_ini,
        "d_prev_fim": d_prev_fim,
    }


# ---------------------------------------------------------------------------
# Figuras
# ---------------------------------------------------------------------------

_GEO_PATH = (
    __import__("pathlib").Path(__file__).resolve().parents[2]
    / "data" / "geo" / "DP.json"
)


def _dp_map_center(dp_cod: Optional[str]) -> tuple:
    """Retorna (lat, lon, zoom) para o DP selecionado.

    Lê DP.json, extrai o polígono da DP e calcula centróide + zoom a partir
    do bounding box. Fallback: SP-Capital centro/zoom padrão.
    """
    if not dp_cod or not _GEO_PATH.exists():
        return (-23.5505, -46.6333, 11)

    import json
    with open(_GEO_PATH, encoding="utf-8") as fh:
        geojson = json.load(fh)

    target = None
    for feat in geojson.get("features", []):
        raw = feat.get("properties", {}).get("DpGeoCod", "")
        try:
            norm = str(int(float(raw)))
        except (ValueError, TypeError):
            norm = str(raw).strip()
        if norm == str(dp_cod).strip():
            target = feat
            break

    if target is None:
        return (-23.5505, -46.6333, 11)

    coords_list: list = []

    def _extract(coords):
        if not coords:
            return
        if isinstance(coords[0], (int, float)):
            coords_list.append(coords)
        else:
            for c in coords:
                _extract(c)

    geom = target.get("geometry", {})
    geom_type = geom.get("type", "")
    if geom_type == "Polygon":
        for ring in geom.get("coordinates", []):
            _extract(ring)
    elif geom_type == "MultiPolygon":
        for poly in geom.get("coordinates", []):
            for ring in poly:
                _extract(ring)

    if not coords_list:
        return (-23.5505, -46.6333, 11)

    lons = [c[0] for c in coords_list]
    lats = [c[1] for c in coords_list]
    lat_c = (min(lats) + max(lats)) / 2
    lon_c = (min(lons) + max(lons)) / 2
    span = max(max(lats) - min(lats), max(lons) - min(lons))
    if span < 0.03:
        zoom = 14
    elif span < 0.07:
        zoom = 13
    elif span < 0.15:
        zoom = 12
    else:
        zoom = 11
    return (lat_c, lon_c, zoom)


def _gather_pontos(f: "GlobalFilters", max_total: int = 12_000) -> pd.DataFrame:
    """Coleta pontos de ocorrência para o período e filtros de f.

    Itera partições ano/mês, aplica filtros de natureza e DP em Python,
    e limita ao total de max_total pontos (amostragem reproducível).
    """
    ini = f.data_ini
    fim = f.data_fim

    year_months: list = []
    y, m = ini.year, ini.month
    while (y, m) <= (fim.year, fim.month):
        year_months.append((y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1

    frames = []
    per_month = max(500, max_total // max(len(year_months), 1))
    for ano, mes in year_months:
        try:
            df_pt = _data.pontos(ano, mes, natureza=None, dp_cod=f.dp_cod,
                                 max_rows=per_month)
            if not df_pt.empty:
                frames.append(df_pt)
        except Exception:
            pass

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    if f.naturezas and "NATUREZA_APURADA" in combined.columns:
        combined = combined[combined["NATUREZA_APURADA"].isin(f.naturezas)].copy()
    if len(combined) > max_total:
        combined = combined.sample(max_total, random_state=42)
    return combined


def _build_choro_figure(d: dict) -> Optional[go.Figure]:
    """Mapa coroplético de ocorrências por DP usando DP.json + open-street-map."""
    import json

    dp_all: pd.DataFrame = d.get("dp_all", pd.DataFrame())
    if dp_all.empty or not _GEO_PATH.exists():
        return None

    with open(_GEO_PATH, encoding="utf-8") as fh:
        geojson = json.load(fh)

    # Normaliza DpGeoCod no GeoJSON para inteiro-como-string ("10102")
    for feat in geojson.get("features", []):
        raw = feat.get("properties", {}).get("DpGeoCod", "")
        try:
            feat["properties"]["DpGeoCod"] = str(int(float(raw)))
        except (ValueError, TypeError):
            feat["properties"]["DpGeoCod"] = str(raw).strip()

    center_lat, center_lon, zoom = d.get("dp_map_center", (-23.5505, -46.6333, 11))
    fig = px.choropleth_mapbox(
        dp_all,
        geojson=geojson,
        locations="DpGeoCod",
        color="N",
        featureidkey="properties.DpGeoCod",
        mapbox_style="open-street-map",
        zoom=zoom,
        center={"lat": center_lat, "lon": center_lon},
        color_continuous_scale="YlOrRd",
        opacity=0.7,
        title="Distribuição Geográfica de Ocorrências por Delegacia (DP)",
        labels={"N": "Ocorrências", "DpGeoCod": "Cód. DP"},
    )
    fig.update_layout(margin=dict(l=0, r=0, t=50, b=0), height=520)
    return fig


def _build_pontos_figure(d: dict) -> Optional[go.Figure]:
    """Mapa de pontos dispersos de ocorrência."""
    pts: pd.DataFrame = d.get("pontos_f", pd.DataFrame())
    if pts.empty or not {"LATITUDE", "LONGITUDE"}.issubset(pts.columns):
        return None

    center_lat, center_lon, zoom = d.get("dp_map_center", (-23.5505, -46.6333, 11))
    color_col = "NATUREZA_APURADA" if "NATUREZA_APURADA" in pts.columns else None
    hover: dict = {}
    if "NATUREZA_APURADA" in pts.columns:
        hover["NATUREZA_APURADA"] = True
    if "DATA_OCORRENCIA_BO" in pts.columns:
        hover["DATA_OCORRENCIA_BO"] = True

    total_str = f"{len(pts):,}".replace(",", ".")
    fig = px.scatter_mapbox(
        pts,
        lat="LATITUDE", lon="LONGITUDE",
        color=color_col,
        hover_data=hover or None,
        mapbox_style="open-street-map",
        zoom=zoom,
        center={"lat": center_lat, "lon": center_lon},
        opacity=0.55,
        title=f"Pontos de Ocorrência ({total_str} registros)",
        labels={"NATUREZA_APURADA": "Natureza"},
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_traces(marker_size=5)
    fig.update_layout(
        margin=dict(l=0, r=0, t=50, b=0),
        height=520,
        legend=dict(orientation="h", yanchor="bottom", y=-0.15),
    )
    return fig


def _build_calor_figure(d: dict) -> Optional[go.Figure]:
    """Mapa de calor (densidade) de ocorrências."""
    pts: pd.DataFrame = d.get("pontos_f", pd.DataFrame())
    if pts.empty or not {"LATITUDE", "LONGITUDE"}.issubset(pts.columns):
        return None

    center_lat, center_lon, zoom = d.get("dp_map_center", (-23.5505, -46.6333, 11))
    total_str = f"{len(pts):,}".replace(",", ".")
    fig = px.density_mapbox(
        pts,
        lat="LATITUDE", lon="LONGITUDE",
        radius=12,
        mapbox_style="open-street-map",
        zoom=zoom,
        center={"lat": center_lat, "lon": center_lon},
        color_continuous_scale="YlOrRd",
        opacity=0.75,
        title=f"Mapa de Calor — Densidade de Ocorrências ({total_str} registros)",
    )
    fig.update_layout(margin=dict(l=0, r=0, t=50, b=0), height=520)
    return fig


def build_figures(d: dict) -> dict:
    """Constrói todas as figuras Plotly para o relatório."""
    figs: dict[str, go.Figure] = {}
    _PALETTE = px.colors.qualitative.Set2

    # 1) Evolução mensal — top 5 naturezas
    if not d["mensal"].empty:
        fig = px.line(
            d["mensal"], x="DATA", y="N", color="NATUREZA_APURADA",
            title="Evolução Mensal — Top 5 Naturezas",
            labels={"N": "Ocorrências", "DATA": "Mês", "NATUREZA_APURADA": "Natureza"},
            color_discrete_sequence=_PALETTE,
        )
        fig.update_traces(mode="lines+markers", marker_size=4)
        fig.update_layout(
            legend=dict(orientation="h", yanchor="bottom", y=-0.35, x=0),
            margin=dict(t=50, b=80),
        )
        figs["evolucao_mensal"] = fig

    # 2) Top 20 naturezas — barras horizontais
    if not d["nat_counts"].empty:
        top20 = d["nat_counts"].head(20).copy()
        top20_rev = top20.iloc[::-1]
        fig = px.bar(
            top20_rev, x="N", y="NATUREZA_APURADA", orientation="h",
            title="Ranking — Top 20 Naturezas no Período",
            labels={"N": "Ocorrências", "NATUREZA_APURADA": "Natureza"},
            color="N", color_continuous_scale="YlOrRd",
        )
        fig.update_layout(height=520, showlegend=False, margin=dict(t=50, l=200))
        figs["top_naturezas"] = fig

    # 3) Comparação YoY — barras agrupadas
    if not d["yoy_df"].empty:
        f_obj = d["filtros"]
        fig = go.Figure()
        fig.add_trace(go.Bar(
            name=f"Período atual",
            y=d["yoy_df"]["NATUREZA_APURADA"],
            x=d["yoy_df"]["atual"],
            orientation="h",
            marker_color="#1565C0",
            text=d["yoy_df"]["atual"].astype(int).astype(str),
            textposition="outside",
        ))
        fig.add_trace(go.Bar(
            name=f"Mesmo período ano anterior",
            y=d["yoy_df"]["NATUREZA_APURADA"],
            x=d["yoy_df"]["anterior"],
            orientation="h",
            marker_color="#90CAF9",
            text=d["yoy_df"]["anterior"].astype(int).astype(str),
            textposition="outside",
        ))
        fig.update_layout(
            barmode="group",
            title="Comparação com o Mesmo Período do Ano Anterior — Top 10 Naturezas",
            height=520,
            margin=dict(t=50, l=200),
            legend=dict(orientation="h", yanchor="bottom", y=-0.15),
        )
        figs["yoy"] = fig

    # 4) Ranking por DP
    if not d["dp_rank"].empty:
        rev = d["dp_rank"].iloc[::-1]
        fig = px.bar(
            rev, x="N", y="DpGeoDes", orientation="h",
            title="Ranking por Delegacia (DP) — Top 15 no Período",
            labels={"N": "Ocorrências", "DpGeoDes": "Delegacia"},
            color="N", color_continuous_scale="Blues",
        )
        fig.update_layout(height=480, showlegend=False, margin=dict(t=50, l=220))
        figs["ranking_dp"] = fig

    # 5) Evolução anual por natureza (área empilhada)
    if not d["evol_anual"].empty and len(d["anos_unicos"]) > 1:
        top5_nat = d["nat_counts"].head(5)["NATUREZA_APURADA"].tolist() if not d["nat_counts"].empty else []
        ev_top = d["evol_anual"][d["evol_anual"]["NATUREZA_APURADA"].isin(top5_nat)] if top5_nat else d["evol_anual"]
        if not ev_top.empty:
            fig = px.bar(
                ev_top, x="ANO", y="N", color="NATUREZA_APURADA",
                title="Total Anual por Natureza",
                labels={"N": "Ocorrências", "ANO": "Ano", "NATUREZA_APURADA": "Natureza"},
                color_discrete_sequence=_PALETTE,
                barmode="stack",
            )
            fig.update_layout(
                legend=dict(orientation="h", yanchor="bottom", y=-0.35),
                margin=dict(t=50, b=100),
            )
            figs["evol_anual"] = fig

    # 6) Série mensal total (tendência)
    if not d["serie_total"].empty:
        fig = px.area(
            d["serie_total"], x="DATA", y="N",
            title="Tendência Mensal — Total de Ocorrências",
            labels={"N": "Ocorrências", "DATA": "Mês"},
            color_discrete_sequence=["#0C2B4E"],
        )
        fig.update_traces(fill="tozeroy", fillcolor="rgba(12,43,78,0.15)")
        fig.update_layout(margin=dict(t=50))
        figs["serie_total"] = fig

    # 7) Mapa coroplético por DP
    _choro = _build_choro_figure(d)
    if _choro is not None:
        figs["mapa_dp"] = _choro

    # 8) Mapa de pontos
    _pontos_fig = _build_pontos_figure(d)
    if _pontos_fig is not None:
        figs["mapa_pontos"] = _pontos_fig

    # 9) Mapa de calor
    _calor_fig = _build_calor_figure(d)
    if _calor_fig is not None:
        figs["mapa_calor"] = _calor_fig

    # 10) Matriz hora × dia
    if not d["mhd_f"].empty and {"FAIXA_HORA", "DIA_SEMANA", "N"}.issubset(d["mhd_f"].columns):
        pivot = (
            d["mhd_f"].groupby(["DIA_SEMANA", "FAIXA_HORA"], observed=True)["N"]
            .sum().unstack(fill_value=0)
        )
        dias_ord = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
        faixas_ord = ["Madrugada", "Manhã", "Tarde", "Noite"]
        dias = [x for x in dias_ord if x in pivot.index]
        faixas = [x for x in faixas_ord if x in pivot.columns]
        if dias and faixas:
            pivot = pivot.reindex(index=dias, columns=faixas, fill_value=0)
            fig = px.imshow(
                pivot, text_auto=",d",
                color_continuous_scale="YlOrRd",
                title="Distribuição por Dia da Semana e Faixa Horária",
                labels={"x": "Faixa Horária", "y": "Dia da Semana", "color": "Ocorrências"},
            )
            fig.update_layout(height=320, margin=dict(t=50))
            figs["matriz"] = fig

    return figs


# ---------------------------------------------------------------------------
# Insights
# ---------------------------------------------------------------------------

def generate_insights(d: dict) -> str:
    """Gera insights: Claude → Gemini Flash → regras analíticas."""
    try:
        return _insights_claude(d)
    except Exception:
        pass
    try:
        return _insights_gemini(d)
    except Exception:
        pass
    return _insights_rules(d)


def _insights_rules(d: dict) -> str:
    f = d["filtros"]
    total = d["total_periodo"]
    delta = d["delta_yoy"]
    escopo = f"Delegacia **{f.dp_des}**" if f.dp_cod else "**SP-Capital (toda a cidade)**"
    nat_counts = d["nat_counts"]
    dp_rank = d["dp_rank"]
    mhd_f = d["mhd_f"]

    blocos = []

    # Parágrafo 1 — contexto e volume
    p1 = (
        f"O presente relatório analisa os registros criminais de {escopo} "
        f"no período de **{f.data_ini.strftime('%d/%m/%Y')}** a "
        f"**{f.data_fim.strftime('%d/%m/%Y')}**. "
        f"Foram registradas **{total:,} ocorrências** no intervalo analisado."
        .replace(",", ".")
    )
    if f.naturezas:
        p1 += f" A análise está filtrada para: {', '.join(f.naturezas[:5])}{'...' if len(f.naturezas) > 5 else ''}."
    blocos.append(p1)

    # Parágrafo 2 — tendência YoY
    total_anterior = d.get("total_periodo_anterior", 0)
    d_prev_ini = d.get("d_prev_ini")
    d_prev_fim = d.get("d_prev_fim")
    if total_anterior > 0:
        dir_str = "elevação" if delta > 0 else "redução"
        sinal_emoji = "⚠️" if delta > 10 else ("✅" if delta < -5 else "➡️")
        periodo_anterior_label = (
            f"{d_prev_ini.strftime('%d/%m/%Y')} a {d_prev_fim.strftime('%d/%m/%Y')}"
            if d_prev_ini and d_prev_fim else "mesmo período do ano anterior"
        )
        p2 = (
            f"{sinal_emoji} No período analisado foram registradas **{total:,}** ocorrências, "
            f"representando uma {dir_str} de **{abs(delta):.1f}%** em relação ao período "
            f"equivalente anterior ({periodo_anterior_label}, **{total_anterior:,}** ocorrências)."
            .replace(",", ".")
        )
        if delta > 15:
            p2 += " Variação expressiva que merece atenção operacional prioritária."
        elif delta > 5:
            p2 += " Variação moderada de alta que requer monitoramento contínuo."
        elif delta < -10:
            p2 += " Variação favorável, indicando possível resultado positivo das ações preventivas."
        elif delta < -3:
            p2 += " Leve redução, tendência a ser confirmada nos próximos meses."
        blocos.append(p2)

    # Parágrafo 3 — naturezas predominantes
    if not nat_counts.empty:
        top3 = nat_counts.head(3)
        linhas_nat = []
        for _, row in top3.iterrows():
            pct = row["N"] / total * 100 if total else 0
            linhas_nat.append(
                f"**{row['NATUREZA_APURADA']}** ({int(row['N']):,} · {pct:.1f}%)".replace(",", ".")
            )
        p3 = (
            "As naturezas criminais mais frequentes no período foram: "
            + ", ".join(linhas_nat) + ". "
        )
        if not nat_counts.empty:
            top1_pct = nat_counts.iloc[0]["N"] / total * 100 if total else 0
            if top1_pct > 40:
                p3 += (
                    f"A natureza **{nat_counts.iloc[0]['NATUREZA_APURADA']}** "
                    f"concentra {top1_pct:.0f}% do volume total, "
                    f"indicando forte dominância deste tipo criminal no recorte."
                )
        blocos.append(p3)

    # Parágrafo 4 — ranking de DPs
    if not dp_rank.empty and not f.dp_cod:
        top3_dp = dp_rank.head(3)
        linhas_dp = [
            f"**{r['DpGeoDes']}** ({int(r['N']):,})".replace(",", ".")
            for _, r in top3_dp.iterrows()
        ]
        total_dp_top3 = int(top3_dp["N"].sum())
        pct_top3 = total_dp_top3 / total * 100 if total else 0
        p4 = (
            f"As delegacias com maior volume de ocorrências foram: {', '.join(linhas_dp)}. "
            f"As três primeiras concentram **{pct_top3:.1f}%** do total registrado no período."
        )
        blocos.append(p4)

    # Parágrafo 5 — padrão temporal (hora × dia)
    if not mhd_f.empty and {"FAIXA_HORA", "DIA_SEMANA", "N"}.issubset(mhd_f.columns):
        faixa_tot = mhd_f.groupby("FAIXA_HORA", observed=True)["N"].sum()
        dia_tot = mhd_f.groupby("DIA_SEMANA", observed=True)["N"].sum()
        pico_faixa = faixa_tot.idxmax()
        pico_dia = dia_tot.idxmax()
        pico_dia_label = (
            str(pico_dia).split(".", 1)[-1].replace("-", " ").title()
            if "." in str(pico_dia) else str(pico_dia)
        )
        pct_faixa = faixa_tot[pico_faixa] / faixa_tot.sum() * 100 if faixa_tot.sum() > 0 else 0
        # Segunda faixa mais crítica
        faixa_ord = faixa_tot.sort_values(ascending=False)
        segunda_faixa = faixa_ord.index[1] if len(faixa_ord) > 1 else ""
        pct_segunda = faixa_ord.iloc[1] / faixa_tot.sum() * 100 if len(faixa_ord) > 1 else 0
        p5 = (
            f"A análise temporal revela maior concentração de ocorrências na faixa "
            f"**{pico_faixa}** ({pct_faixa:.1f}% dos registros)"
        )
        if segunda_faixa:
            p5 += f", seguida por **{segunda_faixa}** ({pct_segunda:.1f}%)"
        p5 += (
            f", com maior incidência às **{pico_dia_label}s**. "
            "Esses padrões podem subsidiar o planejamento de rondas e reforço de efetivo "
            "nos horários e dias de maior incidência."
        )
        blocos.append(p5)

    # Parágrafo 5b — pico por dia do mês
    dia_mes_f = d.get("dia_mes_f")
    if dia_mes_f is not None and not dia_mes_f.empty and "DIA_MES" in dia_mes_f.columns:
        dm_tot = dia_mes_f.groupby("DIA_MES")["N"].sum()
        if not dm_tot.empty:
            pico_dm = int(dm_tot.idxmax())
            pct_dm = dm_tot[pico_dm] / dm_tot.sum() * 100 if dm_tot.sum() > 0 else 0
            p5b = (
                f"Em relação à distribuição por dia do mês, o dia **{pico_dm}** apresenta "
                f"o maior volume (**{int(dm_tot[pico_dm]):,}** ocorrências, {pct_dm:.1f}% do total). "
                "Esse dado pode orientar o planejamento mensal de reforço operacional."
            )
            blocos.append(p5b)

    # Parágrafo 6 — observação de uso
    blocos.append(
        "_Nota: este relatório é gerado automaticamente com base nos registros da SSP-SP "
        "disponíveis na base de dados do portal. Os dados podem estar sujeitos a "
        "atualizações e reprocessamentos pela fonte oficial._"
    )

    return "\n\n".join(blocos)


def _build_insights_prompt(d: dict) -> str:
    """Monta o prompt analítico compartilhado por Claude e Gemini."""
    f = d["filtros"]
    d_prev_ini = d["d_prev_ini"]
    d_prev_fim = d["d_prev_fim"]
    escopo = f"Delegacia {f.dp_des}" if f.dp_cod else "SP-Capital (toda a cidade)"

    # --- Top 10 naturezas -------------------------------------------------------
    nat_str = "\n".join(
        f"  {i+1}. {r['NATUREZA_APURADA']}: {int(r['N']):,} ocorrencias"
        for i, (_, r) in enumerate(d["nat_counts"].head(10).iterrows())
    ) if not d["nat_counts"].empty else "  (sem dados)"

    # --- Top 5 DPs (só quando sem filtro de DP) ---------------------------------
    dp_str = ""
    if not d["dp_rank"].empty and not f.dp_cod:
        dp_str = "\nTop 5 Delegacias com maior volume:\n" + "\n".join(
            f"  {i+1}. {r['DpGeoDes']}: {int(r['N']):,}"
            for i, (_, r) in enumerate(d["dp_rank"].head(5).iterrows())
        )

    # --- Comparativo YoY por natureza (top 5) -----------------------------------
    yoy_str = ""
    if not d["yoy_df"].empty and d["total_periodo_anterior"] > 0:
        linhas = []
        for _, row in d["yoy_df"].head(5).iterrows():
            var = row.get("variacao_pct", float("nan"))
            if pd.isna(var):
                var_str = "nova natureza"
            else:
                var_str = f"{var:+.1f}%"
            linhas.append(
                f"  {row['NATUREZA_APURADA']}: {int(row['atual']):,} atual"
                f" vs {int(row['anterior']):,} anterior ({var_str})"
            )
        yoy_str = (
            f"\nComparativo por natureza"
            f" ({f.data_ini.strftime('%d/%m/%Y')}-{f.data_fim.strftime('%d/%m/%Y')}"
            f" vs {d_prev_ini.strftime('%d/%m/%Y')}-{d_prev_fim.strftime('%d/%m/%Y')}):\n"
            + "\n".join(linhas)
        )

    # --- Padrão hora × dia -------------------------------------------------------
    mhd_str = "  Dados de hora/dia nao disponiveis para este filtro."
    if not d["mhd_f"].empty and {"FAIXA_HORA", "DIA_SEMANA", "N"}.issubset(d["mhd_f"].columns):
        faixa_tot = d["mhd_f"].groupby("FAIXA_HORA", observed=True)["N"].sum()
        dia_tot   = d["mhd_f"].groupby("DIA_SEMANA",  observed=True)["N"].sum()
        pico_faixa = faixa_tot.idxmax()
        pico_dia   = dia_tot.idxmax()
        pico_dia_label = (
            str(pico_dia).split(".", 1)[-1].replace("-", " ").title()
            if "." in str(pico_dia) else str(pico_dia)
        )
        pct_faixa = faixa_tot[pico_faixa] / faixa_tot.sum() * 100 if faixa_tot.sum() > 0 else 0
        # Distribuição completa por faixa
        faixas_detalhe = "  ".join(
            f"{faixa}: {int(n):,} ({n/faixa_tot.sum()*100:.1f}%)"
            for faixa, n in faixa_tot.sort_index().items()
        )
        mhd_str = (
            f"Pico: faixa {pico_faixa} ({pct_faixa:.1f}%), dia {pico_dia_label}.\n"
            f"  Distribuicao por faixa: {faixas_detalhe}"
        )

    # --- Pico por dia do mês -----------------------------------------------------
    dm_str = ""
    if d.get("dia_mes_f") is not None and not d["dia_mes_f"].empty:
        dm_tot = d["dia_mes_f"].groupby("DIA_MES")["N"].sum()
        if not dm_tot.empty:
            pico_dm = int(dm_tot.idxmax())
            pct_dm = dm_tot[pico_dm] / dm_tot.sum() * 100 if dm_tot.sum() > 0 else 0
            dm_str = f"\nPico por dia do mes: dia {pico_dm} ({int(dm_tot[pico_dm]):,} ocorrencias, {pct_dm:.1f}% do total)."

    # --- Tendência dos últimos meses ---------------------------------------------
    trend_str = ""
    if not d["serie_total"].empty and len(d["serie_total"]) >= 2:
        ultimos = d["serie_total"].tail(4)
        linhas_t = [
            f"  {str(row['DATA'])[:7]}: {int(row['N']):,} ocorrencias"
            for _, row in ultimos.iterrows()
        ]
        trend_str = "\nEvolucao recente (ultimos meses no periodo):\n" + "\n".join(linhas_t)

    # --- Variação total ----------------------------------------------------------
    if d["total_periodo_anterior"] > 0:
        delta_label = (
            f"{d['delta_yoy']:+.1f}% em relacao ao mesmo periodo do ano anterior"
            f" ({d['total_periodo_anterior']:,} ocorrencias de"
            f" {d_prev_ini.strftime('%d/%m/%Y')} a {d_prev_fim.strftime('%d/%m/%Y')})"
        )
    else:
        delta_label = "sem periodo anterior disponivel para comparacao"

    return f"""Voce e um analista senior de seguranca publica do Estado de Sao Paulo, \
especializado em inteligencia policial e gestao estrategica de recursos. \
Analise os dados criminais abaixo e produza um texto analitico em **5 a 6 paragrafos**, \
escrito em portugues formal e objetivo, adequado para um relatorio institucional da SSP-SP \
destinado a gestores e delegados.

O texto deve obrigatoriamente:
1. Contextualizar o volume total e a variacao percentual em relacao ao mesmo periodo do ano anterior
2. Destacar as naturezas criminais dominantes com percentuais sobre o total
3. Identificar naturezas em crescimento acelerado ou queda relevante (usar tabela YoY se disponivel)
4. Analisar o padrao temporal (faixa de hora e dia da semana de maior incidencia)
5. Se disponivel, mencionar o pico por dia do mes e implicacoes para planejamento de policiamento
6. Concluir com recomendacoes operacionais especificas (reforco de efetivo, rondas direcionadas, acoes preventivas)
- Usar negrito (**texto**) para destacar numeros e conclusoes-chave
- Citar numeros absolutos e percentuais (evite afirmacoes vagas)

DADOS DO RELATORIO:
Periodo analisado: {f.data_ini.strftime('%d/%m/%Y')} a {f.data_fim.strftime('%d/%m/%Y')}
Escopo geografico: {escopo}
Naturezas filtradas: {', '.join(f.naturezas) if f.naturezas else 'todas as naturezas'}
Total de ocorrencias no periodo: {d['total_periodo']:,}
Variacao YoY: {delta_label}

Top 10 naturezas criminais:
{nat_str}
{yoy_str}
{dp_str}

Padrao temporal (hora x dia da semana):
{mhd_str}
{dm_str}
{trend_str}
"""


def _insights_claude(d: dict) -> str:
    """Insights via Claude (Anthropic API)."""
    try:
        import anthropic  # type: ignore
    except ImportError:
        raise RuntimeError("anthropic nao instalado")
    api_key = _get_api_key("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY nao configurada")

    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1200,
        messages=[{"role": "user", "content": _build_insights_prompt(d)}],
    )
    return msg.content[0].text


def _insights_gemini(d: dict) -> str:
    """Insights via Gemini 2.0 Flash (Google AI — tier gratuito).

    Requer: pip install google-genai
    Chave:  GOOGLE_API_KEY (aistudio.google.com → Get API key — sem cartão)
    """
    try:
        from google import genai  # type: ignore
    except ImportError:
        raise RuntimeError("google-genai nao instalado")
    api_key = _get_api_key("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY nao configurada")

    client = genai.Client(api_key=api_key)
    prompt = _build_insights_prompt(d)
    # Tenta modelos em ordem de preferência (gratuitos disponíveis)
    for _model in ("gemini-2.5-flash-lite", "gemini-2.0-flash", "gemini-2.5-flash"):
        try:
            response = client.models.generate_content(model=_model, contents=prompt)
            return response.text
        except Exception:
            continue
    raise RuntimeError("Nenhum modelo Gemini disponivel no momento")


# ---------------------------------------------------------------------------
# Renderização HTML
# ---------------------------------------------------------------------------

_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: 'Segoe UI', Arial, sans-serif;
    background: #F5F7FA;
    color: #1A1A2E;
    font-size: 14px;
}
header {
    background: linear-gradient(135deg, #0C2B4E 0%, #1565C0 100%);
    color: white;
    padding: 32px 48px 24px;
    border-bottom: 4px solid #FFC107;
}
header .brand { font-size: 12px; letter-spacing: 2px; opacity: 0.8; text-transform: uppercase; }
header h1 { font-size: 28px; font-weight: 700; margin: 8px 0 4px; }
header .subtitle { font-size: 16px; opacity: 0.85; margin-bottom: 16px; }
header .meta {
    display: flex; gap: 24px; font-size: 12px; opacity: 0.7;
    flex-wrap: wrap; border-top: 1px solid rgba(255,255,255,0.2); padding-top: 12px;
}
main { max-width: 1200px; margin: 0 auto; padding: 32px 24px; }
section { background: white; border-radius: 8px; padding: 24px; margin-bottom: 24px;
          box-shadow: 0 1px 4px rgba(0,0,0,0.08); }
section h2 {
    font-size: 16px; font-weight: 600; color: #0C2B4E;
    border-left: 4px solid #1565C0; padding-left: 12px; margin-bottom: 20px;
}
.kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; }
.kpi-card {
    background: #F0F4FF; border-radius: 8px; padding: 20px;
    border-top: 3px solid #1565C0;
}
.kpi-title { font-size: 11px; text-transform: uppercase; letter-spacing: 1px;
              color: #666; margin-bottom: 8px; }
.kpi-value { font-size: 28px; font-weight: 700; color: #0C2B4E; }
.kpi-sub { font-size: 12px; color: #888; margin-top: 4px; }
.kpi-card.alert { border-top-color: #E53935; }
.kpi-card.good  { border-top-color: #43A047; }
.kpi-card.alert .kpi-sub { color: #E53935; font-weight: 600; }
.kpi-card.good  .kpi-sub { color: #43A047; font-weight: 600; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
thead tr { background: #0C2B4E; color: white; }
thead th { padding: 10px 14px; text-align: left; font-weight: 500; }
tbody tr:nth-child(even) { background: #F5F7FA; }
tbody tr:hover { background: #E3F2FD; }
tbody td { padding: 9px 14px; border-bottom: 1px solid #EEE; }
.insights { background: #FFFDE7; border: 1px solid #FFE082; }
.insights h2 { border-left-color: #FFC107; }
.insights-body { font-size: 14px; line-height: 1.75; color: #333; }
.insights-body p { margin-bottom: 14px; }
.insights-body strong { color: #0C2B4E; }
.insights-body em { color: #888; font-style: italic; font-size: 12px; }
footer {
    text-align: center; padding: 24px; font-size: 11px; color: #999;
    border-top: 1px solid #DDD; margin-top: 16px;
}
@media print {
    header { background: #0C2B4E !important; -webkit-print-color-adjust: exact; }
    section { page-break-inside: avoid; }
}
@media (max-width: 768px) { .kpi-grid { grid-template-columns: repeat(2, 1fr); } }
"""


def render_html(f: GlobalFilters, d: dict, figs: dict, insights: str) -> str:
    """Gera o relatório como HTML autocontido com Plotly via CDN."""
    escopo = f"Delegacia {f.dp_des}" if f.dp_cod else "SP-Capital"
    nat_label = ", ".join(f.naturezas) if f.naturezas else "Todas as naturezas"
    gerado_em = datetime.now().strftime("%d/%m/%Y às %H:%M")

    def _fig_html(fig: go.Figure) -> str:
        return pio.to_html(fig, full_html=False, include_plotlyjs=False,
                           config={"responsive": True})

    def _kpi(titulo: str, valor: str, sub: str = "", cls: str = "") -> str:
        sub_html = f'<div class="kpi-sub">{sub}</div>' if sub else ""
        return (
            f'<div class="kpi-card {cls}">'
            f'<div class="kpi-title">{titulo}</div>'
            f'<div class="kpi-value">{valor}</div>{sub_html}</div>'
        )

    delta = d["delta_yoy"]
    delta_cls = "alert" if delta > 0 else ("good" if delta < 0 else "")
    num_nats = len(d["nat_counts"]) if not d["nat_counts"].empty else 0
    total_str = f"{d['total_periodo']:,}".replace(",", ".")
    ult_ano_str = f"{d['total_ultimo_ano']:,}".replace(",", ".")
    delta_str = f"{delta:+.1f}% vs. ano anterior"

    kpis_html = (
        _kpi("Total no Período", total_str)
        + _kpi(f"Total em {d['ultimo_ano']}", ult_ano_str, delta_str, delta_cls)
        + _kpi("Naturezas Distintas", str(num_nats))
        + _kpi("Escopo", escopo)
    )

    sections: list[str] = []

    # KPIs
    sections.append(
        f'<section><h2>Indicadores Principais</h2>'
        f'<div class="kpi-grid">{kpis_html}</div></section>'
    )

    # Figuras na ordem desejada
    fig_config = [
        ("serie_total",      "Tendência Mensal — Total Geral"),
        ("evolucao_mensal",  "Evolução Mensal — Top 5 Naturezas"),
        ("evol_anual",       "Comparativo Anual por Natureza"),
        ("top_naturezas",    "Ranking de Naturezas Criminais"),
        ("yoy",              "Comparação com o Mesmo Período do Ano Anterior"),
        ("ranking_dp",       "Ranking por Delegacia (DP) — Top 15"),
        ("mapa_dp",          "Distribuição Geográfica por Delegacia (DP)"),
        ("mapa_pontos",      "Pontos de Ocorrência"),
        ("mapa_calor",       "Mapa de Calor — Densidade de Ocorrências"),
        ("matriz",           "Distribuição por Dia da Semana e Faixa Horária"),
    ]
    for key, titulo in fig_config:
        if key in figs:
            sections.append(
                f'<section><h2>{titulo}</h2>{_fig_html(figs[key])}</section>'
            )

    # Tabela top naturezas
    if not d["nat_counts"].empty and d["total_periodo"] > 0:
        rows = "".join(
            f'<tr><td>{i+1}</td><td>{r["NATUREZA_APURADA"]}</td>'
            f'<td style="text-align:right">{int(r["N"]):,}</td>'
            f'<td style="text-align:right">{r["N"]/d["total_periodo"]*100:.1f}%</td></tr>'
            for i, (_, r) in enumerate(d["nat_counts"].head(20).iterrows())
        ).replace(",", ".")
        sections.append(
            '<section><h2>Tabela de Naturezas Criminais — Top 20</h2>'
            '<table><thead><tr><th>#</th><th>Natureza</th>'
            '<th>Ocorrências</th><th>% do Total</th></tr></thead>'
            f'<tbody>{rows}</tbody></table></section>'
        )

    # Ranking DP como tabela
    if not d["dp_rank"].empty:
        dp_rows = "".join(
            f'<tr><td>{i+1}</td><td>{r["DpGeoDes"]}</td>'
            f'<td style="text-align:right">{int(r["N"]):,}</td></tr>'
            for i, (_, r) in enumerate(d["dp_rank"].iterrows())
        ).replace(",", ".")
        sections.append(
            '<section><h2>Tabela de Delegacias — Top 15</h2>'
            '<table><thead><tr><th>#</th><th>Delegacia</th>'
            '<th>Ocorrências</th></tr></thead>'
            f'<tbody>{dp_rows}</tbody></table></section>'
        )

    # Insights
    insights_clean = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', insights)
    insights_clean = re.sub(r'\*(.+?)\*', r'<em>\1</em>', insights_clean)
    insights_clean = insights_clean.replace("\n\n", "</p><p>").replace("\n", "<br>")
    sections.append(
        '<section class="insights"><h2>Insights Analíticos</h2>'
        f'<div class="insights-body"><p>{insights_clean}</p></div></section>'
    )

    sections_html = "\n".join(sections)

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Relatório Criminal — {escopo} · {f.data_ini.strftime('%m/%Y')}</title>
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
<style>{_CSS}</style>
</head>
<body>
<header>
  <div class="brand">InsightGeoLab AI · Portal de Análise Criminal</div>
  <h1>Relatório de Análise Criminal</h1>
  <div class="subtitle">{escopo}</div>
  <div class="meta">
    <span>📅 Período: {f.data_ini.strftime('%d/%m/%Y')} a {f.data_fim.strftime('%d/%m/%Y')}</span>
    <span>🔍 Naturezas: {nat_label}</span>
    <span>🕐 Gerado em: {gerado_em}</span>
  </div>
</header>
<main>{sections_html}</main>
<footer>
  <p>Fonte dos dados: SSP-SP · IBGE · Portal de Análise Criminal — InsightGeoLab AI &copy; 2026</p>
  <p>Este relatório é de uso interno. As visualizações interativas requerem conexão com a internet (Plotly CDN).</p>
</footer>
</body>
</html>"""


# ===========================================================================
# Geração de PDF (requer kaleido + fpdf2)
# ===========================================================================

_BLUE   = (12,  43,  78)
_ACCENT = (21, 101, 192)
_GOLD   = (255, 193,   7)
_LIGHT  = (240, 244, 255)
_WHITE  = (255, 255, 255)
_BODY   = (40,  40,  60)
_GREY   = (150, 150, 150)

# Dimensões (px) para exportar cada figura via kaleido.
_FIG_DIMS: dict[str, tuple[int, int]] = {
    "serie_total":     (1100, 420),
    "evolucao_mensal": (1100, 460),
    "evol_anual":      (1100, 460),
    "top_naturezas":   (1100, 580),
    "yoy":             (1100, 580),
    "ranking_dp":      (1100, 520),
    "mapa_dp":         (1100, 580),
    "mapa_pontos":     (1100, 580),
    "mapa_calor":      (1100, 580),
    "matriz":          (1000, 380),
}


def _safe_pdf_text(text: str) -> str:
    """Garante compatibilidade com latin-1 (fonte Helvetica do fpdf2).

    Substitui os caracteres fora do range latin-1 mais comuns por equivalentes
    ASCII antes de escrever qualquer texto no PDF. Acentos portugueses
    (é, ã, ç, à etc.) são latin-1 e passam intactos.
    """
    _subs = {
        '–': '-',    # en dash  –
        '—': '-',    # em dash  —
        '→': '->',   # seta     →
        '←': '<-',   # seta     ←
        '‘': "'",    # aspas    '
        '’': "'",    # aspas    '
        '“': '"',    # aspas    "
        '”': '"',    # aspas    "
        '…': '...',  # reticências …
    }
    for orig, sub in _subs.items():
        text = text.replace(orig, sub)
    # Descarta qualquer outro char fora de latin-1 (emojis, etc.)
    return text.encode('latin-1', errors='ignore').decode('latin-1')


def _strip_md(text: str) -> str:
    """Remove marcações markdown; substitui **bold** por MAIÚSCULAS; sanitiza para latin-1."""
    text = re.sub(r'\*\*(.+?)\*\*', lambda m: m.group(1).upper(), text)
    text = re.sub(r'\*(.+?)\*', r'\1', text)
    text = re.sub(r'_(.+?)_', r'\1', text)   # _itálico_ → texto sem sublinhado
    text = text.lstrip('_').rstrip('_')       # underscores soltos nas bordas
    return _safe_pdf_text(text)


def _figs_to_png(figs: dict) -> dict[str, bytes]:
    """Converte cada figura Plotly em bytes PNG via kaleido."""
    import plotly.io as pio
    out: dict[str, bytes] = {}
    for key, fig in figs.items():
        w, h = _FIG_DIMS.get(key, (1100, 460))
        try:
            out[key] = pio.to_image(fig, format="png", width=w, height=h, scale=1.5)
        except Exception:
            pass
    return out


def _pdf_header_bar(pdf, escopo: str, periodo: str) -> None:
    """Barra de cabeçalho azul escuro com título e período."""
    pdf.set_fill_color(*_BLUE)
    pdf.rect(0, 0, 210, 38, style="F")
    # Barra dourada na base
    pdf.set_fill_color(*_GOLD)
    pdf.rect(0, 36, 210, 2, style="F")

    pdf.set_text_color(*_WHITE)
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_xy(15, 8)
    pdf.cell(0, 10, "Relatorio de Analise Criminal", ln=True)

    pdf.set_font("Helvetica", "", 12)
    pdf.set_xy(15, 20)
    pdf.cell(0, 6, escopo, ln=True)

    pdf.set_font("Helvetica", "I", 9)
    pdf.set_text_color(*_GOLD)
    pdf.set_xy(15, 28)
    pdf.cell(0, 6, f"InsightGeoLab AI  |  {periodo}", ln=True)

    pdf.set_text_color(*_BODY)
    pdf.set_y(46)


def _pdf_section_title(pdf, title: str) -> None:
    """Título de seção com borda esquerda azul."""
    if pdf.get_y() > 265:
        pdf.add_page()
    pdf.set_fill_color(*_ACCENT)
    pdf.rect(15, pdf.get_y(), 3, 7, style="F")
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(*_BLUE)
    pdf.set_xy(20, pdf.get_y())
    pdf.cell(0, 7, title, ln=True)
    pdf.set_text_color(*_BODY)
    pdf.ln(2)


def _pdf_kpi_row(pdf, items: list[tuple[str, str, str]], delta: float) -> None:
    """4 cartões KPI em linha."""
    card_w = 42.5
    x0 = 15.0
    y0 = pdf.get_y()
    card_h = 22.0

    for i, (titulo, valor, sub) in enumerate(items):
        x = x0 + i * (card_w + 1)
        # Borda superior colorida
        border_col = _ACCENT
        if i == 1:
            border_col = (227, 57, 53) if delta > 0 else ((67, 160, 71) if delta < 0 else _ACCENT)
        pdf.set_fill_color(*border_col)
        pdf.rect(x, y0, card_w, 2, style="F")
        # Fundo card
        pdf.set_fill_color(*_LIGHT)
        pdf.rect(x, y0 + 2, card_w, card_h - 2, style="F")
        # Título
        pdf.set_font("Helvetica", "", 7)
        pdf.set_text_color(*_GREY)
        pdf.set_xy(x + 2, y0 + 4)
        pdf.cell(card_w - 4, 4, titulo.upper(), ln=True)
        # Valor
        pdf.set_font("Helvetica", "B", 14)
        pdf.set_text_color(*_BLUE)
        pdf.set_xy(x + 2, y0 + 9)
        pdf.cell(card_w - 4, 7, str(valor), ln=True)
        # Sub — delta card (i==1) herda a cor da borda
        if sub:
            pdf.set_font("Helvetica", "", 7)
            sub_col = border_col if i == 1 and border_col != _ACCENT else _GREY
            pdf.set_text_color(*sub_col)
            pdf.set_xy(x + 2, y0 + 17)
            pdf.cell(card_w - 4, 4, str(sub), ln=True)

    pdf.set_text_color(*_BODY)
    pdf.set_y(y0 + card_h + 4)


def _pdf_add_chart(pdf, png_bytes: bytes) -> None:
    """Insere imagem PNG centralizada, com quebra de página automática."""
    import io
    # Estima altura da imagem em mm (w=180mm, proporcional)
    from PIL import Image as PILImage
    img = PILImage.open(io.BytesIO(png_bytes))
    w_px, h_px = img.size
    img_h_mm = 180.0 * h_px / w_px

    if pdf.get_y() + img_h_mm > 275:
        pdf.add_page()

    pdf.image(io.BytesIO(png_bytes), x=15, y=pdf.get_y(), w=180)
    pdf.set_y(pdf.get_y() + img_h_mm + 4)


def _pdf_table(
    pdf,
    headers: list[str],
    col_widths: list[float],
    rows: list[list[str]],
    row_height: float = 6.5,
) -> None:
    """Tabela simples com cabeçalho azul e linhas alternadas."""
    # Cabeçalho
    pdf.set_fill_color(*_BLUE)
    pdf.set_text_color(*_WHITE)
    pdf.set_font("Helvetica", "B", 8)
    for header, cw in zip(headers, col_widths):
        pdf.cell(cw, 8, header, border=0, fill=True, align="L")
    pdf.ln()

    pdf.set_text_color(*_BODY)
    pdf.set_font("Helvetica", "", 8)
    for i, row in enumerate(rows):
        if pdf.get_y() > 275:
            pdf.add_page()
            # Repete cabeçalho após quebra
            pdf.set_fill_color(*_BLUE)
            pdf.set_text_color(*_WHITE)
            pdf.set_font("Helvetica", "B", 8)
            for header, cw in zip(headers, col_widths):
                pdf.cell(cw, 8, header, border=0, fill=True, align="L")
            pdf.ln()
            pdf.set_text_color(*_BODY)
            pdf.set_font("Helvetica", "", 8)

        fill = i % 2 == 1
        if fill:
            pdf.set_fill_color(245, 247, 250)
        for j, (cell, cw) in enumerate(zip(row, col_widths)):
            align = "R" if j >= len(headers) - 2 else "L"
            pdf.cell(cw, row_height, str(cell), border=0, fill=fill, align=align)
        pdf.ln()
    pdf.ln(2)


def _pdf_insights(pdf, text: str) -> None:
    """Seção de insights com fundo amarelo claro.

    Usa ``fill=True`` no ``multi_cell`` para que o fundo seja desenhado
    linha-a-linha junto com o texto, evitando a necessidade de re-renderizar
    o conteúdo por cima de um rect desenhado depois.
    """
    clean = _strip_md(text)
    paragraphs = [p.strip() for p in clean.split("\n\n") if p.strip()]

    if pdf.get_y() > 240:
        pdf.add_page()

    x0 = 15

    # Barra dourada lateral + título — desenhados antes do texto
    y_title = pdf.get_y()
    pdf.set_fill_color(*_GOLD)
    pdf.rect(x0, y_title, 3, 8, style="F")
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(*_BLUE)
    pdf.set_xy(x0 + 5, y_title)
    pdf.cell(0, 8, "Insights Analiticos", ln=True)
    pdf.set_text_color(*_BODY)
    pdf.ln(3)

    # Parágrafos — fundo amarelo via fill=True no multi_cell (texto único)
    for para in paragraphs:
        if pdf.get_y() > 270:
            pdf.add_page()
        if para.startswith("Nota:") or para.startswith("Nota "):
            pdf.set_font("Helvetica", "I", 8)
            pdf.set_text_color(*_GREY)
            pdf.set_fill_color(255, 255, 255)
        else:
            pdf.set_font("Helvetica", "", 9)
            pdf.set_text_color(*_BODY)
            pdf.set_fill_color(255, 253, 231)
        pdf.set_x(x0 + 5)
        pdf.multi_cell(172, 5, para, border=0, align="J", fill=True)
        pdf.ln(2)

    pdf.set_text_color(*_BODY)
    pdf.ln(3)


def render_pdf(
    f: GlobalFilters,
    d: dict,
    figs: dict,
    insights: str,
    on_progress=None,
) -> bytes:
    """Gera PDF completo. Requer kaleido e fpdf2.

    ``on_progress(step, total, label)`` é chamado a cada etapa se fornecido.
    """
    try:
        from fpdf import FPDF
        from PIL import Image  # noqa: F401 — verifica se Pillow está disponível
    except ImportError as e:
        raise RuntimeError(f"Dependência ausente para PDF: {e}. Execute: pip install fpdf2 Pillow kaleido") from e

    escopo = _safe_pdf_text(
        f"Delegacia {f.dp_des}" if f.dp_cod else "SP-Capital (Cidade de Sao Paulo)"
    )
    periodo = f"{f.data_ini.strftime('%d/%m/%Y')} a {f.data_fim.strftime('%d/%m/%Y')}"
    nat_label = _safe_pdf_text(
        ", ".join(f.naturezas[:3]) + ("..." if len(f.naturezas) > 3 else "") if f.naturezas else "Todas"
    )

    total_steps = len(figs) + 3
    step = 0

    def _progress(label: str) -> None:
        nonlocal step
        step += 1
        if on_progress:
            on_progress(step, total_steps, label)

    # 1. Exporta figuras como PNG
    _progress("Exportando gráficos...")
    import io
    import plotly.io as pio

    fig_pngs: dict[str, bytes] = {}
    fig_order = [
        "serie_total", "evolucao_mensal", "evol_anual",
        "top_naturezas", "yoy", "ranking_dp",
        "mapa_dp", "mapa_pontos", "mapa_calor", "matriz",
    ]
    for key in fig_order:
        if key in figs:
            w, h = _FIG_DIMS.get(key, (1100, 460))
            try:
                fig_pngs[key] = pio.to_image(figs[key], format="png", width=w, height=h, scale=1.5)
            except Exception:
                pass
            _progress(f"Gráfico: {key}")

    # 2. Monta PDF
    _progress("Compondo PDF...")

    class _PDF(FPDF):
        def header(self):
            if self.page_no() > 1:
                self.set_fill_color(*_BLUE)
                self.rect(0, 0, 210, 7, style="F")
                self.set_font("Helvetica", "I", 6.5)
                self.set_text_color(*_WHITE)
                self.set_xy(10, 1)
                self.cell(140, 5, f"Portal de Analise Criminal · {escopo} · {periodo}")
                self.set_xy(155, 1)
                self.cell(50, 5, "InsightGeoLab AI", align="R")
                self.set_text_color(*_BODY)
                self.set_y(12)

        def footer(self):
            self.set_y(-11)
            self.set_font("Helvetica", "I", 7)
            self.set_text_color(*_GREY)
            self.cell(0, 5,
                f"Fonte: SSP-SP · IBGE · Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}  |  Pag. {self.page_no()}",
                align="C",
            )

    pdf = _PDF(orientation="P", unit="mm", format="A4")
    pdf.set_margins(15, 15, 15)
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # Capa
    _pdf_header_bar(pdf, escopo, periodo)

    # Metadados
    pdf.set_font("Helvetica", "", 8.5)
    pdf.set_text_color(*_GREY)
    pdf.set_x(15)
    pdf.cell(0, 5, f"Naturezas filtradas: {nat_label}  |  Gerado em: {datetime.now().strftime('%d/%m/%Y às %H:%M')}", ln=True)
    pdf.ln(4)

    # KPIs
    _pdf_section_title(pdf, "Indicadores Principais")
    delta = d["delta_yoy"]
    total_str = f"{d['total_periodo']:,}".replace(",", ".")
    ult_ano_str = f"{d['total_ultimo_ano']:,}".replace(",", ".")
    delta_str = f"{delta:+.1f}% vs. ano ant."
    num_nats = str(len(d["nat_counts"])) if not d["nat_counts"].empty else "0"

    _pdf_kpi_row(pdf, [
        ("Total no Periodo",         total_str,   ""),
        (f"Total em {d['ultimo_ano']}", ult_ano_str, delta_str),
        ("Naturezas Distintas",      num_nats,    ""),
        ("Escopo",                   escopo[:30], ""),
    ], delta=delta)

    # Gráficos
    chart_titles = {
        "serie_total":     "Tendencia Mensal - Total de Ocorrencias",
        "evolucao_mensal": "Evolucao Mensal - Top 5 Naturezas",
        "evol_anual":      "Comparativo Anual por Natureza",
        "top_naturezas":   "Ranking - Top 20 Naturezas Criminais",
        "yoy":             "Comparacao com o Mesmo Periodo do Ano Anterior",
        "ranking_dp":      "Ranking por Delegacia (DP) - Top 15",
        "mapa_dp":         "Distribuicao Geografica por Delegacia (DP)",
        "mapa_pontos":     "Pontos de Ocorrencia",
        "mapa_calor":      "Mapa de Calor - Densidade de Ocorrencias",
        "matriz":          "Distribuicao por Dia da Semana e Faixa Horaria",
    }
    for key in fig_order:
        if key in fig_pngs:
            pdf.add_page()
            _pdf_section_title(pdf, chart_titles.get(key, key))
            _pdf_add_chart(pdf, fig_pngs[key])

    # Tabela top naturezas
    if not d["nat_counts"].empty and d["total_periodo"] > 0:
        pdf.add_page()
        _pdf_section_title(pdf, "Tabela de Naturezas Criminais - Top 20")
        headers = ["#", "Natureza", "Ocorrencias", "% do Total"]
        col_widths = [10.0, 110.0, 34.0, 26.0]
        rows = [
            [
                str(i + 1),
                str(r["NATUREZA_APURADA"])[:60],
                f"{int(r['N']):,}".replace(",", "."),
                f"{r['N'] / d['total_periodo'] * 100:.1f}%",
            ]
            for i, (_, r) in enumerate(d["nat_counts"].head(20).iterrows())
        ]
        _pdf_table(pdf, headers, col_widths, rows)

    # Tabela ranking DP
    if not d["dp_rank"].empty:
        if pdf.get_y() > 200:
            pdf.add_page()
        _pdf_section_title(pdf, "Ranking por Delegacia (DP) - Top 15")
        headers = ["#", "Delegacia", "Ocorrencias"]
        col_widths = [10.0, 140.0, 30.0]
        rows = [
            [
                str(i + 1),
                _safe_pdf_text(str(r["DpGeoDes"]))[:55],
                f"{int(r['N']):,}".replace(",", "."),
            ]
            for i, (_, r) in enumerate(d["dp_rank"].iterrows())
        ]
        _pdf_table(pdf, headers, col_widths, rows)

    # Insights
    pdf.add_page()
    _pdf_section_title(pdf, "")   # espaço antes dos insights
    _pdf_insights(pdf, insights)

    _progress("Finalizando...")
    return bytes(pdf.output())
