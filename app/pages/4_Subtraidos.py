"""Análise de patrimônio subtraído: Celulares · Veículos · Objetos.

Fonte: SSP-SP — bases CelularesSubtraidos, VeiculosSubtraidos,
ObjetosSubtraidos, processadas pelo pipeline/aggregate_subtraidos.py.
"""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import plotly.express as px
import streamlit as st

from lib.branding import apply_brand, header
from lib.filters import sidebar_filters, sidebar_footer
from lib import data
from lib.downloads import download_buttons

apply_brand("Subtraídos · InsightGeoLab AI")
header(
    "Patrimônio Subtraído · SP-Capital",
    "Celulares, Veículos e Objetos subtraídos registrados na SSP-SP",
)

f = sidebar_filters()
sidebar_footer()

RUBRICA_LABEL = {
    "FURTO":        "Furto (sem violência)",
    "ROUBO":        "Roubo (com violência)",
    "PERDA EXTRAVIO": "Perda / Extravio",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mask_rubrica(df: pd.DataFrame, sel: list[str]) -> pd.Series:
    if not sel or "RUBRICA" not in df.columns:
        return pd.Series(True, index=df.index)
    col = df["RUBRICA"].astype("string").str.upper().str.strip()
    mask = pd.Series(False, index=df.index)
    for s in sel:
        mask |= col.str.startswith(s)
    return mask


def _serie_mensal(df: pd.DataFrame, color_col: str, top_n: int = 8) -> pd.DataFrame:
    """Agrega por DATA (primeiro dia do mês) e color_col, limita a top_n valores."""
    df2 = df.copy()
    df2["DATA"] = pd.to_datetime(
        df2["ANO"].astype(str) + "-" + df2["MES"].astype(str).str.zfill(2) + "-01"
    )
    top_vals = (
        df2.groupby(color_col, observed=True)["N"]
        .sum().nlargest(top_n).index.tolist()
    )
    df2[color_col] = df2[color_col].where(df2[color_col].isin(top_vals), other="Outros")
    return (
        df2.groupby(["DATA", color_col], observed=True)["N"]
        .sum().reset_index().sort_values("DATA")
    )


def _rubrica_filter(prefix: str, opcoes: list[str]) -> list[str]:
    """Widget de seleção de RUBRICA local a cada aba."""
    return st.multiselect(
        "Tipo de crime",
        options=opcoes,
        default=[],
        placeholder="Todos (Furto + Roubo + Perda/Extravio)",
        key=f"rubrica_{prefix}",
    )


def _mask_marca(df: pd.DataFrame, sel: list[str]) -> pd.Series:
    if not sel or "MARCA_OBJETO" not in df.columns:
        return pd.Series(True, index=df.index)
    return df["MARCA_OBJETO"].isin(sel)


def _marca_filter(prefix: str, marcas: list[str]) -> list[str]:
    return st.multiselect(
        "Filtrar por marca",
        options=marcas,
        default=[],
        placeholder="Todas as marcas",
        key=f"marcas_{prefix}",
    )


def _bairro_chart(df_all: pd.DataFrame, fonte: str,
                  f_filters, rubrica_sel: list[str]) -> None:
    """Gráfico horizontal de logradouros com maior incidência (top 15).

    Agrupa por LOGRADOURO; se disponível, mostra BAIRRO no label como contexto.
    Cai para agrupamento por BAIRRO caso LOGRADOURO ainda não exista no parquet.
    """
    if df_all.empty:
        st.info("Agregado de logradouros ainda não gerado. Rode `python pipeline/aggregate_subtraidos.py`.")
        return
    sub = df_all[df_all["FONTE"] == fonte].copy()
    if sub.empty:
        return
    mask = f_filters.mask_date(sub)
    if rubrica_sel:
        mask &= _mask_rubrica(sub, rubrica_sel)
    sub = sub.loc[mask]
    if sub.empty:
        st.info("Sem dados para o período/filtro selecionado.")
        return

    has_logr = "LOGRADOURO" in sub.columns
    group_col = "LOGRADOURO" if has_logr else "BAIRRO"

    # Filtra nulos/NA antes de agrupar
    sub = sub[sub[group_col].notna() & (sub[group_col].astype(str) != "<NA>") & (sub[group_col].astype(str) != "")]

    if has_logr:
        # Agrega por logradouro; pega o bairro mais frequente como contexto
        agg = sub.groupby("LOGRADOURO", observed=True).agg(N=("N", "sum")).reset_index()
        bairro_ctx = (
            sub.groupby(["LOGRADOURO", "BAIRRO"], observed=True)["N"]
            .sum().reset_index()
            .sort_values("N", ascending=False)
            .drop_duplicates("LOGRADOURO")[["LOGRADOURO", "BAIRRO"]]
        )
        agg = agg.merge(bairro_ctx, on="LOGRADOURO", how="left")
        bairro_col = agg["BAIRRO"].fillna("").astype(str)
        agg["Logradouro"] = agg["LOGRADOURO"] + agg.apply(
            lambda r: f" ({r['BAIRRO']})" if r["BAIRRO"] not in ("", "<NA>") else "", axis=1
        )
        top = agg.nlargest(15, "N").sort_values("N")[["Logradouro", "N"]].rename(columns={"N": "Ocorrências"})
        y_col = "Logradouro"
    else:
        top = (
            sub.groupby("BAIRRO", observed=True)["N"]
            .sum().nlargest(15).reset_index()
            .rename(columns={"BAIRRO": "Logradouro", "N": "Ocorrências"})
            .sort_values("Ocorrências")
        )
        y_col = "Logradouro"

    if top.empty:
        return
    fig = px.bar(
        top, x="Ocorrências", y=y_col, orientation="h",
        color="Ocorrências", color_continuous_scale="OrRd",
    )
    fig.update_layout(height=500, showlegend=False,
                      margin=dict(t=10, b=10, l=10), coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)


def _kpi_row(cols_data: list[tuple[str, str]]) -> None:
    cols = st.columns(len(cols_data))
    for col, (label, value) in zip(cols, cols_data):
        col.metric(label, value)


# ---------------------------------------------------------------------------
# Abas
# ---------------------------------------------------------------------------

df_bairro_all = data.por_bairro_subtraidos()

tab_cel, tab_vei, tab_obj = st.tabs(["📱 Celulares", "🚗 Veículos", "📦 Objetos"])


# ═══════════════════════════════════════════════════════════════════════════
# ABA 1 — CELULARES
# ═══════════════════════════════════════════════════════════════════════════
with tab_cel:
    df_cel_raw = data.por_celulares()
    if df_cel_raw.empty:
        st.info("Agregado de celulares ainda não gerado. Rode `python pipeline/aggregate_subtraidos.py`.")
    else:
        rubricas_cel = sorted(
            df_cel_raw["RUBRICA"].dropna().astype(str).unique().tolist()
        ) if "RUBRICA" in df_cel_raw.columns else []

        sel_rub_cel = _rubrica_filter("cel", rubricas_cel)
        marcas_cel = sorted(df_cel_raw["MARCA_OBJETO"].dropna().astype(str).unique().tolist()) if "MARCA_OBJETO" in df_cel_raw.columns else []
        sel_marcas_cel = _marca_filter("cel", marcas_cel)

        mask_cel = (
            f.mask_date(df_cel_raw)
            & _mask_rubrica(df_cel_raw, sel_rub_cel)
            & _mask_marca(df_cel_raw, sel_marcas_cel)
            & f.mask_dp(df_cel_raw)
        )
        df_cel = df_cel_raw.loc[mask_cel].copy()

        total_cel = int(df_cel["N"].sum())
        bloqueados = int(
            df_cel.loc[df_cel.get("FLAG_BLOQUEIO", pd.Series(dtype=str)) == "S", "N"].sum()
            if "FLAG_BLOQUEIO" in df_cel.columns else 0
        )
        pct_bloq = f"{bloqueados / total_cel * 100:.1f}%" if total_cel else "—"

        _kpi_row([
            ("Total de aparelhos", f"{total_cel:,}"),
            ("Bloqueados (Find My / Android)", f"{bloqueados:,}"),
            ("Taxa de bloqueio", pct_bloq),
        ])

        st.divider()

        # Série temporal por marca
        st.subheader("Evolução mensal por marca")
        serie_cel = _serie_mensal(df_cel, "MARCA_OBJETO")
        if not serie_cel.empty:
            fig = px.line(
                serie_cel, x="DATA", y="N", color="MARCA_OBJETO",
                labels={"DATA": "Mês", "N": "Aparelhos", "MARCA_OBJETO": "Marca"},
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.update_layout(height=360, margin=dict(t=10, b=10),
                              legend=dict(orientation="h", y=-0.2))
            st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)

        # Ranking de marcas
        with col1:
            st.subheader("Ranking de marcas")
            top_n_cel = st.slider("Top N marcas", 5, 20, 10, key="topn_cel")
            rank_cel = (
                df_cel.groupby("MARCA_OBJETO", observed=True)["N"]
                .sum().nlargest(top_n_cel).reset_index()
                .sort_values("N")
            )
            if not rank_cel.empty:
                fig2 = px.bar(
                    rank_cel, x="N", y="MARCA_OBJETO", orientation="h",
                    labels={"N": "Aparelhos", "MARCA_OBJETO": "Marca"},
                    color="N", color_continuous_scale="Blues",
                )
                fig2.update_layout(height=340, showlegend=False,
                                   margin=dict(t=10, b=10), coloraxis_showscale=False)
                st.plotly_chart(fig2, use_container_width=True)

        # Taxa de bloqueio por marca
        with col2:
            st.subheader("Taxa de bloqueio por marca")
            if "FLAG_BLOQUEIO" in df_cel.columns:
                bl = (
                    df_cel.groupby(["MARCA_OBJETO", "FLAG_BLOQUEIO"], observed=True)["N"]
                    .sum().reset_index()
                )
                tot_m = bl.groupby("MARCA_OBJETO", observed=True)["N"].sum()
                bloq_m = bl.loc[bl["FLAG_BLOQUEIO"] == "S"].groupby("MARCA_OBJETO", observed=True)["N"].sum()
                taxa = (bloq_m / tot_m * 100).dropna().sort_values(ascending=True).reset_index()
                taxa.columns = ["MARCA_OBJETO", "pct_bloqueio"]
                taxa = taxa[taxa["MARCA_OBJETO"].isin(rank_cel["MARCA_OBJETO"])]
                if not taxa.empty:
                    fig3 = px.bar(
                        taxa, x="pct_bloqueio", y="MARCA_OBJETO", orientation="h",
                        labels={"pct_bloqueio": "% Bloqueados", "MARCA_OBJETO": "Marca"},
                        color="pct_bloqueio", color_continuous_scale="Greens",
                        range_x=[0, 100],
                    )
                    fig3.update_layout(height=340, showlegend=False,
                                       margin=dict(t=10, b=10), coloraxis_showscale=False)
                    st.plotly_chart(fig3, use_container_width=True)

        # Distribuição por local
        st.subheader("Local do crime")
        col3, col4 = st.columns(2)
        with col3:
            if "DESCR_TIPOLOCAL" in df_cel.columns:
                loc_cel = (
                    df_cel.groupby("DESCR_TIPOLOCAL", observed=True)["N"]
                    .sum().nlargest(5).reset_index()
                )
                fig4 = px.pie(loc_cel, names="DESCR_TIPOLOCAL", values="N",
                              color_discrete_sequence=px.colors.qualitative.Pastel,
                              title="Tipo de local")
                fig4.update_layout(height=300, margin=dict(t=30, b=10))
                st.plotly_chart(fig4, use_container_width=True)
        with col4:
            if "DESCR_PERIODO" in df_cel.columns:
                per_cel = (
                    df_cel.groupby("DESCR_PERIODO", observed=True)["N"]
                    .sum().reset_index()
                )
                fig5 = px.pie(per_cel, names="DESCR_PERIODO", values="N",
                              color_discrete_sequence=px.colors.qualitative.Set3,
                              title="Período do dia")
                fig5.update_layout(height=300, margin=dict(t=30, b=10))
                st.plotly_chart(fig5, use_container_width=True)

        st.subheader("Logradouros com maior incidência")
        _bairro_chart(df_bairro_all, "CELULARES", f, sel_rub_cel)

        download_buttons(df_cel.drop(columns=["COORDS_VALIDAS"], errors="ignore"),
                         basename="celulares_subtraidos")


# ═══════════════════════════════════════════════════════════════════════════
# ABA 2 — VEÍCULOS
# ═══════════════════════════════════════════════════════════════════════════
with tab_vei:
    df_vei_raw = data.por_veiculos()
    if df_vei_raw.empty:
        st.info("Agregado de veículos ainda não gerado. Rode `python pipeline/aggregate_subtraidos.py`.")
    else:
        rubricas_vei = sorted(
            df_vei_raw["RUBRICA"].dropna().astype(str).unique().tolist()
        ) if "RUBRICA" in df_vei_raw.columns else []

        sel_rub_vei = _rubrica_filter("vei", rubricas_vei)
        marcas_vei = sorted(df_vei_raw["MARCA_OBJETO"].dropna().astype(str).unique().tolist()) if "MARCA_OBJETO" in df_vei_raw.columns else []
        sel_marcas_vei = _marca_filter("vei", marcas_vei)

        mask_vei = (
            f.mask_date(df_vei_raw)
            & _mask_rubrica(df_vei_raw, sel_rub_vei)
            & _mask_marca(df_vei_raw, sel_marcas_vei)
            & f.mask_dp(df_vei_raw)
        )
        df_vei = df_vei_raw.loc[mask_vei].copy()

        total_vei = int(df_vei["N"].sum())
        recuperados = int(
            df_vei.loc[df_vei["FLAG_STATUS"].astype("string").str.startswith("RECUPERADO"), "N"].sum()
            if "FLAG_STATUS" in df_vei.columns else 0
        )
        pct_rec = f"{recuperados / total_vei * 100:.1f}%" if total_vei else "—"

        _kpi_row([
            ("Total de veículos", f"{total_vei:,}"),
            ("Recuperados", f"{recuperados:,}"),
            ("Taxa de recuperação", pct_rec),
        ])

        st.divider()

        col_v1, col_v2 = st.columns(2)

        # Roubo vs Furto ao longo do tempo
        with col_v1:
            st.subheader("Roubo vs Furto — evolução mensal")
            if "DESCR_MODO_OBJETO" in df_vei.columns:
                serie_vei = _serie_mensal(df_vei, "DESCR_MODO_OBJETO", top_n=6)
                if not serie_vei.empty:
                    fig6 = px.line(
                        serie_vei, x="DATA", y="N", color="DESCR_MODO_OBJETO",
                        labels={"DATA": "Mês", "N": "Veículos", "DESCR_MODO_OBJETO": "Modo"},
                        color_discrete_sequence=px.colors.qualitative.Set1,
                    )
                    fig6.update_layout(height=320, margin=dict(t=10, b=10),
                                       legend=dict(orientation="h", y=-0.25))
                    st.plotly_chart(fig6, use_container_width=True)

        # Taxa de recuperação por tipo de veículo
        with col_v2:
            st.subheader("Recuperação por tipo de veículo")
            if "DESCR_TIPO_OBJETO" in df_vei.columns and "FLAG_STATUS" in df_vei.columns:
                rec_tipo = (
                    df_vei.groupby(["DESCR_TIPO_OBJETO", "FLAG_STATUS"], observed=True)["N"]
                    .sum().reset_index()
                )
                tot_t = rec_tipo.groupby("DESCR_TIPO_OBJETO", observed=True)["N"].sum()
                rec_t = rec_tipo.loc[
                    rec_tipo["FLAG_STATUS"].astype("string").str.startswith("RECUPERADO")
                ].groupby("DESCR_TIPO_OBJETO", observed=True)["N"].sum()
                taxa_t = (rec_t / tot_t * 100).dropna().sort_values(ascending=True).reset_index()
                taxa_t.columns = ["DESCR_TIPO_OBJETO", "pct_rec"]
                if not taxa_t.empty:
                    fig7 = px.bar(
                        taxa_t, x="pct_rec", y="DESCR_TIPO_OBJETO", orientation="h",
                        labels={"pct_rec": "% Recuperados", "DESCR_TIPO_OBJETO": "Tipo"},
                        color="pct_rec", color_continuous_scale="RdYlGn",
                        range_x=[0, 100],
                    )
                    fig7.update_layout(height=320, showlegend=False,
                                       margin=dict(t=10, b=10), coloraxis_showscale=False)
                    st.plotly_chart(fig7, use_container_width=True)

        # Ranking de marcas mais roubadas
        st.subheader("Marcas mais subtraídas")
        col_v3, col_v4 = st.columns(2)
        with col_v3:
            top_n_vei = st.slider("Top N marcas", 5, 20, 12, key="topn_vei")
            rank_vei = (
                df_vei.groupby("MARCA_OBJETO", observed=True)["N"]
                .sum().nlargest(top_n_vei).reset_index().sort_values("N")
            )
            if not rank_vei.empty:
                fig8 = px.bar(
                    rank_vei, x="N", y="MARCA_OBJETO", orientation="h",
                    labels={"N": "Veículos", "MARCA_OBJETO": "Marca"},
                    color="N", color_continuous_scale="Oranges",
                )
                fig8.update_layout(height=380, showlegend=False,
                                   margin=dict(t=10, b=10), coloraxis_showscale=False)
                st.plotly_chart(fig8, use_container_width=True)

        # Taxa de recuperação por marca
        with col_v4:
            if "FLAG_STATUS" in df_vei.columns and not rank_vei.empty:
                bl_m = (
                    df_vei[df_vei["MARCA_OBJETO"].isin(rank_vei["MARCA_OBJETO"])]
                    .groupby(["MARCA_OBJETO", "FLAG_STATUS"], observed=True)["N"]
                    .sum().reset_index()
                )
                tot_mm = bl_m.groupby("MARCA_OBJETO", observed=True)["N"].sum()
                rec_mm = bl_m.loc[
                    bl_m["FLAG_STATUS"].astype("string").str.startswith("RECUPERADO")
                ].groupby("MARCA_OBJETO", observed=True)["N"].sum()
                taxa_mm = (rec_mm / tot_mm * 100).dropna().sort_values(ascending=True).reset_index()
                taxa_mm.columns = ["MARCA_OBJETO", "pct_rec"]
                st.markdown("**Taxa de recuperação por marca**")
                if not taxa_mm.empty:
                    fig9 = px.bar(
                        taxa_mm, x="pct_rec", y="MARCA_OBJETO", orientation="h",
                        labels={"pct_rec": "% Recuperados", "MARCA_OBJETO": "Marca"},
                        color="pct_rec", color_continuous_scale="RdYlGn",
                        range_x=[0, 100],
                    )
                    fig9.update_layout(height=380, showlegend=False,
                                       margin=dict(t=10, b=10), coloraxis_showscale=False)
                    st.plotly_chart(fig9, use_container_width=True)

        st.subheader("Logradouros com maior incidência")
        _bairro_chart(df_bairro_all, "VEICULOS", f, sel_rub_vei)

        download_buttons(df_vei, basename="veiculos_subtraidos")


# ═══════════════════════════════════════════════════════════════════════════
# ABA 3 — OBJETOS
# ═══════════════════════════════════════════════════════════════════════════
with tab_obj:
    df_obj_raw = data.por_objetos()
    if df_obj_raw.empty:
        st.info("Agregado de objetos ainda não gerado. Rode `python pipeline/aggregate_subtraidos.py`.")
    else:
        rubricas_obj = sorted(
            df_obj_raw["RUBRICA"].dropna().astype(str).unique().tolist()
        ) if "RUBRICA" in df_obj_raw.columns else []

        sel_rub_obj = _rubrica_filter("obj", rubricas_obj)

        mask_obj = (
            f.mask_date(df_obj_raw)
            & _mask_rubrica(df_obj_raw, sel_rub_obj)
            & f.mask_dp(df_obj_raw)
        )
        df_obj = df_obj_raw.loc[mask_obj].copy()

        total_obj = int(df_obj["N"].sum())
        docs = int(
            df_obj.loc[
                df_obj.get("DESCR_TIPO_OBJETO", pd.Series(dtype=str))
                .astype("string").str.upper().str.startswith("DOC"), "N"
            ].sum()
            if "DESCR_TIPO_OBJETO" in df_obj.columns else 0
        )
        pct_docs = f"{docs / total_obj * 100:.1f}%" if total_obj else "—"

        _kpi_row([
            ("Total de objetos", f"{total_obj:,}"),
            ("Documentos subtraídos", f"{docs:,}"),
            ("% documentos", pct_docs),
        ])

        st.divider()

        col_o1, col_o2 = st.columns(2)

        # Série temporal por tipo de objeto
        with col_o1:
            st.subheader("Evolução mensal por tipo")
            if "DESCR_TIPO_OBJETO" in df_obj.columns:
                serie_obj = _serie_mensal(df_obj, "DESCR_TIPO_OBJETO", top_n=6)
                if not serie_obj.empty:
                    fig10 = px.line(
                        serie_obj, x="DATA", y="N", color="DESCR_TIPO_OBJETO",
                        labels={"DATA": "Mês", "N": "Objetos", "DESCR_TIPO_OBJETO": "Tipo"},
                        color_discrete_sequence=px.colors.qualitative.Set2,
                    )
                    fig10.update_layout(height=320, margin=dict(t=10, b=10),
                                        legend=dict(orientation="h", y=-0.25))
                    st.plotly_chart(fig10, use_container_width=True)

        # Ranking de tipos
        with col_o2:
            st.subheader("Tipos mais subtraídos")
            rank_obj = (
                df_obj.groupby("DESCR_TIPO_OBJETO", observed=True)["N"]
                .sum().nlargest(15).reset_index().sort_values("N")
            )
            if not rank_obj.empty:
                fig11 = px.bar(
                    rank_obj, x="N", y="DESCR_TIPO_OBJETO", orientation="h",
                    labels={"N": "Objetos", "DESCR_TIPO_OBJETO": "Tipo"},
                    color="N", color_continuous_scale="Purples",
                )
                fig11.update_layout(height=380, showlegend=False,
                                    margin=dict(t=10, b=10), coloraxis_showscale=False)
                st.plotly_chart(fig11, use_container_width=True)

        # Análise de documentos — subtipos
        st.subheader("Documentos subtraídos — detalhamento")
        if "DESCR_TIPO_OBJETO" in df_obj.columns and "DESCR_SUBTIPO_OBJETO" in df_obj.columns:
            df_docs = df_obj[
                df_obj["DESCR_TIPO_OBJETO"].astype("string").str.upper().str.startswith("DOC")
            ]
            if not df_docs.empty:
                col_d1, col_d2 = st.columns(2)
                with col_d1:
                    sub_rank = (
                        df_docs.groupby("DESCR_SUBTIPO_OBJETO", observed=True)["N"]
                        .sum().nlargest(15).reset_index().sort_values("N")
                    )
                    sub_rank = sub_rank[sub_rank["DESCR_SUBTIPO_OBJETO"] != "OUTROS"]
                    fig12 = px.bar(
                        sub_rank, x="N", y="DESCR_SUBTIPO_OBJETO", orientation="h",
                        labels={"N": "Ocorrências", "DESCR_SUBTIPO_OBJETO": "Tipo de documento"},
                        color="N", color_continuous_scale="Reds",
                    )
                    fig12.update_layout(height=400, showlegend=False,
                                        margin=dict(t=10, b=10), coloraxis_showscale=False)
                    st.plotly_chart(fig12, use_container_width=True)

                with col_d2:
                    serie_docs = _serie_mensal(df_docs, "DESCR_SUBTIPO_OBJETO", top_n=5)
                    if not serie_docs.empty:
                        fig13 = px.line(
                            serie_docs, x="DATA", y="N", color="DESCR_SUBTIPO_OBJETO",
                            labels={"DATA": "Mês", "N": "Ocorrências",
                                    "DESCR_SUBTIPO_OBJETO": "Documento"},
                            color_discrete_sequence=px.colors.qualitative.Set1,
                        )
                        fig13.update_layout(height=400, margin=dict(t=10, b=10),
                                            legend=dict(orientation="h", y=-0.25))
                        st.plotly_chart(fig13, use_container_width=True)
            else:
                st.info("Sem dados de documentos no período/filtro selecionado.")

        # Local do crime
        st.subheader("Local do crime")
        col_o3, col_o4 = st.columns(2)
        with col_o3:
            if "DESCR_TIPOLOCAL" in df_obj.columns:
                loc_obj = (
                    df_obj.groupby("DESCR_TIPOLOCAL", observed=True)["N"]
                    .sum().nlargest(5).reset_index()
                )
                fig14 = px.pie(loc_obj, names="DESCR_TIPOLOCAL", values="N",
                               color_discrete_sequence=px.colors.qualitative.Pastel,
                               title="Tipo de local")
                fig14.update_layout(height=300, margin=dict(t=30, b=10))
                st.plotly_chart(fig14, use_container_width=True)
        with col_o4:
            if "DESCR_PERIODO" in df_obj.columns:
                per_obj = (
                    df_obj.groupby("DESCR_PERIODO", observed=True)["N"]
                    .sum().reset_index()
                )
                fig15 = px.pie(per_obj, names="DESCR_PERIODO", values="N",
                               color_discrete_sequence=px.colors.qualitative.Set3,
                               title="Período do dia")
                fig15.update_layout(height=300, margin=dict(t=30, b=10))
                st.plotly_chart(fig15, use_container_width=True)

        st.subheader("Logradouros com maior incidência")
        _bairro_chart(df_bairro_all, "OBJETOS", f, sel_rub_obj)

        download_buttons(df_obj, basename="objetos_subtraidos")
