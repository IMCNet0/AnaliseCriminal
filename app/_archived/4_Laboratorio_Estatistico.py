"""Laboratório Estatístico — usuário escolhe a técnica e a aplica ao recorte atual.

Técnicas disponíveis:
- Outliers: z-score, IQR, Isolation Forest.
- Clusterização: K-means, Hierárquico.
- Correlações: Pearson/Spearman/Kendall.
- Testes de hipótese: Mann-Whitney, Kruskal-Wallis, Qui-quadrado.
- Intervalos de confiança (bootstrap).
"""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px

from lib.branding import apply_brand, header
from lib.filters import sidebar_filters
from lib import data, stats
from lib.downloads import download_buttons

apply_brand("Laboratório Estatístico · InsightGeoLab AI")
header("Laboratório Estatístico", "Aplique técnicas avançadas ao recorte atual")

f = sidebar_filters()

# Base: agregado por município (padrão mais rico para estatística multivariada)
df_muni = data.por_municipio()
if df_muni.empty:
    st.info("Rode o pipeline antes de abrir esta página.")
    st.stop()

mask = f.mask_date(df_muni) & f.mask_natureza(df_muni)
df_muni_f = df_muni.loc[mask]

label_col = "NM_MUN" if "NM_MUN" in df_muni_f.columns else "CD_MUN"

# Pivot: município × natureza (totais no período)
pivot = (
    df_muni_f.groupby([label_col, "NATUREZA_APURADA"], observed=True)["N"].sum()
    .unstack(fill_value=0)
)

tabs = st.tabs(["🚩 Outliers", "🧩 Clusterização", "🔗 Correlações",
                "⚖️ Testes de hipótese", "📐 Intervalos de confiança"])


# ------- OUTLIERS -------
with tabs[0]:
    metodo = st.selectbox("Método", ["Z-score", "IQR", "Isolation Forest"])
    nat = st.selectbox("Natureza-alvo", pivot.columns.tolist())
    if metodo == "Z-score":
        thr = st.slider("Threshold |z|", 1.5, 5.0, 3.0, 0.1)
        res = stats.outliers_zscore(pivot[nat], threshold=thr)
    elif metodo == "IQR":
        k = st.slider("Multiplicador k", 0.5, 3.0, 1.5, 0.1)
        res = stats.outliers_iqr(pivot[nat], k=k)
    else:
        cont = st.slider("Contamination", 0.01, 0.2, 0.05, 0.01)
        res = stats.outliers_isolation_forest(pivot, contamination=cont)
    out = pd.DataFrame({
        "municipio": pivot.index,
        "valor": pivot[nat].values,
        "score": res.score.reindex(pivot.index).values,
        "outlier": res.flags.reindex(pivot.index).fillna(False).values,
    }).sort_values("score", ascending=False, key=abs)
    st.success(f"{res.n_outliers} municípios marcados como outlier · método = {res.method}")
    st.dataframe(out.head(100), use_container_width=True, height=380)
    download_buttons(out, basename=f"outliers_{res.method}")


# ------- CLUSTERIZAÇÃO -------
with tabs[1]:
    metodo = st.selectbox("Método ", ["K-means", "Hierárquico (Ward)"])
    k = st.slider("Número de clusters", 2, 10, 4)
    # Usa matriz normalizada de naturezas como features
    X = pivot.copy()
    try:
        if metodo == "K-means":
            labels = stats.kmeans(X, n_clusters=k)
        else:
            labels = stats.hierarchical(X, n_clusters=k, linkage="ward")
        sil = stats.silhouette(X, labels)
        st.metric("Silhouette score", f"{sil:.3f}", help="Próximo de 1 = clusters bem separados")
        clust = X.copy()
        clust["cluster"] = labels
        perfil = clust.groupby("cluster").mean().round(1)
        st.write("#### Perfil médio por cluster (ocorrências)")
        st.dataframe(perfil, use_container_width=True)
        # Barras: nº de municípios por cluster
        counts = clust.groupby("cluster").size().reset_index(name="n_municipios")
        st.plotly_chart(px.bar(counts, x="cluster", y="n_municipios"),
                        use_container_width=True)
        download_buttons(clust.reset_index(), basename="clusters_municipios")
    except Exception as e:
        st.error(str(e))


# ------- CORRELAÇÕES -------
with tabs[2]:
    metodo = st.selectbox("Método  ", ["spearman", "pearson", "kendall"])
    top_n = st.slider("Top N naturezas (por volume) ", 5, 30, 12)
    top_nat = pivot.sum(axis=0).sort_values(ascending=False).head(top_n).index
    corr = stats.correlation_matrix(pivot[top_nat], method=metodo)
    fig = px.imshow(corr, zmin=-1, zmax=1, color_continuous_scale="RdBu_r",
                    aspect="auto", text_auto=".2f")
    fig.update_layout(height=620)
    st.plotly_chart(fig, use_container_width=True)
    download_buttons(corr.reset_index(), basename=f"correlacoes_{metodo}")


# ------- TESTES DE HIPÓTESE -------
with tabs[3]:
    tipo = st.selectbox("Teste", ["Mann-Whitney U (2 grupos)",
                                  "Kruskal-Wallis (k grupos)",
                                  "Qui-quadrado de independência"])
    if tipo == "Mann-Whitney U (2 grupos)":
        nat = st.selectbox("Natureza", pivot.columns.tolist(), key="mwu_nat")
        # Divide municípios em 2 grupos pela mediana de população agregada (proxy: total geral)
        tot = pivot.sum(axis=1)
        a = pivot[nat][tot > tot.median()]
        b = pivot[nat][tot <= tot.median()]
        st.write(stats.test_mann_whitney(a, b))
    elif tipo == "Kruskal-Wallis (k grupos)":
        nat = st.selectbox("Natureza", pivot.columns.tolist(), key="kw_nat")
        q = pd.qcut(pivot.sum(axis=1), 4, labels=False, duplicates="drop")
        groups = [pivot[nat][q == i] for i in sorted(q.dropna().unique())]
        st.write(stats.test_kruskal(groups))
    else:
        cols = st.multiselect("Escolha 2 naturezas para tabela de contingência",
                              pivot.columns.tolist(), max_selections=2)
        if len(cols) == 2:
            a = (pivot[cols[0]] > pivot[cols[0]].median()).astype(int)
            b = (pivot[cols[1]] > pivot[cols[1]].median()).astype(int)
            cont = pd.crosstab(a, b)
            st.write(cont)
            st.write(stats.test_chi2(cont))


# ------- INTERVALOS DE CONFIANÇA -------
with tabs[4]:
    nat = st.selectbox("Natureza   ", pivot.columns.tolist(), key="ci_nat")
    estat = st.selectbox("Estatística", ["média", "mediana"])
    func = np.mean if estat == "média" else np.median
    est, lo, hi = stats.bootstrap_ci(pivot[nat], stat=func, n_boot=3000)
    st.metric(estat.capitalize(),
              f"{est:,.1f}".replace(",", "."),
              help=f"IC 95% bootstrap: [{lo:,.1f} ; {hi:,.1f}]".replace(",", "."))
