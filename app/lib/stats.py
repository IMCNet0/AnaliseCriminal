"""Módulos estatísticos disponíveis para o usuário no Laboratório Estatístico.

Cada função é pura (recebe DataFrame/Series, devolve DataFrame/figura/dict).
As páginas Streamlit expõem os parâmetros via widgets e chamam estas funções.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

import numpy as np
import pandas as pd


# ----------- OUTLIERS ------------

@dataclass
class OutlierResult:
    flags: pd.Series      # bool, True = outlier
    score: pd.Series      # score contínuo (z, distância ao IQR, etc.)
    method: str
    n_outliers: int


def outliers_zscore(s: pd.Series, threshold: float = 3.0) -> OutlierResult:
    s = pd.to_numeric(s, errors="coerce")
    mu, sigma = s.mean(), s.std(ddof=1)
    if sigma == 0 or pd.isna(sigma):
        return OutlierResult(pd.Series(False, index=s.index), pd.Series(0.0, index=s.index), "zscore", 0)
    z = (s - mu) / sigma
    flags = z.abs() > threshold
    return OutlierResult(flags.fillna(False), z, "zscore", int(flags.sum()))


def outliers_iqr(s: pd.Series, k: float = 1.5) -> OutlierResult:
    s = pd.to_numeric(s, errors="coerce")
    q1, q3 = s.quantile(0.25), s.quantile(0.75)
    iqr = q3 - q1
    lo, hi = q1 - k * iqr, q3 + k * iqr
    flags = (s < lo) | (s > hi)
    score = pd.Series(0.0, index=s.index)
    score[s < lo] = (lo - s[s < lo]) / (iqr if iqr else 1)
    score[s > hi] = (s[s > hi] - hi) / (iqr if iqr else 1)
    return OutlierResult(flags.fillna(False), score, "iqr", int(flags.sum()))


def outliers_isolation_forest(df: pd.DataFrame, contamination: float = 0.05,
                              random_state: int = 42) -> OutlierResult:
    """df: apenas colunas numéricas a serem consideradas."""
    from sklearn.ensemble import IsolationForest
    X = df.select_dtypes(include="number").dropna()
    iso = IsolationForest(contamination=contamination, random_state=random_state, n_jobs=-1)
    pred = iso.fit_predict(X)
    score = pd.Series(-iso.score_samples(X), index=X.index, name="iso_score")
    flags = pd.Series(pred == -1, index=X.index)
    return OutlierResult(flags, score, "isolation_forest", int(flags.sum()))


# ----------- SÉRIES TEMPORAIS ------------

def stl_decompose(ts: pd.Series, period: int = 12, robust: bool = True) -> pd.DataFrame:
    """Decomposição STL. ts deve ter DatetimeIndex contínuo."""
    from statsmodels.tsa.seasonal import STL
    ts = ts.asfreq("MS").interpolate(limit_direction="both")
    res = STL(ts, period=period, robust=robust).fit()
    return pd.DataFrame({
        "observed": ts,
        "trend": res.trend,
        "seasonal": res.seasonal,
        "resid": res.resid,
    })


def forecast_arima(ts: pd.Series, horizon: int = 12,
                   order: tuple[int, int, int] = (1, 1, 1),
                   seasonal_order: tuple[int, int, int, int] = (1, 0, 1, 12)) -> pd.DataFrame:
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    ts = ts.asfreq("MS").interpolate(limit_direction="both")
    model = SARIMAX(ts, order=order, seasonal_order=seasonal_order,
                    enforce_stationarity=False, enforce_invertibility=False).fit(disp=False)
    pred = model.get_forecast(steps=horizon)
    fc = pred.predicted_mean
    ci = pred.conf_int(alpha=0.05)
    out = pd.DataFrame({
        "yhat": fc,
        "yhat_lower": ci.iloc[:, 0],
        "yhat_upper": ci.iloc[:, 1],
    })
    out.index.name = "DATA"
    return out


def prophet_available() -> bool:
    """Detecta se o Prophet foi instalado. Em prod (Streamlit Cloud, 1GB RAM)
    removemos do requirements porque stan/cmdstanpy consomem ~400 MB só no import.
    Em dev local segue ativo se o usuário instalou."""
    try:
        import importlib.util
        return importlib.util.find_spec("prophet") is not None
    except Exception:
        return False


def forecast_prophet(ts: pd.Series, horizon: int = 12) -> pd.DataFrame:
    """Alternativa ao ARIMA — mais robusta a feriados e quebras estruturais."""
    if not prophet_available():
        raise RuntimeError(
            "Prophet não está instalado neste ambiente (economia de memória no "
            "Streamlit Cloud). Use SARIMA, que oferece precisão comparável pra "
            "esta série mensal."
        )
    from prophet import Prophet
    df = ts.rename("y").reset_index().rename(columns={ts.index.name or "index": "ds"})
    m = Prophet(weekly_seasonality=False, daily_seasonality=False)
    m.fit(df)
    future = m.make_future_dataframe(periods=horizon, freq="MS")
    pred = m.predict(future)
    out = pred.set_index("ds")[["yhat", "yhat_lower", "yhat_upper"]].tail(horizon)
    out.index.name = "DATA"
    return out


# ----------- CLUSTERIZAÇÃO ------------

def kmeans(df_num: pd.DataFrame, n_clusters: int = 4, random_state: int = 42) -> pd.Series:
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
    X = StandardScaler().fit_transform(df_num.dropna())
    km = KMeans(n_clusters=n_clusters, n_init="auto", random_state=random_state).fit(X)
    return pd.Series(km.labels_, index=df_num.dropna().index, name=f"cluster_k{n_clusters}")


def hierarchical(df_num: pd.DataFrame, n_clusters: int = 4,
                 linkage: Literal["ward", "complete", "average", "single"] = "ward") -> pd.Series:
    from sklearn.cluster import AgglomerativeClustering
    from sklearn.preprocessing import StandardScaler
    X = StandardScaler().fit_transform(df_num.dropna())
    hc = AgglomerativeClustering(n_clusters=n_clusters, linkage=linkage).fit(X)
    return pd.Series(hc.labels_, index=df_num.dropna().index, name=f"cluster_h{n_clusters}")


def silhouette(df_num: pd.DataFrame, labels: pd.Series) -> float:
    from sklearn.metrics import silhouette_score
    from sklearn.preprocessing import StandardScaler
    X = StandardScaler().fit_transform(df_num.loc[labels.index].dropna())
    return float(silhouette_score(X, labels))


# ----------- CORRELAÇÕES E TESTES ------------

def correlation_matrix(df_num: pd.DataFrame,
                       method: Literal["pearson", "spearman", "kendall"] = "spearman") -> pd.DataFrame:
    return df_num.corr(method=method)


def test_mann_whitney(a: pd.Series, b: pd.Series) -> dict:
    from scipy.stats import mannwhitneyu
    a, b = a.dropna(), b.dropna()
    stat, p = mannwhitneyu(a, b, alternative="two-sided")
    return {"U": float(stat), "p_value": float(p), "n_a": len(a), "n_b": len(b)}


def test_kruskal(groups: list[pd.Series]) -> dict:
    from scipy.stats import kruskal
    groups = [g.dropna() for g in groups if len(g.dropna()) > 0]
    stat, p = kruskal(*groups)
    return {"H": float(stat), "p_value": float(p), "k": len(groups)}


def test_chi2(contingency: pd.DataFrame) -> dict:
    from scipy.stats import chi2_contingency
    chi2, p, dof, _ = chi2_contingency(contingency.values)
    return {"chi2": float(chi2), "p_value": float(p), "dof": int(dof)}


def bootstrap_ci(s: pd.Series, stat=np.mean, n_boot: int = 5_000,
                 alpha: float = 0.05, random_state: int = 42) -> tuple[float, float, float]:
    rng = np.random.default_rng(random_state)
    arr = s.dropna().to_numpy()
    if len(arr) == 0:
        return (np.nan, np.nan, np.nan)
    idx = rng.integers(0, len(arr), size=(n_boot, len(arr)))
    boot = np.apply_along_axis(stat, 1, arr[idx])
    lo, hi = np.quantile(boot, [alpha / 2, 1 - alpha / 2])
    return float(stat(arr)), float(lo), float(hi)


# ----------- UTILIDADES ------------

def taxa_per_100k(counts: pd.Series, pop: pd.Series) -> pd.Series:
    """counts e pop devem estar alinhadas pelo index (mesmo município / mesma área)."""
    return (counts / pop) * 100_000
