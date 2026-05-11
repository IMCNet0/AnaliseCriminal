"""Agregado auxiliar: matriz Dia do Mês × Faixa Hora × Natureza × DP.

Gera ``data/aggregates/dia_mes.parquet`` a partir do dataset bruto
particionado. Alimenta a matriz temática "Dia do Mês × Faixa Hora"
na página Séries Temporais.

Colunas: ANO · MES · DIA_MES (1-31) · FAIXA_HORA · NATUREZA_APURADA · DpGeoCod · N

Faixas de hora — mesmas definições de aggregate_hora_dia.py:
  00:00              → "00:00"          (hora declarada exata meia-noite / sem hora)
  00:01 às 06:00     → "00:01–06:00"
  06:01 às 12:00     → "06:01–12:00"
  12:01 às 18:00     → "12:01–18:00"
  18:01 às 23:59     → "18:01–23:59"

Rode DEPOIS de ``run_all.py``.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from common import PROCESSED, AGGREGATES, setup_logging

log = logging.getLogger(__name__)

DP_JSON = Path(__file__).resolve().parent.parent / "data" / "geo" / "DP.json"

FAIXAS_ORDEM = [
    "00:00", "00:01–06:00", "06:01–12:00", "12:01–18:00", "18:01–23:59",
]


def _faixa_from_hora(s: pd.Series) -> pd.Series:
    """Converte HORA_OCORRENCIA_BO ("HH:MM") em faixa rotulada."""
    s = s.astype("string").str.strip()
    parts = s.str.extract(r"^\s*(\d{1,2})\s*:\s*(\d{1,2})")
    hh = pd.to_numeric(parts[0], errors="coerce")
    mm = pd.to_numeric(parts[1], errors="coerce")
    hh = hh.where((hh >= 0) & (hh <= 23))
    mm = mm.where((mm >= 0) & (mm <= 59))
    mins = hh * 60 + mm
    bins = pd.cut(
        mins,
        bins=[-1, 0, 360, 720, 1080, 1439],
        labels=FAIXAS_ORDEM,
        right=True,
    )
    return bins.astype("string")


def load_base() -> pd.DataFrame:
    import pyarrow as pa
    import pyarrow.dataset as ds

    path = PROCESSED / "sp_dados_criminais"
    log.info("Lendo dataset: %s", path)
    partitioning = ds.partitioning(
        pa.schema([("ANO", pa.int32()), ("MES", pa.int32())]),
        flavor="hive",
    )
    dataset = ds.dataset(str(path), format="parquet", partitioning=partitioning)
    cols = [
        "ANO", "MES", "NATUREZA_APURADA",
        "DATA_OCORRENCIA_BO", "HORA_OCORRENCIA_BO",
        "LATITUDE", "LONGITUDE", "COORDS_VALIDAS",
    ]
    avail = dataset.schema.names
    cols = [c for c in cols if c in avail]
    df = dataset.to_table(columns=cols).to_pandas()
    if "ANO" in df.columns:
        df["ANO"] = df["ANO"].astype("Int16")
    if "MES" in df.columns:
        df["MES"] = df["MES"].astype("Int8")
    log.info("  %s linhas carregadas", f"{len(df):,}")
    return df


def _assign_dp(df: pd.DataFrame) -> pd.DataFrame:
    """Atribui DpGeoCod via spatial join — idêntico ao de aggregate_hora_dia.py."""
    if not DP_JSON.exists():
        log.warning("DP.json não encontrado — DpGeoCod ausente")
        df["DpGeoCod"] = pd.NA
        return df

    import geopandas as gpd

    log.info("Spatial join ponto → DP (%s pontos)…", f"{len(df):,}")
    valid_mask = (
        df.get("COORDS_VALIDAS", pd.Series(False, index=df.index))
        .fillna(False).astype(bool)
        & df["LATITUDE"].notna()
        & df["LONGITUDE"].notna()
    )
    df_v = df.loc[valid_mask].copy().reset_index(drop=True)

    gdf_dp = gpd.read_file(str(DP_JSON))
    if gdf_dp.crs is None:
        gdf_dp = gdf_dp.set_crs("EPSG:4326")
    else:
        gdf_dp = gdf_dp.to_crs("EPSG:4326")
    gdf_dp = gdf_dp[["DpGeoCod", "geometry"]].copy()

    pts = gpd.GeoDataFrame(
        df_v,
        geometry=gpd.points_from_xy(df_v["LONGITUDE"], df_v["LATITUDE"]),
        crs="EPSG:4326",
    )
    joined = gpd.sjoin(pts, gdf_dp, how="left", predicate="within")
    if joined.index.has_duplicates:
        joined = joined[~joined.index.duplicated(keep="first")]
    joined = joined.drop(columns=["geometry", "index_right"], errors="ignore")

    if "DpGeoCod" in joined.columns:
        nums = pd.to_numeric(joined["DpGeoCod"], errors="coerce")
        valid_nums = nums.dropna()
        if not valid_nums.empty and (valid_nums == valid_nums.astype("int64")).all():
            joined["DpGeoCod"] = nums.astype("Int64").astype("string")
        else:
            joined["DpGeoCod"] = joined["DpGeoCod"].astype("string").str.strip()

    df = df.copy()
    df["DpGeoCod"] = pd.NA
    df.loc[valid_mask, "DpGeoCod"] = joined["DpGeoCod"].values
    log.info("  DpGeoCod atribuído a %s/%s linhas",
             f"{df['DpGeoCod'].notna().sum():,}", f"{len(df):,}")
    return df


def normalize_natureza(series: pd.Series) -> pd.Series:
    from unidecode import unidecode
    s = series.astype("string")
    return s.map(
        lambda x: None if pd.isna(x) else " ".join(unidecode(str(x)).upper().split())
    ).astype("string")


def build() -> pd.DataFrame:
    df = load_base()

    if "NATUREZA_APURADA" in df.columns:
        df["NATUREZA_APURADA"] = normalize_natureza(df["NATUREZA_APURADA"])

    df = _assign_dp(df)

    # Dia do mês a partir da data de ocorrência
    if "DATA_OCORRENCIA_BO" in df.columns:
        dates = pd.to_datetime(df["DATA_OCORRENCIA_BO"], errors="coerce")
        df["DIA_MES"] = dates.dt.day.astype("Int8")
    else:
        df["DIA_MES"] = pd.NA

    # Faixa horária a partir da hora de ocorrência
    if "HORA_OCORRENCIA_BO" in df.columns:
        df["FAIXA_HORA"] = _faixa_from_hora(df["HORA_OCORRENCIA_BO"])
    else:
        df["FAIXA_HORA"] = pd.NA

    before = len(df)
    df = df.dropna(subset=["DIA_MES", "FAIXA_HORA"])
    log.info("  %s linhas após descartar sem DIA_MES/FAIXA_HORA (de %s)",
             f"{len(df):,}", f"{before:,}")

    grp = (
        df.groupby(
            ["ANO", "MES", "NATUREZA_APURADA", "DpGeoCod", "DIA_MES", "FAIXA_HORA"],
            observed=True, dropna=False,
        ).size().reset_index(name="N")
    )
    grp["N"] = grp["N"].astype("Int32")
    grp["DIA_MES"] = grp["DIA_MES"].astype("Int8")
    return grp


def main() -> None:
    setup_logging()
    AGGREGATES.mkdir(parents=True, exist_ok=True)
    out = AGGREGATES / "dia_mes.parquet"
    grp = build()
    grp.to_parquet(out, index=False, engine="pyarrow")
    log.info("OK → %s (%s linhas)", out, f"{len(grp):,}")


if __name__ == "__main__":
    main()
