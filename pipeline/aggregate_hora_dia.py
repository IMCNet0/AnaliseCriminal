"""Agregado auxiliar: matriz temática Dia da Semana × Faixa Hora × DESC_PERIODO.

Gera ``data/aggregates/matriz_hora_dia.parquet`` a partir do dataset bruto
particionado em ``data/processed/sp_dados_criminais``. Esse agregado
alimenta a página "Séries Temporais" do portal.

Chaves: ANO · MES · NATUREZA_APURADA · DIA_SEMANA · FAIXA_HORA · DESC_PERIODO · N

Faixas de hora (derivadas de ``HORA_OCORRENCIA_BO``, "HH:MM"):
  • 00-05  → Madrugada
  • 06-11  → Manhã
  • 12-17  → Tarde
  • 18-23  → Noite

Dia da semana extraído de ``DATA_OCORRENCIA_BO`` (pt-BR: Seg…Dom).

Rode DEPOIS de ``run_all.py`` — ele depende do dataset já ingerido.
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from common import PROCESSED, AGGREGATES, setup_logging

log = logging.getLogger(__name__)

DIAS_PT = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]


def _faixa_from_hora(s: pd.Series) -> pd.Series:
    """HORA_OCORRENCIA_BO costuma vir como "HH:MM" (string). Convertemos
    pro inteiro da hora (0..23) e mapeamos para Madrugada/Manhã/Tarde/Noite.
    Valores inválidos viram ``NaN`` e são descartados no groupby.
    """
    s = s.astype("string")
    # Pega os 2 primeiros dígitos "HH:MM" → HH. Cobre também "H:MM" (→ H).
    h = s.str.extract(r"^\s*(\d{1,2})", expand=False)
    h = pd.to_numeric(h, errors="coerce")
    # Valores plausíveis 0..23
    h = h.where((h >= 0) & (h <= 23))
    bins = pd.cut(
        h, bins=[-1, 5, 11, 17, 23],
        labels=["Madrugada", "Manhã", "Tarde", "Noite"],
    )
    return bins.astype("string")


def _dia_from_data(s: pd.Series) -> pd.Series:
    d = pd.to_datetime(s, errors="coerce")
    wd = d.dt.weekday   # 0=Seg, 6=Dom
    return wd.map({i: DIAS_PT[i] for i in range(7)}).astype("string")


def load_base() -> pd.DataFrame:
    """Lê só as colunas necessárias pra a matriz — mantém leve mesmo na base
    completa (~5M+ linhas)."""
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
        "DATA_OCORRENCIA_BO", "HORA_OCORRENCIA_BO", "DESC_PERIODO",
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


def normalize_natureza(series: pd.Series) -> pd.Series:
    """Mesma canonicalização de pipeline/aggregate.py (sem acento, upper,
    colapsa espaços). Necessária pra casar com NATUREZA_APURADA dos outros
    agregados."""
    from unidecode import unidecode
    s = series.astype("string")
    return s.map(lambda x: None if pd.isna(x) else " ".join(unidecode(str(x)).upper().split())).astype("string")


def build() -> pd.DataFrame:
    df = load_base()

    # Canoniza natureza (igual ao resto do pipeline)
    if "NATUREZA_APURADA" in df.columns:
        df["NATUREZA_APURADA"] = normalize_natureza(df["NATUREZA_APURADA"])

    # Deriva eixos da matriz
    df["DIA_SEMANA"] = _dia_from_data(df["DATA_OCORRENCIA_BO"])
    df["FAIXA_HORA"] = _faixa_from_hora(df["HORA_OCORRENCIA_BO"])

    # DESC_PERIODO pode não existir em anos antigos — cria placeholder.
    if "DESC_PERIODO" not in df.columns:
        df["DESC_PERIODO"] = pd.NA
    df["DESC_PERIODO"] = df["DESC_PERIODO"].astype("string")

    # Remove linhas sem pelo menos Dia+Faixa (não alimentam a matriz).
    before = len(df)
    df = df.dropna(subset=["DIA_SEMANA", "FAIXA_HORA"])
    log.info("  %s linhas após descartar sem dia/hora (de %s)",
             f"{len(df):,}", f"{before:,}")

    grp = (
        df.groupby(
            ["ANO", "MES", "NATUREZA_APURADA",
             "DIA_SEMANA", "FAIXA_HORA", "DESC_PERIODO"],
            observed=True, dropna=False,
        ).size().reset_index(name="N")
    )
    # Tipagem enxuta pro parquet
    grp["N"] = grp["N"].astype("Int32")
    return grp


def main() -> None:
    setup_logging()
    AGGREGATES.mkdir(parents=True, exist_ok=True)
    out = AGGREGATES / "matriz_hora_dia.parquet"
    grp = build()
    grp.to_parquet(out, index=False, engine="pyarrow")
    log.info("OK → %s (%s linhas)", out, f"{len(grp):,}")


if __name__ == "__main__":
    main()
