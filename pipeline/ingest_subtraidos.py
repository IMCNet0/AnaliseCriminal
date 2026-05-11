"""Ingestão das bases de objetos subtraídos (celulares, veículos, objetos diversos).

Cada linha = 1 objeto de um BO. Um BO pode ter múltiplas linhas. Para análise agregada
mantemos todas as linhas e deduplicamos no nível de consulta se necessário.
"""
from __future__ import annotations

from pathlib import Path
import logging
import re

import numpy as np
import pandas as pd

from common import (
    RAW_SSP,
    PROCESSED,
    read_xlsx_streaming,
    clean_null_strings,
    parse_datetime_safe,
    to_float_safe,
    to_nullable_int,
    valid_sp_bounds,
    optimize_dtypes,
    stringify_cols,
    stringify_object_cols,
    setup_logging,
)

log = logging.getLogger(__name__)

FAMILIES = {
    "celulares": ("CelularesSubtraidos_*.xlsx", "celulares_subtraidos"),
    "veiculos": ("VeiculosSubtraidos_*.xlsx", "veiculos_subtraidos"),
    "objetos": ("ObjetosSubtraidos_*.xlsx", "objetos_subtraidos"),
}

KEEP_COLS = [
    "ID_DELEGACIA", "NOME_DEPARTAMENTO", "NOME_SECCIONAL", "NOME_DELEGACIA", "NOME_MUNICIPIO",
    "ANO_BO", "NUM_BO", "VERSAO",
    "DATA_OCORRENCIA_BO", "HORA_OCORRENCIA",
    "DATAHORA_REGISTRO_BO", "DATA_COMUNICACAO_BO",
    "DESCR_PERIODO",
    "RUBRICA", "DESCR_CONDUTA",
    "DESCR_TIPOLOCAL", "DESCR_SUBTIPOLOCAL",
    "CIDADE", "LOGRADOURO", "BAIRRO", "CEP",
    "LATITUDE", "LONGITUDE",
    "DESCR_MODO_OBJETO", "DESCR_TIPO_OBJETO", "DESCR_SUBTIPO_OBJETO",
    "QUANTIDADE_OBJETO", "MARCA_OBJETO",
    "FLAG_FLAGRANTE", "FLAG_STATUS", "FLAG_BLOQUEIO", "FLAG_DESBLOQUEIO",
    "MES_REGISTRO_BO", "ANO_REGISTRO_BO",
]

CATEGORICAL = [
    "NOME_DEPARTAMENTO", "NOME_SECCIONAL", "NOME_DELEGACIA", "NOME_MUNICIPIO",
    "CIDADE", "DESCR_PERIODO", "DESCR_TIPOLOCAL", "DESCR_SUBTIPOLOCAL",
    "RUBRICA", "DESCR_CONDUTA",
    "DESCR_MODO_OBJETO", "DESCR_TIPO_OBJETO", "DESCR_SUBTIPO_OBJETO",
    "MARCA_OBJETO", "FLAG_FLAGRANTE", "FLAG_STATUS",
    "FLAG_BLOQUEIO", "FLAG_DESBLOQUEIO",
]

FORCE_STRING = [
    "NUM_BO", "NUMERO_LOGRADOURO", "BAIRRO", "LOGRADOURO", "LOGRADOURO_VERSAO",
    "HORA_OCORRENCIA", "CEP", "CIDADE",
    "RUBRICA", "DESCR_CONDUTA", "MARCA_OBJETO",
    "DESCR_MODO_OBJETO", "DESCR_TIPO_OBJETO", "DESCR_SUBTIPO_OBJETO",
    "DESC_LEI", "AUTORIA_BO", "TIPO_INTOLERANCIA", "DESCR_APRESENTACAO",
    # VERSAO vem como int64 em uns anos, double em outros, string em outros
    # (ex.: 2017=double, 2022=string, 2023=int64). Unificar como string
    # é a única forma de evitar drift entre partições na leitura.
    "VERSAO",
]


def transform_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_null_strings(df)

    for col in KEEP_COLS:
        if col not in df.columns:
            df[col] = np.nan
    df = df[KEEP_COLS].copy()

    df["DATA_OCORRENCIA_BO"] = parse_datetime_safe(df["DATA_OCORRENCIA_BO"])
    df["DATAHORA_REGISTRO_BO"] = parse_datetime_safe(df["DATAHORA_REGISTRO_BO"])
    df["DATA_COMUNICACAO_BO"] = parse_datetime_safe(df["DATA_COMUNICACAO_BO"])

    df["LATITUDE"] = to_float_safe(df["LATITUDE"])
    df["LONGITUDE"] = to_float_safe(df["LONGITUDE"])
    # to_nullable_int evita "cannot safely cast non-equivalent float64 to int*"
    # mesmo quando há NaN, inf, valores fracionários ou fora do range do dtype.
    df["QUANTIDADE_OBJETO"] = to_nullable_int(df["QUANTIDADE_OBJETO"], "Int32")
    df["ANO_BO"] = to_nullable_int(df["ANO_BO"], "Int16")
    df["MES_REGISTRO_BO"] = to_nullable_int(df["MES_REGISTRO_BO"], "Int8")
    df["ANO_REGISTRO_BO"] = to_nullable_int(df["ANO_REGISTRO_BO"], "Int16")

    coords_ok = valid_sp_bounds(df["LATITUDE"], df["LONGITUDE"])
    df["COORDS_VALIDAS"] = coords_ok.astype("bool")
    df.loc[~coords_ok, ["LATITUDE", "LONGITUDE"]] = np.nan

    df["ANO"] = to_nullable_int(
        df["ANO_REGISTRO_BO"].fillna(df["DATAHORA_REGISTRO_BO"].dt.year), "Int16"
    )
    df["MES"] = to_nullable_int(
        df["MES_REGISTRO_BO"].fillna(df["DATAHORA_REGISTRO_BO"].dt.month), "Int8"
    )

    df = df.dropna(subset=["ANO", "MES"])

    # IMPORTANTE: força as colunas categóricas a "string" ANTES do optimize_dtypes.
    # Quando um chunk vem com a coluna 100% vazia, ela fica float64 (NaN),
    # vira category[float] e conflita no Parquet com outros chunks que têm
    # category[string]. Forçar "string" garante um schema estável entre partições.
    df = stringify_cols(df, FORCE_STRING + CATEGORICAL)
    df = stringify_object_cols(df)
    df = optimize_dtypes(df, categorical_cols=CATEGORICAL)
    return df


def ingest_family(glob_pat: str, out_name: str) -> int:
    files = sorted(RAW_SSP.glob(glob_pat))
    if not files:
        log.warning("Nenhum arquivo bate com %s", glob_pat)
        return 0
    out = PROCESSED / out_name
    out.mkdir(parents=True, exist_ok=True)

    grand = 0
    for p in files:
        log.info("Ingerindo %s", p.name)
        total = 0
        for chunk in read_xlsx_streaming(p, chunksize=40_000):
            t = transform_chunk(chunk)
            if t.empty:
                continue
            t.to_parquet(
                out, index=False, partition_cols=["ANO", "MES"], engine="pyarrow"
            )
            total += len(t)
            log.info("  ↳ %s linhas acumuladas neste arquivo", f"{total:,}")
        grand += total
    log.info("%s: %s linhas em %s", out_name, f"{grand:,}", out)
    return grand


def main(families: list[str] | None = None) -> None:
    """Ingere famílias de subtraídos.

    ``families`` aceita uma lista com qualquer combinação de
    {'celulares', 'veiculos', 'objetos'}; se None, roda as três.
    """
    setup_logging()
    targets = families if families else list(FAMILIES.keys())
    for key in targets:
        if key not in FAMILIES:
            log.error("Família desconhecida: %s (válidas: %s)", key, list(FAMILIES))
            continue
        pat, name = FAMILIES[key]
        ingest_family(pat, name)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Ingere bases de subtraídos da SSP-SP.")
    ap.add_argument(
        "--family",
        nargs="+",
        choices=list(FAMILIES.keys()),
        help="Subconjunto a rodar (ex.: --family veiculos objetos). "
             "Útil para retomar do ponto em que um crash parou.",
    )
    args = ap.parse_args()
    main(families=args.family)
