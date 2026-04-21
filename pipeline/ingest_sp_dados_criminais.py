"""Ingestão da base principal SPDadosCriminais (série 2022→atual).

Cada linha = um registro de crime com latitude/longitude e recorte administrativo
completo (município, IBGE, CMD, BTL, CIA, departamento/seccional/delegacia).
"""
from __future__ import annotations

from pathlib import Path
import logging

import pandas as pd
import numpy as np

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

KEEP_COLS = [
    "NOME_DEPARTAMENTO", "NOME_SECCIONAL", "NOME_DELEGACIA", "NOME_MUNICIPIO",
    "NUM_BO", "ANO_BO",
    "DATA_REGISTRO", "DATA_OCORRENCIA_BO", "HORA_OCORRENCIA_BO", "DESC_PERIODO",
    "DESCR_TIPOLOCAL", "DESCR_SUBTIPOLOCAL",
    "BAIRRO",
    "LATITUDE", "LONGITUDE",
    "RUBRICA", "DESCR_CONDUTA", "NATUREZA_APURADA",
    "MES_ESTATISTICA", "ANO_ESTATISTICA",
    "CMD", "BTL", "CIA", "COD_IBGE",
]

CATEGORICAL = [
    "NOME_DEPARTAMENTO", "NOME_SECCIONAL", "NOME_DELEGACIA", "NOME_MUNICIPIO",
    "DESC_PERIODO", "DESCR_TIPOLOCAL", "DESCR_SUBTIPOLOCAL",
    "NATUREZA_APURADA", "RUBRICA", "DESCR_CONDUTA",
    "CMD", "BTL", "CIA",
]

# Colunas que podem vir com tipos mistos (int quando o BO é só dígitos, str quando
# tem letras). Tudo vira string para o Parquet aceitar.
FORCE_STRING = [
    "NUM_BO", "NUMERO_LOGRADOURO", "BAIRRO", "LOGRADOURO",
    "HORA_OCORRENCIA_BO", "RUBRICA", "DESCR_CONDUTA",
    "CMD", "BTL", "CIA", "COD_IBGE",
]


def transform_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_null_strings(df)

    # Colunas que podem vir com nomes levemente diferentes entre anos.
    # norm_col() já padronizou; mantemos apenas as previstas.
    for col in KEEP_COLS:
        if col not in df.columns:
            df[col] = np.nan

    df = df[KEEP_COLS].copy()

    df["DATA_REGISTRO"] = parse_datetime_safe(df["DATA_REGISTRO"])
    df["DATA_OCORRENCIA_BO"] = parse_datetime_safe(df["DATA_OCORRENCIA_BO"])
    df["LATITUDE"] = to_float_safe(df["LATITUDE"])
    df["LONGITUDE"] = to_float_safe(df["LONGITUDE"])
    # to_nullable_int evita "cannot safely cast non-equivalent float64 to int*"
    # mesmo quando há NaN, inf, valores fracionários ou fora do range do dtype.
    df["MES_ESTATISTICA"] = to_nullable_int(df["MES_ESTATISTICA"], "Int8")
    df["ANO_ESTATISTICA"] = to_nullable_int(df["ANO_ESTATISTICA"], "Int16")
    df["ANO_BO"] = to_nullable_int(df["ANO_BO"], "Int16")

    # Máscara de coordenadas válidas para SP
    coords_ok = valid_sp_bounds(df["LATITUDE"], df["LONGITUDE"])
    df["COORDS_VALIDAS"] = coords_ok.astype("bool")
    df.loc[~coords_ok, ["LATITUDE", "LONGITUDE"]] = np.nan

    # Partições
    df["ANO"] = to_nullable_int(
        df["ANO_ESTATISTICA"].fillna(df["DATA_REGISTRO"].dt.year), "Int16"
    )
    df["MES"] = to_nullable_int(
        df["MES_ESTATISTICA"].fillna(df["DATA_REGISTRO"].dt.month), "Int8"
    )

    # Remove linhas sem ANO/MES (vão para outras partições, mas se não há nem isso é lixo)
    df = df.dropna(subset=["ANO", "MES"])

    # Garante tipos consistentes antes do parquet.
    # IMPORTANTE: força as colunas categóricas a "string" ANTES do optimize_dtypes.
    # Se um chunk vem com a coluna 100% vazia, ela fica float64 (NaN), vira
    # category[float] e conflita com category[string] de outros chunks no Parquet,
    # produzindo erros como: ArrowInvalid "Failed to parse 'TABOAO DA SERRA' as float".
    df = stringify_cols(df, FORCE_STRING + CATEGORICAL)
    df = stringify_object_cols(df)
    df = optimize_dtypes(df, categorical_cols=CATEGORICAL)
    return df


def ingest_file(path: Path, out_dir: Path) -> int:
    log.info("Ingerindo %s", path.name)
    total = 0
    for chunk in read_xlsx_streaming(path, chunksize=50_000):
        t = transform_chunk(chunk)
        if t.empty:
            continue
        t.to_parquet(
            out_dir,
            index=False,
            partition_cols=["ANO", "MES"],
            engine="pyarrow",
        )
        total += len(t)
        log.info("  ↳ %s linhas acumuladas neste arquivo", f"{total:,}")
    return total


def main() -> None:
    setup_logging()
    out = PROCESSED / "sp_dados_criminais"
    out.mkdir(parents=True, exist_ok=True)

    files = sorted(RAW_SSP.glob("SPDadosCriminais_*.xlsx"))
    if not files:
        log.error("Nenhum SPDadosCriminais_*.xlsx em %s", RAW_SSP)
        return

    grand = 0
    for p in files:
        grand += ingest_file(p, out)
    log.info("OK. %s registros escritos em %s", f"{grand:,}", out)


if __name__ == "__main__":
    main()
