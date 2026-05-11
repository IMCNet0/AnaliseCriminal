"""Agregado auxiliar: matriz temática Dia da Semana × Faixa Hora × DESC_PERIODO.

Gera ``data/aggregates/matriz_hora_dia.parquet`` a partir do dataset bruto
particionado em ``data/processed/sp_dados_criminais``. Alimenta a página
"Séries Temporais" do portal (heatmap com dias em coluna / faixas em linha).

Chaves: ANO · MES · NATUREZA_APURADA · DIA_SEMANA · FAIXA_HORA · DESC_PERIODO · N

Faixas de hora (derivadas de ``HORA_OCORRENCIA_BO``, "HH:MM"):
  • 00:00              → rótulo "00:00"              (≈ 0,4% · geralmente falta
    de hora declarada pela vítima — fica isolado pra não poluir as demais)
  • 00:01 às 06:00     → rótulo "00:01–06:00"
  • 06:01 às 12:00     → rótulo "06:01–12:00"
  • 12:01 às 18:00     → rótulo "12:01–18:00"
  • 18:01 às 23:59     → rótulo "18:01–23:59"

O binning é feito em MINUTOS do dia (HH*60+MM) pra preservar exatamente
a regra "início exclusivo, fim inclusivo" de 6 em 6 horas.

Dia da semana extraído de ``DATA_OCORRENCIA_BO`` em formato "N.NOME" por
extenso pt-BR (ex.: ``1.DOMINGO``) — casa com o mock do cliente.

Rode DEPOIS de ``run_all.py`` — ele depende do dataset já ingerido.
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from common import PROCESSED, AGGREGATES, setup_logging

log = logging.getLogger(__name__)

# Ordem isoweek: 0=Seg … 6=Dom. Usamos rótulos "N.NOME" pt-BR com o DOMINGO
# numerado como 1 (convenção comercial do cliente), o que inverte a
# numeração natural do weekday do pandas. O mapa abaixo faz a conversão.
DIAS_PT = {
    0: "2.SEGUNDA-FEIRA",
    1: "3.TERÇA-FEIRA",
    2: "4.QUARTA-FEIRA",
    3: "5.QUINTA-FEIRA",
    4: "6.SEXTA-FEIRA",
    5: "7.SÁBADO",
    6: "1.DOMINGO",
}

# Ordem canônica pros eixos / pivots no app.
DIAS_ORDEM = [
    "1.DOMINGO", "2.SEGUNDA-FEIRA", "3.TERÇA-FEIRA",
    "4.QUARTA-FEIRA", "5.QUINTA-FEIRA", "6.SEXTA-FEIRA", "7.SÁBADO",
]
FAIXAS_ORDEM = [
    "00:00", "00:01–06:00", "06:01–12:00", "12:01–18:00", "18:01–23:59",
]


def _faixa_from_hora(s: pd.Series) -> pd.Series:
    """Converte ``HORA_OCORRENCIA_BO`` ("HH:MM" string) em faixa rotulada.

    Regra: convertemos para minutos desde meia-noite (0..1439) e aplicamos
    ``pd.cut`` com bordas ``[-1, 0, 360, 720, 1080, 1439]`` (right=True).
    A borda em 0 isola a categoria "00:00" das demais.

    Valores inválidos (None, fora de 0..1439, texto não-numérico) saem como
    ``NaN`` e são descartados no groupby.
    """
    s = s.astype("string").str.strip()
    # Extrai "HH" e "MM" independentemente (aceita "H:M", "HH:MM", "HH:MM:SS"…).
    parts = s.str.extract(r"^\s*(\d{1,2})\s*:\s*(\d{1,2})")
    hh = pd.to_numeric(parts[0], errors="coerce")
    mm = pd.to_numeric(parts[1], errors="coerce")
    # Validação: 0 ≤ HH ≤ 23, 0 ≤ MM ≤ 59
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


def _dia_from_data(s: pd.Series) -> pd.Series:
    """Extrai o dia da semana em formato "N.NOME" (pt-BR) da data."""
    d = pd.to_datetime(s, errors="coerce")
    wd = d.dt.weekday  # 0=Seg, 6=Dom
    return wd.map(DIAS_PT).astype("string")


DP_JSON = Path(__file__).resolve().parent.parent / "data" / "geo" / "DP.json"


def load_base() -> pd.DataFrame:
    """Lê só as colunas necessárias pra a matriz — mantém leve mesmo na base
    completa (~5M+ linhas).

    Lê partição a partição (ANO/MES) para tolerar arquivos com schema
    divergente (ex.: partições antigas sem DESC_PERIODO). Arquivos com
    erro de leitura são ignorados com aviso.
    """
    import pyarrow.parquet as pq

    path = PROCESSED / "sp_dados_criminais"
    log.info("Lendo dataset: %s", path)

    WANT = [
        "NATUREZA_APURADA", "DATA_OCORRENCIA_BO", "HORA_OCORRENCIA_BO",
        "DESC_PERIODO", "LATITUDE", "LONGITUDE", "COORDS_VALIDAS",
    ]

    frames: list[pd.DataFrame] = []
    skipped = 0
    for ano_dir in sorted(path.glob("ANO=*")):
        ano = int(ano_dir.name.split("=")[1])
        for mes_dir in sorted(ano_dir.glob("MES=*")):
            mes = int(mes_dir.name.split("=")[1])
            for pfile in sorted(mes_dir.rglob("*.parquet")):
                try:
                    pf = pq.ParquetFile(str(pfile))
                    avail = set(pf.schema_arrow.names)
                    cols = [c for c in WANT if c in avail]
                    chunk = pf.read(columns=cols).to_pandas()
                    chunk["ANO"] = ano
                    chunk["MES"] = mes
                    # DESC_PERIODO pode estar ausente em partições antigas
                    if "DESC_PERIODO" not in chunk.columns:
                        chunk["DESC_PERIODO"] = pd.NA
                    frames.append(chunk)
                except Exception as exc:
                    log.warning("Ignorando arquivo com erro (%s): %s", pfile.name, exc)
                    skipped += 1

    if skipped:
        log.warning("  %s arquivo(s) ignorado(s) por erro de leitura", skipped)
    if not frames:
        raise RuntimeError(f"Nenhum arquivo lido em {path}")

    df = pd.concat(frames, ignore_index=True)
    df["ANO"] = df["ANO"].astype("Int16")
    df["MES"] = df["MES"].astype("Int8")
    log.info("  %s linhas carregadas", f"{len(df):,}")
    return df


def _assign_dp(df: pd.DataFrame) -> pd.DataFrame:
    """Atribui DpGeoCod a cada linha via spatial join ponto-em-polígono.

    Linhas sem coordenada válida ou fora de qualquer polígono de DP recebem
    DpGeoCod=<NA> e continuam no agregado (aparecem ao filtrar por 'todos').
    """
    if not DP_JSON.exists():
        log.warning("DP.json não encontrado — DpGeoCod ausente na matriz")
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

    # Normaliza float "10102.0" → string "10102"
    if "DpGeoCod" in joined.columns:
        nums = pd.to_numeric(joined["DpGeoCod"], errors="coerce")
        valid_nums = nums.dropna()
        if not valid_nums.empty and (valid_nums == valid_nums.astype("int64")).all():
            joined["DpGeoCod"] = nums.astype("Int64").astype("string")
        else:
            joined["DpGeoCod"] = joined["DpGeoCod"].astype("string").str.strip()

    # Insere DpGeoCod de volta no df original (inválidos/fora ficam com <NA>)
    df = df.copy()
    df["DpGeoCod"] = pd.NA
    df.loc[valid_mask, "DpGeoCod"] = joined["DpGeoCod"].values
    log.info("  DpGeoCod atribuído a %s/%s linhas",
             f"{df['DpGeoCod'].notna().sum():,}", f"{len(df):,}")
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

    # Atribui DpGeoCod via spatial join (permite filtro por DP no app)
    df = _assign_dp(df)

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
            ["ANO", "MES", "NATUREZA_APURADA", "DpGeoCod",
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
