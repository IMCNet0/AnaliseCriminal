"""Gera uma amostra do dataset de pontos pra rodar no Streamlit Cloud.

``data/processed/sp_dados_criminais/`` tem ~5M linhas e pesa ~600 MB. Isso é
inviável no repo (>100 MB por arquivo quebra o GitHub, e 600 MB no cold start
do Community Cloud dá delay ruim). Este script produz
``data/processed_sample/sp_dados_criminais/`` particionado igual, com no
máximo ``--max-per-partition`` pontos válidos por ANO×MES, mantendo só as
colunas que o ``app/lib/data.py::pontos()`` consome.

Tamanho típico da saída: ≲ 50 MB (5k pontos × 50 meses × ~200 B/linha).

Também aplica a mesma normalização de natureza usada no ``aggregate.py``
(unidecode + upper) pra casar com a lista exibida na UI sem ter que
normalizar em runtime.

Uso:
    python pipeline/build_sample.py
    python pipeline/build_sample.py --max-per-partition 3000  # mais leve
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from common import PROCESSED, setup_logging

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
SAMPLE = ROOT / "data" / "processed_sample" / "sp_dados_criminais"

KEEP_COLS = [
    "LATITUDE", "LONGITUDE",
    "NATUREZA_APURADA",
    "NOME_MUNICIPIO",
    "DATA_OCORRENCIA_BO",
    "COORDS_VALIDAS",
]

DP_JSON = ROOT / "data" / "geo" / "DP.json"


def _build_dp_gdf():
    """Carrega DP.json como GeoDataFrame (mantém só DpGeoCod + geometry)."""
    import geopandas as gpd
    if not DP_JSON.exists():
        return None
    gdf = gpd.read_file(str(DP_JSON))
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs("EPSG:4326")
    return gdf[["DpGeoCod", "geometry"]].copy()


def _assign_dp(df: pd.DataFrame, gdf_dp) -> pd.DataFrame:
    """Spatial join ponto-em-polígono para atribuir DpGeoCod a cada amostra."""
    import geopandas as gpd
    pts = gpd.GeoDataFrame(
        df.reset_index(drop=True),
        geometry=gpd.points_from_xy(df["LONGITUDE"], df["LATITUDE"]),
        crs="EPSG:4326",
    )
    joined = gpd.sjoin(pts, gdf_dp, how="left", predicate="within")
    if joined.index.has_duplicates:
        joined = joined[~joined.index.duplicated(keep="first")]
    df = joined.drop(columns=["geometry", "index_right"], errors="ignore")
    # Normaliza: float 10102.0 → string "10102"
    if "DpGeoCod" in df.columns:
        nums = pd.to_numeric(df["DpGeoCod"], errors="coerce")
        valid = nums.dropna()
        if not valid.empty and (valid == valid.astype("int64")).all():
            df["DpGeoCod"] = nums.astype("Int64").astype("string")
        else:
            df["DpGeoCod"] = df["DpGeoCod"].astype("string").str.strip()
    return df


def norm_natureza(series: pd.Series) -> pd.Series:
    from unidecode import unidecode
    s = series.astype("string")
    return s.map(
        lambda x: None if pd.isna(x) else " ".join(unidecode(str(x)).upper().split())
    ).astype("string")


def read_partition(ano: int, mes: int, cols: list[str]) -> pd.DataFrame:
    """Lê uma partição específica (ANO=x/MES=y) da base completa."""
    partitioning = ds.partitioning(
        pa.schema([("ANO", pa.int32()), ("MES", pa.int32())]),
        flavor="hive",
    )
    dataset = ds.dataset(
        str(PROCESSED / "sp_dados_criminais"),
        format="parquet",
        partitioning=partitioning,
    )
    flt = (ds.field("ANO") == ano) & (ds.field("MES") == mes)
    return dataset.to_table(columns=cols, filter=flt).to_pandas()


def list_partitions() -> list[tuple[int, int]]:
    base = PROCESSED / "sp_dados_criminais"
    out: list[tuple[int, int]] = []
    for ano_dir in sorted(base.glob("ANO=*")):
        try:
            ano = int(ano_dir.name.split("=", 1)[1])
        except ValueError:
            continue
        for mes_dir in sorted(ano_dir.glob("MES=*")):
            try:
                mes = int(mes_dir.name.split("=", 1)[1])
            except ValueError:
                continue
            out.append((ano, mes))
    return out


def main() -> int:
    setup_logging()
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--max-per-partition", type=int, default=5000,
                    help="Máximo de pontos válidos por ANO×MES (default: 5000)")
    ap.add_argument("--seed", type=int, default=42,
                    help="Seed do random para reprodutibilidade")
    args = ap.parse_args()

    base = PROCESSED / "sp_dados_criminais"
    if not base.exists():
        log.error("Base completa não existe: %s", base)
        return 1

    SAMPLE.mkdir(parents=True, exist_ok=True)
    parts = list_partitions()
    if not parts:
        log.error("Nenhuma partição encontrada em %s", base)
        return 1

    gdf_dp = _build_dp_gdf()
    if gdf_dp is None:
        log.warning("DP.json não encontrado — DpGeoCod não será incluído na amostra")

    rng = np.random.default_rng(args.seed)
    total_in = 0
    total_out = 0

    log.info("Gerando amostra com até %s pontos/partição em %s",
             f"{args.max_per_partition:,}", SAMPLE)
    for i, (ano, mes) in enumerate(parts, 1):
        df = read_partition(ano, mes, KEEP_COLS)
        total_in += len(df)
        # Mantém só pontos com coord válida (o app filtra isso de qualquer forma)
        df = df[df["COORDS_VALIDAS"].fillna(False).astype(bool)]
        df = df.dropna(subset=["LATITUDE", "LONGITUDE"])
        if len(df) > args.max_per_partition:
            idx = rng.choice(len(df), size=args.max_per_partition, replace=False)
            df = df.iloc[idx].copy()

        if df.empty:
            continue

        # Normaliza natureza agora pra casar com os agregados sem runtime overhead
        df["NATUREZA_APURADA"] = norm_natureza(df["NATUREZA_APURADA"])

        # Atribui DpGeoCod via spatial join para permitir filtro por delegacia na UI
        if gdf_dp is not None:
            df = _assign_dp(df, gdf_dp)

        out_dir = SAMPLE / f"ANO={ano}" / f"MES={mes}"
        out_dir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, out_dir / "part.parquet", compression="snappy")
        total_out += len(df)
        if i % 10 == 0 or i == len(parts):
            log.info("  %d/%d  (ANO=%d MES=%02d  %s→%s)",
                     i, len(parts), ano, mes,
                     f"{total_in:,}", f"{total_out:,}")

    # Tamanho final em disco
    size_bytes = sum(p.stat().st_size for p in SAMPLE.rglob("*.parquet"))
    log.info("")
    log.info("Amostra gerada: %s linhas (de %s originais)",
             f"{total_out:,}", f"{total_in:,}")
    log.info("Tamanho em disco: %.1f MB em %s",
             size_bytes / (1024 * 1024), SAMPLE)
    log.info("Tamanho por ponto médio: %.0f bytes", size_bytes / max(total_out, 1))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
