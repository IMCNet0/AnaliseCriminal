"""Agrega datasets de Subtraídos (Celulares, Veículos, Objetos).

Gera em data/aggregates/:
  por_celulares.parquet
    ANO · MES · RUBRICA · MARCA_OBJETO · DESCR_TIPOLOCAL · DESCR_PERIODO
    · FLAG_BLOQUEIO · DpGeoCod · N

  por_veiculos.parquet
    ANO · MES · RUBRICA · DESCR_MODO_OBJETO · DESCR_TIPO_OBJETO
    · MARCA_OBJETO · DESCR_TIPOLOCAL · DESCR_PERIODO · FLAG_STATUS
    · DpGeoCod · N

  por_objetos.parquet
    ANO · MES · RUBRICA · DESCR_TIPO_OBJETO · DESCR_SUBTIPO_OBJETO
    · DESCR_TIPOLOCAL · DESCR_PERIODO · DpGeoCod · N
    (DESCR_SUBTIPO_OBJETO: top-50 mais frequentes; demais → "OUTROS")

Rode DEPOIS de pipeline/aggregate.py (depende de DP.json já existir).
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from common import PROCESSED, AGGREGATES, setup_logging

log = logging.getLogger(__name__)

SP_ALIASES = {"SAO PAULO", "S.PAULO", "S. PAULO", "SAO PAULO-SP"}
DP_JSON = Path(__file__).resolve().parent.parent / "data" / "geo" / "DP.json"

# Top-N subtipos de objeto a manter individualmente; demais → "OUTROS"
TOP_SUBTIPOS = 50


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm_str(s: pd.Series) -> pd.Series:
    """unidecode + upper + colapsa espaços (mesma lógica do pipeline principal)."""
    from unidecode import unidecode
    return (
        s.astype("string")
        .map(lambda x: None if pd.isna(x) else " ".join(unidecode(str(x)).upper().split()))
        .astype("string")
    )


def load_dataset(name: str, want: list[str]) -> pd.DataFrame:
    """Lê dataset partição a partição, filtrando SP-Capital na carga."""
    path = PROCESSED / name
    if not path.exists():
        raise FileNotFoundError(f"Dataset não encontrado: {path}")

    log.info("Lendo %s …", name)
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
                    cols = [c for c in want if c in avail]
                    chunk = pf.read(columns=cols).to_pandas()
                    chunk["ANO"] = ano
                    chunk["MES"] = mes
                    # Filtro SP-Capital
                    if "NOME_MUNICIPIO" in chunk.columns:
                        nm = chunk["NOME_MUNICIPIO"].astype("string").str.upper().str.strip()
                        chunk = chunk[nm.isin(SP_ALIASES)]
                    if not chunk.empty:
                        frames.append(chunk)
                except Exception as exc:
                    log.warning("Ignorando %s: %s", pfile.name, exc)
                    skipped += 1

    if skipped:
        log.warning("  %s arquivo(s) ignorado(s) por erro", skipped)
    if not frames:
        raise RuntimeError(f"Nenhum dado lido de {path}")

    df = pd.concat(frames, ignore_index=True)
    df["ANO"] = df["ANO"].astype("Int16")
    df["MES"] = df["MES"].astype("Int8")
    log.info("  %s linhas SP-Capital carregadas", f"{len(df):,}")
    return df


def assign_dp(df: pd.DataFrame) -> pd.DataFrame:
    """Atribui DpGeoCod via spatial join ponto-em-polígono (mesmo padrão de
    aggregate_hora_dia.py). Linhas sem coord válida ficam com DpGeoCod=<NA>."""
    if not DP_JSON.exists():
        log.warning("DP.json não encontrado — DpGeoCod ausente no agregado")
        df["DpGeoCod"] = pd.NA
        return df

    import geopandas as gpd

    valid_mask = (
        df.get("COORDS_VALIDAS", pd.Series(False, index=df.index))
        .fillna(False).astype(bool)
        & df["LATITUDE"].notna()
        & df["LONGITUDE"].notna()
    )
    df_v = df.loc[valid_mask].copy().reset_index(drop=True)
    log.info("  Spatial join: %s pts válidos de %s totais",
             f"{len(df_v):,}", f"{len(df):,}")

    gdf_dp = gpd.read_file(str(DP_JSON))
    gdf_dp = gdf_dp.to_crs("EPSG:4326") if gdf_dp.crs else gdf_dp.set_crs("EPSG:4326")
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

    df = df.copy()
    df["DpGeoCod"] = pd.NA
    df.loc[valid_mask, "DpGeoCod"] = joined["DpGeoCod"].values
    log.info("  DpGeoCod atribuído a %s/%s linhas",
             f"{df['DpGeoCod'].notna().sum():,}", f"{len(df):,}")
    return df


def _groupby_count(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    present = [c for c in cols if c in df.columns]
    return (
        df.groupby(present, observed=True, dropna=False)
        .size().reset_index(name="N")
        .assign(N=lambda d: d["N"].astype("Int32"))
    )


def build_bairro(df: pd.DataFrame, fonte: str) -> pd.DataFrame:
    """Agrega por logradouro+bairro sem spatial join.

    Colunas resultado: FONTE · ANO · MES · LOGRADOURO · BAIRRO · RUBRICA · N
    """
    df2 = df.copy()
    df2["FONTE"] = fonte
    if "RUBRICA" in df2.columns:
        df2["RUBRICA"] = _norm_str(df2["RUBRICA"])
    for col in ("LOGRADOURO", "BAIRRO"):
        if col in df2.columns:
            df2[col] = df2[col].astype("string").str.upper().str.strip()
    return _groupby_count(df2, ["FONTE", "ANO", "MES", "LOGRADOURO", "BAIRRO", "RUBRICA"])


# ---------------------------------------------------------------------------
# Build por dataset
# ---------------------------------------------------------------------------

def build_celulares(df: pd.DataFrame) -> pd.DataFrame:
    if "RUBRICA" in df.columns:
        df = df.copy()
        df["RUBRICA"] = _norm_str(df["RUBRICA"])
    if "FLAG_BLOQUEIO" in df.columns:
        df = df.copy()
        fb = df["FLAG_BLOQUEIO"].astype("string").str.upper().str.strip()
        df["FLAG_BLOQUEIO"] = fb.where(fb.isin({"S", "N"}), other=pd.NA)

    return _groupby_count(df, [
        "ANO", "MES", "RUBRICA", "MARCA_OBJETO",
        "DESCR_TIPOLOCAL", "DESCR_PERIODO", "FLAG_BLOQUEIO", "DpGeoCod",
    ])


def build_veiculos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ("RUBRICA", "DESCR_MODO_OBJETO", "FLAG_STATUS"):
        if col in df.columns:
            df[col] = _norm_str(df[col])

    return _groupby_count(df, [
        "ANO", "MES", "RUBRICA", "DESCR_MODO_OBJETO", "DESCR_TIPO_OBJETO",
        "MARCA_OBJETO", "DESCR_TIPOLOCAL", "DESCR_PERIODO", "FLAG_STATUS", "DpGeoCod",
    ])


def build_objetos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "RUBRICA" in df.columns:
        df["RUBRICA"] = _norm_str(df["RUBRICA"])

    # Mantém apenas os TOP_SUBTIPOS mais frequentes; demais → "OUTROS"
    if "DESCR_SUBTIPO_OBJETO" in df.columns:
        top = (
            df["DESCR_SUBTIPO_OBJETO"].astype("string")
            .value_counts().head(TOP_SUBTIPOS).index.tolist()
        )
        df["DESCR_SUBTIPO_OBJETO"] = (
            df["DESCR_SUBTIPO_OBJETO"].astype("string")
            .where(df["DESCR_SUBTIPO_OBJETO"].isin(top), other="OUTROS")
        )

    return _groupby_count(df, [
        "ANO", "MES", "RUBRICA", "DESCR_TIPO_OBJETO", "DESCR_SUBTIPO_OBJETO",
        "DESCR_TIPOLOCAL", "DESCR_PERIODO", "DpGeoCod",
    ])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    setup_logging()
    AGGREGATES.mkdir(parents=True, exist_ok=True)

    # Colunas geo — comuns aos 3 datasets
    GEO = ["NOME_MUNICIPIO", "LATITUDE", "LONGITUDE", "COORDS_VALIDAS"]
    bairro_frames: list[pd.DataFrame] = []

    # --- Celulares ---
    df_cel = load_dataset("celulares_subtraidos", GEO + [
        "LOGRADOURO", "BAIRRO", "RUBRICA", "MARCA_OBJETO", "DESCR_TIPOLOCAL", "DESCR_PERIODO", "FLAG_BLOQUEIO",
    ])
    bairro_frames.append(build_bairro(df_cel, "CELULARES"))
    df_cel = assign_dp(df_cel)
    agg_cel = build_celulares(df_cel)
    del df_cel
    out = AGGREGATES / "por_celulares.parquet"
    agg_cel.to_parquet(out, index=False, engine="pyarrow")
    log.info("✓ %s  (%s linhas)", out.name, f"{len(agg_cel):,}")

    # --- Veículos ---
    df_vei = load_dataset("veiculos_subtraidos", GEO + [
        "LOGRADOURO", "BAIRRO", "RUBRICA", "DESCR_MODO_OBJETO", "DESCR_TIPO_OBJETO", "MARCA_OBJETO",
        "DESCR_TIPOLOCAL", "DESCR_PERIODO", "FLAG_STATUS",
    ])
    bairro_frames.append(build_bairro(df_vei, "VEICULOS"))
    df_vei = assign_dp(df_vei)
    agg_vei = build_veiculos(df_vei)
    del df_vei
    out = AGGREGATES / "por_veiculos.parquet"
    agg_vei.to_parquet(out, index=False, engine="pyarrow")
    log.info("✓ %s  (%s linhas)", out.name, f"{len(agg_vei):,}")

    # --- Objetos ---
    df_obj = load_dataset("objetos_subtraidos", GEO + [
        "LOGRADOURO", "BAIRRO", "RUBRICA", "DESCR_TIPO_OBJETO", "DESCR_SUBTIPO_OBJETO",
        "DESCR_TIPOLOCAL", "DESCR_PERIODO",
    ])
    bairro_frames.append(build_bairro(df_obj, "OBJETOS"))
    df_obj = assign_dp(df_obj)
    agg_obj = build_objetos(df_obj)
    del df_obj
    out = AGGREGATES / "por_objetos.parquet"
    agg_obj.to_parquet(out, index=False, engine="pyarrow")
    log.info("✓ %s  (%s linhas)", out.name, f"{len(agg_obj):,}")

    # --- Bairros (consolidado) ---
    agg_bairro = pd.concat(bairro_frames, ignore_index=True)
    out = AGGREGATES / "por_bairro_subtraidos.parquet"
    agg_bairro.to_parquet(out, index=False, engine="pyarrow")
    log.info("✓ %s  (%s linhas)", out.name, f"{len(agg_bairro):,}")


if __name__ == "__main__":
    main()
