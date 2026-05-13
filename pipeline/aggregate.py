"""Gera tabelas agregadas que alimentam o Streamlit.

A abordagem agora é **spatial-first**: como cada BO tem LAT/LONG, não casamos
strings de BTL/CIA/CMDO da SSP com o polígono — fazemos point-in-polygon contra
cada camada geojson da PMESP. Isso:

  1. Evita problemas de grafia (ex.: "7ºBPM/M" vs "07º BPM/M").
  2. Funciona mesmo em BOs cuja coluna BTL/CIA veio vazia.
  3. Garante consistência: uma só fonte da verdade (a geometria).

Saída em data/aggregates/:
  por_municipio.parquet   (ano, mes, natureza, CD_MUN, NM_MUN, N)
  por_batalhao.parquet    (ano, mes, natureza, OPM_BTL, cmdo_BTL, N)
  por_companhia.parquet   (ano, mes, natureza, OPM_CIA, btl_CIA, populacao_CIA, N)
  por_comando.parquet     (ano, mes, natureza, CMDO, regiao, N)
  por_dp.parquet          (ano, mes, natureza, DpGeoCod, DpGeoDes, N)
  por_setor.parquet       (ano, mes, natureza, sc_cod, pop_total, favela_2022, N)
  serie_estado.parquet    (ano, mes, natureza, N)
  cubo_natureza.parquet   (natureza, N)
  cubo_conduta.parquet    (DESCR_CONDUTA, N)
"""
from __future__ import annotations

from pathlib import Path
import logging

import pandas as pd
import numpy as np

from common import PROCESSED, AGGREGATES, setup_logging

log = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent.parent
GEO = ROOT / "data" / "geo"

LAYERS = {
    "batalhao":  {"file": "BTL_PMESP.json",   "keep": ["OPM", "cmdo", "g_cmdo"],
                  "rename": {"OPM": "OPM_BTL", "cmdo": "cmdo_BTL", "g_cmdo": "g_cmdo_BTL"}},
    "companhia": {"file": "CIA_PMESP.json",
                  # OPMCOD é ID único por polígono (360 valores distintos).
                  # "OPM" sozinho é ambíguo ("1ªCIA" existe em cada batalhão).
                  "keep": ["OPMCOD", "OPM", "btl", "cmdo", "populacao", "qtd_domic", "area_km2"],
                  "rename": {"OPMCOD": "OPMCOD_CIA", "OPM": "OPM_CIA", "btl": "btl_CIA",
                             "cmdo": "cmdo_CIA",
                             "populacao": "populacao_CIA", "qtd_domic": "qtd_domic_CIA",
                             "area_km2": "area_km2_CIA"}},
    "comando":   {"file": "CMDO_PMESP.json",  "keep": ["cmdo_label", "gdo_cmdo", "regiao", "DEINTER"],
                  "rename": {"cmdo_label": "CMDO"}},
    "dp":        {"file": "DP.json",          "keep": ["DpGeoCod", "DpGeoDes", "SecGeoCod"],
                  "rename": {}},
    "setor":     {"file": "CENSO_simplified.parquet",
                  "keep": ["sc_cod", "CD_MUN", "NM_MUN", "pop_fem", "pop_masc", "favela_2022"],
                  "rename": {}},
}


def load_base() -> pd.DataFrame:
    """Lê o dataset particionado usando pyarrow.dataset com schema explícito.

    Por que não ``pd.read_parquet`` direto? Com ``ANO``/``MES`` apenas no
    nome do diretório (hive), o pyarrow os infere como
    ``dictionary<values=int32, indices=int32>`` e o pandas falha em converter
    pro ``Int16``/``Int8`` nullable declarado no metadata com
    ``NotImplementedError: dictionary<values=int32, indices=int32, ordered=0>``.

    Forçamos o partitioning com schema explícito (int32) — sem dicionário —
    e depois castamos pra Int nullable, mantendo consistência com o resto
    do pipeline.
    """
    import pyarrow.dataset as ds
    import pyarrow as pa

    path = PROCESSED / "sp_dados_criminais"
    log.info("Lendo base agregada: %s", path)
    cols = [
        "ANO", "MES", "NATUREZA_APURADA", "NOME_MUNICIPIO", "COD_IBGE",
        "LATITUDE", "LONGITUDE", "COORDS_VALIDAS", "DESCR_CONDUTA",
    ]
    partitioning = ds.partitioning(
        pa.schema([("ANO", pa.int32()), ("MES", pa.int32())]),
        flavor="hive",
    )
    dataset = ds.dataset(str(path), format="parquet", partitioning=partitioning)
    table = dataset.to_table(columns=cols)
    df = table.to_pandas()
    # Alinha com o resto do pipeline (ingest grava ANO/MES como Int16/Int8 nullable)
    if "ANO" in df.columns:
        df["ANO"] = df["ANO"].astype("Int16")
    if "MES" in df.columns:
        df["MES"] = df["MES"].astype("Int8")
    log.info("  %s linhas carregadas", f"{len(df):,}")
    return df


def load_layer(name: str):
    import geopandas as gpd
    meta = LAYERS[name]
    path = GEO / meta["file"]
    if not path.exists():
        log.warning("Camada %s não encontrada: %s", name, path)
        return None, meta
    if path.suffix == ".parquet":
        gdf = gpd.read_parquet(path)
    else:
        gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs("EPSG:4326")
    # Mantém só as colunas úteis + geometry
    keep = [c for c in meta["keep"] if c in gdf.columns] + ["geometry"]
    gdf = gdf[keep].rename(columns=meta["rename"])
    return gdf, meta


def sjoin_layer(df_points, gdf_layer):
    """Anexa atributos da camada a cada ponto via point-in-polygon.

    Deduplica múltiplas correspondências por ponto (gpd.sjoin duplica o ponto
    quando ele cai em 2+ polígonos). Causas reais observadas:
      - BTL_PMESP tem 181 pares de polígonos com overlap topológico
      - CMDO_PMESP tem sobreposição grande em regiões como Grande SP
    Sem a deduplicação, por_batalhao somava +12k registros acima dos pontos
    válidos e por_comando somava +520k. Mantemos a primeira atribuição por
    ponto (determinística via reset_index) — consistente com a convenção de
    "BO pertence a uma única jurisdição" para fins de ranking.
    """
    import geopandas as gpd
    # Garante índice único por ponto antes do sjoin (o reset_index preserva
    # a ordem das linhas de entrada e dá uma chave limpa para o dedup).
    df_points = df_points.reset_index(drop=True)
    gdf_pts = gpd.GeoDataFrame(
        df_points, geometry=gpd.points_from_xy(df_points["LONGITUDE"], df_points["LATITUDE"]),
        crs="EPSG:4326",
    )
    joined = gpd.sjoin(gdf_pts, gdf_layer, how="left", predicate="within")
    # gpd.sjoin preserva o índice do lado esquerdo; duplicatas = mesmo ponto
    # caiu em múltiplos polígonos.
    if joined.index.has_duplicates:
        n_dup = int(joined.index.duplicated(keep="first").sum())
        log.info("  · %s correspondências duplicadas removidas (ponto em múltiplos polígonos)", f"{n_dup:,}")
        joined = joined[~joined.index.duplicated(keep="first")]
    return joined.drop(columns=["geometry", "index_right"], errors="ignore")


def agg_and_save(df: pd.DataFrame, by: list[str], out: Path, extra_cols: list[str] | None = None) -> None:
    extra_cols = extra_cols or []
    grp = (
        df.groupby(by + extra_cols, observed=True, dropna=False)
        .size().reset_index(name="N")
    )
    grp.to_parquet(out, index=False, engine="pyarrow")
    log.info("  → %s (%s linhas)", out.name, f"{len(grp):,}")


def normalize_natureza(series: pd.Series) -> pd.Series:
    """Canoniza ``NATUREZA_APURADA``: remove acento, upper, colapsa espaços.

    A SSP publicou as mesmas naturezas com grafias diferentes entre anos
    (com e sem acento). Sem normalizar, o Top N trata ``TRÁFICO DE
    ENTORPECENTES`` e ``TRAFICO DE ENTORPECENTES`` como categorias
    distintas e subestima o total real de cada uma. Idem HOMICÍDIO, etc.
    """
    from unidecode import unidecode
    # Mantém nulos como estão; aplica só onde houver string
    s = series.astype("string")
    s = s.map(lambda x: None if pd.isna(x) else " ".join(unidecode(str(x)).upper().split()))
    return s.astype("string")


def main() -> None:
    setup_logging()
    AGGREGATES.mkdir(parents=True, exist_ok=True)

    df = load_base()

    # Normaliza natureza ANTES de qualquer groupby — consolida variações de
    # acento/espaço que a SSP publicou entre anos diferentes.
    before = df["NATUREZA_APURADA"].nunique(dropna=True)
    df["NATUREZA_APURADA"] = normalize_natureza(df["NATUREZA_APURADA"])
    after = df["NATUREZA_APURADA"].nunique(dropna=True)
    log.info("Naturezas: %d → %d categorias (após normalização)", before, after)

    # ---- 1. Série estadual e cubos dimensionais (não dependem de geo) ----
    log.info("Agregando série estadual")
    agg_and_save(df, ["ANO", "MES", "NATUREZA_APURADA"], AGGREGATES / "serie_estado.parquet")
    log.info("Cubo de naturezas")
    agg_and_save(df, ["NATUREZA_APURADA"], AGGREGATES / "cubo_natureza.parquet")

    log.info("Cubo de condutas")
    if "DESCR_CONDUTA" in df.columns:
        df["DESCR_CONDUTA"] = normalize_natureza(df["DESCR_CONDUTA"])
        agg_and_save(df, ["DESCR_CONDUTA"], AGGREGATES / "cubo_conduta.parquet")
    else:
        log.warning("DESCR_CONDUTA não encontrada na base — cubo_conduta.parquet não gerado")

    # ---- 2. Por município (já vem COD_IBGE/NOME_MUNICIPIO no dado) ----
    log.info("Agregando por município")
    agg_and_save(
        df.rename(columns={"COD_IBGE": "CD_MUN", "NOME_MUNICIPIO": "NM_MUN"}),
        ["ANO", "MES", "NATUREZA_APURADA", "CD_MUN", "NM_MUN"],
        AGGREGATES / "por_municipio.parquet",
    )

    # ---- 3. Camadas PMESP + DP + setor (via sjoin) ----
    pts = df[df["COORDS_VALIDAS"]].copy()
    log.info("Pontos com coordenadas válidas para sjoin: %s", f"{len(pts):,}")

    for name in ["batalhao", "companhia", "comando", "dp", "setor"]:
        log.info("sjoin → %s", name)
        gdf, meta = load_layer(name)
        if gdf is None:
            continue
        joined = sjoin_layer(pts, gdf)
        # colunas-chave pós-rename
        if name == "batalhao":
            by = ["ANO", "MES", "NATUREZA_APURADA", "OPM_BTL", "cmdo_BTL"]
            out = AGGREGATES / "por_batalhao.parquet"
        elif name == "companhia":
            # OPMCOD_CIA é a chave única (360 CIAs distintas); OPM_CIA e btl_CIA
            # ficam na saída como rótulos legíveis pra UI/relatórios.
            by = ["ANO", "MES", "NATUREZA_APURADA",
                  "OPMCOD_CIA", "OPM_CIA", "btl_CIA", "populacao_CIA"]
            out = AGGREGATES / "por_companhia.parquet"
        elif name == "comando":
            by = ["ANO", "MES", "NATUREZA_APURADA", "CMDO", "regiao"]
            out = AGGREGATES / "por_comando.parquet"
        elif name == "dp":
            by = ["ANO", "MES", "NATUREZA_APURADA", "DpGeoCod", "DpGeoDes"]
            out = AGGREGATES / "por_dp.parquet"
        else:  # setor
            # Agrega população por setor (soma pop_fem + pop_masc)
            pop = None
            if {"pop_fem", "pop_masc"}.issubset(joined.columns):
                joined["pop_total"] = joined["pop_fem"].fillna(0) + joined["pop_masc"].fillna(0)
            by = ["ANO", "MES", "NATUREZA_APURADA", "sc_cod", "CD_MUN", "favela_2022"]
            out = AGGREGATES / "por_setor.parquet"
        # Remove colunas que não estão no joined (caso o geojson não tenha todas)
        by = [c for c in by if c in joined.columns]
        agg_and_save(joined, by, out)

    log.info("OK. Arquivos em %s", AGGREGATES)


if __name__ == "__main__":
    main()
