"""Converte e enxuga o CENSO.json (401 MB, 102k setores) para um parquet leve.

Por quê: carregar o geojson cru no Streamlit estoura o limite de 1GB de RAM
do Streamlit Community Cloud. Este script:

  1. Mantém apenas SP (CD_UF == '35').
  2. Reduz colunas às estritamente necessárias para o portal.
  3. Simplifica geometrias (tolerância 0.0001° ≈ 11 m).
  4. Escreve em geoparquet (~10x menor e leitura muito mais rápida).

Rodar uma vez após receber/atualizar o CENSO.json.
"""
from __future__ import annotations

from pathlib import Path
import logging

from common import setup_logging

log = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent.parent
GEO = ROOT / "data" / "geo"

KEEP = [
    "sc_cod",
    "CD_MUN", "NM_MUN",
    "CD_DIST", "NM_DIST",
    "pop_fem", "pop_masc",
    "favela_2022",
    "BTL", "CMDO", "GDO_CMDO", "OPM",
    "AREA_KM2",
    "geometry",
]


def main() -> None:
    setup_logging()
    import geopandas as gpd

    src = GEO / "CENSO.json"
    dst = GEO / "CENSO_simplified.parquet"

    if not src.exists():
        log.error("CENSO.json não encontrado em %s", src)
        return

    log.info("Lendo %s (pode demorar…)", src.name)
    gdf = gpd.read_file(src)
    log.info("  %s features carregadas", f"{len(gdf):,}")

    if "CD_UF" in gdf.columns:
        before = len(gdf)
        gdf = gdf[gdf["CD_UF"].astype(str) == "35"]
        log.info("  SP filtrado: %s → %s", f"{before:,}", f"{len(gdf):,}")

    keep_existing = [c for c in KEEP if c in gdf.columns]
    gdf = gdf[keep_existing]

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs("EPSG:4326")

    log.info("Simplificando geometrias…")
    gdf["geometry"] = gdf.geometry.simplify(tolerance=0.0001, preserve_topology=True)

    log.info("Escrevendo %s", dst.name)
    gdf.to_parquet(dst, index=False)
    log.info("Tamanho final: %.1f MB", dst.stat().st_size / 1e6)


if __name__ == "__main__":
    main()
