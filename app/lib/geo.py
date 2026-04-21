"""Utilitários geoespaciais — camadas da InsightGeoLab AI.

Usa os GeoJSON fornecidos em `data/geo/`:

  BTL_PMESP.json    → OPM (batalhão)
  CIA_PMESP.json    → OPM (companhia); traz populacao/qtd_domic/area_km2
  CMDO_PMESP.json   → cmdo_label
  DP.json           → DpGeoCod / DpGeoDes
  CENSO_simplified.parquet  → setor censitário (gerado a partir do CENSO.json)
"""
from __future__ import annotations

from pathlib import Path
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
GEO = ROOT / "data" / "geo"


LAYERS = {
    # rótulo UI         arquivo                     coluna-chave no shapefile
    "Município":        ("municipios_sp.geojson",   "CD_MUN"),
    "Setor Censitário": ("CENSO_simplified.parquet","sc_cod"),
    "Batalhão PMESP":   ("BTL_PMESP.json",          "OPM"),
    # CIA_PMESP: OPMCOD é o ID único (360 valores); OPM sozinho tem só 8
    # porque a numeração da CIA é interna ao batalhão.
    "Companhia PMESP":  ("CIA_PMESP.json",          "OPMCOD"),
    "Comando (CPA)":    ("CMDO_PMESP.json",         "cmdo_label"),
    "Delegacia (DP)":   ("DP.json",                 "DpGeoCod"),
}

# Coluna equivalente no dataset agregado (após sjoin / join por COD_IBGE)
JOIN_KEYS_IN_DATA = {
    "Município":        "COD_IBGE",
    "Setor Censitário": "sc_cod",
    "Batalhão PMESP":   "OPM_BTL",
    "Companhia PMESP":  "OPMCOD_CIA",
    "Comando (CPA)":    "CMDO",
    "Delegacia (DP)":   "DpGeoCod",
}


@st.cache_resource(show_spinner="Carregando camada geográfica…")
def load_layer(recorte: str):
    import geopandas as gpd
    entry = LAYERS.get(recorte)
    if not entry:
        return None, None
    fname, key = entry
    path = GEO / fname
    if not path.exists():
        return None, None
    if path.suffix == ".parquet":
        gdf = gpd.read_parquet(path)
    else:
        gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs("EPSG:4326")
    # Simplificação leve — balanço entre fidelidade e peso no wire
    gdf["geometry"] = gdf.geometry.simplify(tolerance=0.0002, preserve_topology=True)
    return gdf, key
