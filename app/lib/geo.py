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
import pandas as pd
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
    # DP.json traz DpGeoCod (ID numérico, 1039 valores) + DpGeoDes (descrição
    # tipo "001 DP SE SÃO PAULO"). A chave de MERGE é o código; o rótulo
    # visível (tooltip/dropdown sidebar) usa DpGeoDes.
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
    """Carrega o shapefile/geoparquet da camada e devolve ``(gdf, key_col)``.

    SP-Capital (rodada abr/26 #4):
      • Camada **Delegacia (DP)** é filtrada às DPs da Capital usando o
        ``SecGeoCod`` dentro de ``SP_CAPITAL_DP_SECCIONAIS``. Reduz 1039 →
        ~94 polígonos e mantém o coroplético/dropdown coerente com o
        recorte do data-layer.
      • Camada **Setor Censitário** é filtrada por ``CD_MUN`` == SP-Capital.
      • Demais camadas (PMESP) carregam estaduais — não usadas no recorte
        atual mas mantidas para regressão futura.
    """
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

    # ----------------------------------------------------------------------
    # Filtro SP-Capital: aplicado ANTES da simplificação para economizar
    # CPU em shapefiles grandes (CENSO_simplified tem ~250k polígonos).
    # ----------------------------------------------------------------------
    if recorte == "Delegacia (DP)" and "SecGeoCod" in gdf.columns:
        from .data import SP_CAPITAL_DP_SECCIONAIS
        sec = pd.to_numeric(gdf["SecGeoCod"], errors="coerce").astype("Int64")
        gdf = gdf[sec.isin(list(SP_CAPITAL_DP_SECCIONAIS))].copy()
    elif recorte == "Setor Censitário" and "CD_MUN" in gdf.columns:
        from .data import SP_CAPITAL_CD_MUN
        gdf = gdf[gdf["CD_MUN"].astype("string").str.strip() == SP_CAPITAL_CD_MUN].copy()

    # Simplificação leve — balanço entre fidelidade e peso no wire
    gdf["geometry"] = gdf.geometry.simplify(tolerance=0.0002, preserve_topology=True)
    return gdf, key
