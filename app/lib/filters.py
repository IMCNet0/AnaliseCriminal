"""Filtros globais exibidos na sidebar de todas as páginas."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd
import streamlit as st

from . import data


RECORTES = [
    "Município",
    "Setor Censitário",
    "Batalhão PMESP",
    "Companhia PMESP",
    "Comando (CPA)",
    "Delegacia (DP)",
]


@dataclass
class GlobalFilters:
    ano_ini: int
    ano_fim: int
    naturezas: list[str]
    recorte: str

    def mask_date(self, df: pd.DataFrame) -> pd.Series:
        return df["ANO"].between(self.ano_ini, self.ano_fim)

    def mask_natureza(self, df: pd.DataFrame) -> pd.Series:
        if not self.naturezas:
            return pd.Series(True, index=df.index)
        return df["NATUREZA_APURADA"].isin(self.naturezas)


def sidebar_filters(default_naturezas: Optional[list[str]] = None) -> GlobalFilters:
    st.sidebar.markdown("## Filtros")

    anos = data.anos_disponiveis()
    if anos:
        ano_ini, ano_fim = st.sidebar.select_slider(
            "Período (ano)", options=anos, value=(min(anos), max(anos)),
        )
    else:
        ano_ini, ano_fim = 2022, 2026
        st.sidebar.warning("Rodar `python pipeline/run_all.py` para gerar agregados.")

    naturezas = data.naturezas_disponiveis()
    sel_nat = st.sidebar.multiselect(
        "Naturezas (vazio = todas)", naturezas, default=default_naturezas or [],
        help="Escolha uma ou mais naturezas apuradas. Vazio considera todas.",
    )

    recorte = st.sidebar.radio(
        "Recorte geográfico", RECORTES, index=0,
        help="Unidade de análise geográfica. Todos os recortes são obtidos via "
             "point-in-polygon das coordenadas dos BOs.",
    )

    st.sidebar.markdown("---")
    st.sidebar.caption("Fonte: SSP-SP · IBGE · PMESP")
    st.sidebar.caption("InsightGeoLab AI")

    return GlobalFilters(
        ano_ini=int(ano_ini), ano_fim=int(ano_fim),
        naturezas=sel_nat, recorte=recorte,
    )
