"""Camada de acesso aos dados agregados (cache Streamlit).

Todas as leituras passam por @st.cache_data para não reprocessar a cada rerun.
Os agregados vivem em data/aggregates/ e são pequenos o bastante para rodar no
plano gratuito do Streamlit Community Cloud.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
AGG = ROOT / "data" / "aggregates"
# Runtime usa a amostra (data/processed_sample/) — leve o bastante pra subir
# no repo e rodar no Streamlit Community Cloud sem cold start pesado.
# A base completa (data/processed/, ~600 MB) fica fora do git e é usada só
# para regerar agregados/amostra com o pipeline local.
PROCESSED = ROOT / "data" / "processed_sample"
PROCESSED_FULL = ROOT / "data" / "processed"  # fallback local (dev)


def _safe_read(path: Path, **kwargs) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path, engine="pyarrow", **kwargs)


@st.cache_data(show_spinner="Carregando série estadual…", ttl=3600)
def serie_estado() -> pd.DataFrame:
    df = _safe_read(AGG / "serie_estado.parquet")
    if df.empty:
        return df
    df["DATA"] = pd.to_datetime(df["ANO"].astype(str) + "-" + df["MES"].astype(str) + "-01")
    return df


@st.cache_data(show_spinner="Carregando agregados por município…", ttl=3600)
def por_municipio(natureza: Optional[str] = None) -> pd.DataFrame:
    df = _safe_read(AGG / "por_municipio.parquet")
    if natureza and not df.empty:
        df = df[df["NATUREZA_APURADA"] == natureza]
    return df


@st.cache_data(ttl=3600)
def por_batalhao() -> pd.DataFrame:
    return _safe_read(AGG / "por_batalhao.parquet")


@st.cache_data(ttl=3600)
def por_companhia() -> pd.DataFrame:
    """Agregado por CIA. ``OPMCOD_CIA`` é a chave única (cada batalhão tem
    sua própria "1ªCIA", "2ªCIA" etc.; ``OPM_CIA`` sozinho é ambíguo).

    Cria coluna ``LABEL_CIA`` = ``"{OPM_CIA} / {btl_CIA}"`` pra UI.
    """
    df = _safe_read(AGG / "por_companhia.parquet")
    if df.empty:
        return df
    if {"OPM_CIA", "btl_CIA"}.issubset(df.columns):
        df["LABEL_CIA"] = (
            df["OPM_CIA"].astype("string").fillna("?")
            + " / "
            + df["btl_CIA"].astype("string").fillna("?")
        )
    return df


@st.cache_data(ttl=3600)
def por_comando() -> pd.DataFrame:
    return _safe_read(AGG / "por_comando.parquet")


@st.cache_data(ttl=3600)
def por_dp() -> pd.DataFrame:
    return _safe_read(AGG / "por_dp.parquet")


@st.cache_data(ttl=3600)
def por_setor() -> pd.DataFrame:
    return _safe_read(AGG / "por_setor.parquet")


@st.cache_data(show_spinner="Carregando matriz hora × dia…", ttl=3600)
def matriz_hora_dia() -> pd.DataFrame:
    """Agregado Dia da Semana × Faixa Hora × DESC_PERIODO.

    Gerado por ``pipeline/aggregate_hora_dia.py``. Se o arquivo não existir
    ainda (pipeline não rodou), devolve DataFrame vazio — a UI mostra um
    aviso orientando a rodar o agregador.
    """
    df = _safe_read(AGG / "matriz_hora_dia.parquet")
    if df.empty:
        return df
    # Reconstrói DATA (1º dia do mês) — permite reaproveitar f.mask_date() sem
    # precisar duplicar a lógica de ANO+MES na página.
    if {"ANO", "MES"}.issubset(df.columns):
        df["DATA"] = pd.to_datetime(
            df["ANO"].astype("Int64").astype("string") + "-"
            + df["MES"].astype("Int64").astype("string").str.zfill(2) + "-01",
            errors="coerce",
        )
    return df


@st.cache_data(ttl=3600)
def naturezas_disponiveis() -> list[str]:
    df = _safe_read(AGG / "cubo_natureza.parquet")
    if df.empty:
        return []
    return sorted(df["NATUREZA_APURADA"].dropna().astype(str).unique().tolist())


@st.cache_data(ttl=3600)
def anos_disponiveis() -> list[int]:
    df = serie_estado()
    if df.empty:
        return []
    return sorted(df["ANO"].dropna().astype(int).unique().tolist())


def _norm_natureza(s: pd.Series) -> pd.Series:
    """Mesma normalização aplicada em ``pipeline/aggregate.py``.

    A base ``processed/sp_dados_criminais`` mantém a grafia original da SSP
    (com/sem acento, variações), enquanto os agregados guardam a versão
    canônica. Pra o filtro da UI (que vem da lista de agregados) casar com
    a base de pontos, normalizamos a coluna em runtime.
    """
    from unidecode import unidecode
    s = s.astype("string")
    return s.map(lambda x: None if pd.isna(x) else " ".join(unidecode(str(x)).upper().split())).astype("string")


@st.cache_data(show_spinner="Carregando pontos da partição…", ttl=600)
def pontos(ano: int, mes: int, natureza: Optional[str] = None,
           max_rows: int = 50_000) -> pd.DataFrame:
    # Prefere a amostra (commitada no repo). Se rodando localmente e a amostra
    # ainda não foi gerada, cai na base completa se estiver disponível.
    path = PROCESSED / "sp_dados_criminais"
    if not path.exists():
        path = PROCESSED_FULL / "sp_dados_criminais"
        if not path.exists():
            return pd.DataFrame()

    # Vamos direto pro(s) arquivo(s) da partição ANO={ano}/MES={mes}.
    # Isso evita duas armadilhas do pyarrow:
    # 1. Inferência do Hive partitioning como dictionary<int32> (ArrowNotImplementedError
    #    ao mergear com int32 do arquivo — mesmo bug que atacou o pipeline).
    # 2. Schema drift entre partições distintas (ex.: uma partição com
    #    NATUREZA_APURADA:string e outra com tudo null → falha no unify
    #    quando o dataset lê múltiplos arquivos de uma vez).
    part_dir = path / f"ANO={int(ano)}" / f"MES={int(mes)}"
    if not part_dir.exists():
        return pd.DataFrame()
    parquet_files = sorted(part_dir.rglob("*.parquet"))
    if not parquet_files:
        return pd.DataFrame()

    import pyarrow as pa
    import pyarrow.parquet as pq

    WANT = ["LATITUDE", "LONGITUDE", "NATUREZA_APURADA",
            "NOME_MUNICIPIO", "DATA_OCORRENCIA_BO", "COORDS_VALIDAS"]
    tables = []
    for pfile in parquet_files:
        pf = pq.ParquetFile(str(pfile))
        avail = set(pf.schema_arrow.names)
        cols = [c for c in WANT if c in avail]
        tables.append(pf.read(columns=cols))
    if len(tables) == 1:
        table = tables[0]
    else:
        # promote_options="default" permite merge seguro de schemas quase-iguais
        # (null → string etc.) entre fragmentos da MESMA partição
        try:
            table = pa.concat_tables(tables, promote_options="default")
        except TypeError:
            # pyarrow < 14 não suporta promote_options="default"
            table = pa.concat_tables(tables, promote=True)
    df = table.to_pandas()
    if df.empty:
        return df
    df["NATUREZA_APURADA"] = _norm_natureza(df["NATUREZA_APURADA"])
    if natureza:
        df = df[df["NATUREZA_APURADA"] == natureza]
    if "COORDS_VALIDAS" in df.columns:
        df = df[df["COORDS_VALIDAS"].fillna(False).astype(bool)]
    df = df.dropna(subset=["LATITUDE", "LONGITUDE"])
    if len(df) > max_rows:
        df = df.sample(max_rows, random_state=42)
    return df


# Mapa do seletor de recorte → loader correspondente + coluna-chave nos dados
RECORTE_LOADER = {
    "Município":        (por_municipio, "CD_MUN"),
    "Setor Censitário": (por_setor,      "sc_cod"),
    "Batalhão PMESP":   (por_batalhao,   "OPM_BTL"),
    "Companhia PMESP":  (por_companhia,  "OPMCOD_CIA"),
    "Comando (CPA)":    (por_comando,    "CMDO"),
    "Delegacia (DP)":   (por_dp,         "DpGeoCod"),
}
