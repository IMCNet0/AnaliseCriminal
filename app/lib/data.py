"""Camada de acesso aos dados agregados (cache Streamlit).

Todas as leituras passam por @st.cache_data para não reprocessar a cada rerun.
Os agregados vivem em data/aggregates/ e são pequenos o bastante para rodar no
plano gratuito do Streamlit Community Cloud.

────────────────────────────────────────────────────────────────────────────
Escopo: CIDADE DE SÃO PAULO (SP-Capital, IBGE 3550308)
────────────────────────────────────────────────────────────────────────────
Pedido cliente (rodada abr/26 #4): TODO o portal deve operar exclusivamente
com dados da Capital. O filtro é aplicado **na camada de dados** — o resto
do app consome esses loaders sem precisar saber do recorte espacial.

  • por_municipio  → CD_MUN == SP_CAPITAL_CD_MUN
  • por_setor      → CD_MUN == SP_CAPITAL_CD_MUN
  • por_dp         → DpGeoCod ∈ {DPs cujo SecGeoCod pertence à DECAP/DP-Capital}
  • dp_options     → mesmo conjunto de DPs filtrado
  • pontos         → NOME_MUNICIPIO == "SAO PAULO" (após unidecode)

As seccionais ``SP_CAPITAL_DP_SECCIONAIS`` foram derivadas inspecionando
o atributo ``SecGeoCod`` em ``data/geo/DP.json``. São os 8 códigos cujas
DPs descrevem bairros/distritos da Capital (Sé, Bom Retiro, Pinheiros,
Santo Amaro, Itaquera, etc.) — total ≈ 94 DPs, batendo com a contagem
oficial de DPs da Capital.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
AGG = ROOT / "data" / "aggregates"
GEO = ROOT / "data" / "geo"

# ============================================================================
# Constantes do recorte SP-Capital
# ============================================================================
SP_CAPITAL_CD_MUN = "3550308"
SP_CAPITAL_NM_MUN = "SAO PAULO"  # forma unidecode (sem til/acento) usada nos pontos
SP_CAPITAL_NM_MUN_ALIASES = {"SAO PAULO", "S.PAULO", "S. PAULO"}  # abreviações usadas pela SSP

# Seccionais (SecGeoCod do DP.json) cujo conjunto de DPs forma o recorte
# da Capital. Derivado por inspeção: cada seccional aqui contém apenas
# DPs cujo DpGeoDes nomeia um distrito/bairro de SP-Capital, sem citar
# nenhum município vizinho. Total esperado: 94 DPs.
SP_CAPITAL_DP_SECCIONAIS = {
    10100,  # DECAP-1 Centro          (Sé, Bom Retiro, Sta Cecília, Aclimação...)
    10200,  # DECAP-1 Sul             (Vila Mariana, Sacomã, Ipirapuera...)
    10210,  # DECAP-1 Sul-2           (Santo Amaro, Capão Redondo, Parelheiros...)
    10300,  # DECAP-1 Oeste           (Pinheiros, Lapa, Perdizes, Pirituba...)
    20100,  # DECAP-2 Norte           (Casa Verde, Carandiru, Freguesia do Ó...)
    20200,  # DECAP-2 Leste-1         (Penha, Mooca, Tatuapé, Vila Prudente...)
    500070, # DECAP-5 Leste-Extremo   (Itaquera, Itaim Paulista, Ponte Rasa...)
    500080, # DECAP-6 Leste-Extremo-2 (São Mateus, Cidade Tiradentes, Guaianazes...)
}
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


# ============================================================================
# SP-Capital — derivação dos DpGeoCods da Capital a partir do DP.json
# ============================================================================
@st.cache_data(show_spinner="Detectando DPs da Capital…", ttl=86400)
def sp_capital_dp_codes() -> set[str]:
    """Retorna o conjunto (string normalizada) de DpGeoCod da Capital.

    Lê ``data/geo/DP.json`` e mantém só features cujo ``SecGeoCod`` está em
    ``SP_CAPITAL_DP_SECCIONAIS``. Cacheado por 24h porque o shapefile é estático.

    Em caso de erro (arquivo ausente, shapefile corrompido) devolve set vazio
    — o que faria todos os filtros downstream esvaziarem o app. Os chamadores
    devem detectar a degradação via ``serie_estado()`` e mostrar mensagem.
    """
    import json
    p = GEO / "DP.json"
    if not p.exists():
        return set()
    try:
        with open(p, "r", encoding="utf-8") as f:
            g = json.load(f)
    except Exception:
        return set()
    out: set[str] = set()
    for ft in g.get("features", []):
        props = ft.get("properties", {}) or {}
        sec = props.get("SecGeoCod")
        try:
            if int(sec) not in SP_CAPITAL_DP_SECCIONAIS:
                continue
        except (TypeError, ValueError):
            continue
        cod = props.get("DpGeoCod")
        if cod is None:
            continue
        # Normalização coerente com _norm_dp_cod (Int64 → string sem ".0")
        try:
            out.add(str(int(cod)))
        except (TypeError, ValueError):
            out.add(str(cod).strip())
    return out


def _filter_sp_dp(df: pd.DataFrame) -> pd.DataFrame:
    """Restringe um agregado por DP ao conjunto SP-Capital.

    No-op se o df estiver vazio ou não tiver DpGeoCod. Usa o normalizador
    ``_norm_dp_cod`` para casar 130409 (int) com "130409" (string).
    """
    if df.empty or "DpGeoCod" not in df.columns:
        return df
    keep = sp_capital_dp_codes()
    if not keep:
        return df  # sem dicionário, evita zerar tudo silenciosamente
    return df[_norm_dp_cod(df["DpGeoCod"]).isin(keep)].copy()


@st.cache_data(show_spinner="Carregando série da Capital…", ttl=3600)
def serie_estado() -> pd.DataFrame:
    """Série temporal da Cidade de São Paulo (ANO × MES × NATUREZA × N).

    Reconstruída agregando ``por_dp`` restrito às DPs da Capital. Não usa
    mais o ``serie_estado.parquet`` original (estadual), pois o escopo do
    portal foi limitado à Capital (rodada abr/26 #4). O nome do método foi
    mantido por compatibilidade com pages que já chamam ``data.serie_estado()``.
    """
    dfp = por_dp()  # já vem filtrado para a Capital (ver por_dp() abaixo)
    if dfp.empty:
        return pd.DataFrame()
    out = (
        dfp.groupby(["ANO", "MES", "NATUREZA_APURADA"], as_index=False, observed=True)["N"]
        .sum()
    )
    out["DATA"] = pd.to_datetime(
        out["ANO"].astype(str) + "-" + out["MES"].astype(str) + "-01",
        errors="coerce",
    )
    return out


@st.cache_data(show_spinner="Carregando agregados por município…", ttl=3600)
def por_municipio(natureza: Optional[str] = None) -> pd.DataFrame:
    """Agregado por município — filtrado a SP-Capital (CD_MUN=3550308).

    No recorte atual a granularidade ``por_municipio`` colapsa numa única
    linha por (ANO, MES, NATUREZA), pois só sobra o município da Capital.
    Mantida por compat com a página de Rankings.
    """
    df = _safe_read(AGG / "por_municipio.parquet")
    if df.empty:
        return df
    if "CD_MUN" in df.columns:
        df = df[df["CD_MUN"].astype("string").str.strip() == SP_CAPITAL_CD_MUN].copy()
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
    """Agregado por DP — restrito às DPs da Capital (rodada abr/26 #4)."""
    df = _safe_read(AGG / "por_dp.parquet")
    return _filter_sp_dp(df)


@st.cache_data(show_spinner="Carregando lista de Delegacias…", ttl=3600)
def dp_options() -> pd.DataFrame:
    """Lista única (DpGeoCod, DpGeoDes) para popular o dropdown da sidebar.

    Tenta primeiro o agregado ``por_dp.parquet`` (barato). Se ``DpGeoDes``
    não estiver lá — nosso caso atual, o pipeline antigo só guarda o
    código — cai pro shapefile ``data/geo/DP.json`` via ``geo.load_layer``.

    Retorna um DataFrame com duas colunas: ``DpGeoCod`` (string normalizada)
    e ``DpGeoDes`` (string), ordenado por DpGeoDes.
    """
    dfp = por_dp()
    if not dfp.empty and {"DpGeoCod", "DpGeoDes"}.issubset(dfp.columns):
        out = (
            dfp[["DpGeoCod", "DpGeoDes"]]
            .dropna().drop_duplicates()
            .sort_values("DpGeoDes").reset_index(drop=True)
        )
        # Normaliza DpGeoCod pelo mesmo critério de _norm_dp_cod — garante que
        # o valor salvo no session_state ("10102") case com a comparação feita
        # em serie_contextual, que também usa _norm_dp_cod. Sem isso float64
        # "10102.0" → string "10102.0" ≠ "10102" e a série retorna vazia.
        out["DpGeoCod"] = _norm_dp_cod(out["DpGeoCod"])
        out["DpGeoDes"] = out["DpGeoDes"].astype("string")
        return out

    # Fallback: usa a camada geo (importa localmente pra evitar ciclo).
    # geo.load_layer já vem filtrado a SP-Capital (rodada abr/26 #4).
    try:
        from . import geo as geolib
        gdf, _ = geolib.load_layer("Delegacia (DP)")
    except Exception:
        return pd.DataFrame(columns=["DpGeoCod", "DpGeoDes"])
    if gdf is None or "DpGeoCod" not in gdf.columns:
        return pd.DataFrame(columns=["DpGeoCod", "DpGeoDes"])
    des_col = "DpGeoDes" if "DpGeoDes" in gdf.columns else "DpGeoCod"
    out = (
        gdf[["DpGeoCod", des_col]]
        .rename(columns={des_col: "DpGeoDes"})
        .dropna().drop_duplicates()
        .sort_values("DpGeoDes").reset_index(drop=True)
    )
    out["DpGeoCod"] = _norm_dp_cod(out["DpGeoCod"])
    out["DpGeoDes"] = out["DpGeoDes"].astype("string")
    return out


def _norm_dp_cod(s: pd.Series) -> pd.Series:
    """Normaliza DpGeoCod → string (sem espaços, sem ".0" de float cast).

    A mesma lógica que o Home.py aplica inline quando mergeia. Centralizada
    aqui pra o filtro por DP da sidebar casar com os dados.
    """
    nums = pd.to_numeric(s, errors="coerce")
    valid = nums.dropna()
    if not valid.empty and (valid == valid.astype("int64")).all():
        return nums.astype("Int64").astype("string")
    return s.astype("string").str.strip().str.upper()


@st.cache_data(show_spinner="Filtrando série por DP…", ttl=3600)
def serie_contextual(dp_cod: Optional[str] = None) -> pd.DataFrame:
    """Série temporal ANO × MES × NATUREZA_APURADA × N.

    Quando ``dp_cod`` é None → retorna o ``serie_estado`` (estadual).
    Quando informado → filtra ``por_dp`` pelo DpGeoCod e agrega por mês,
    reproduzindo o esquema de colunas do serie_estado (incluindo ``DATA``).
    Usada pela Home/Gráficos/Séries Temporais transparentemente quando o
    usuário escolhe uma delegacia na sidebar.
    """
    if dp_cod is None or str(dp_cod).strip() == "":
        return serie_estado()
    dfp = por_dp()
    if dfp.empty or "DpGeoCod" not in dfp.columns:
        return pd.DataFrame()
    mask = _norm_dp_cod(dfp["DpGeoCod"]) == str(dp_cod).strip()
    sub = dfp.loc[mask]
    if sub.empty:
        return pd.DataFrame(columns=["ANO", "MES", "NATUREZA_APURADA", "N", "DATA"])
    out = (
        sub.groupby(["ANO", "MES", "NATUREZA_APURADA"], as_index=False, observed=True)["N"]
        .sum()
    )
    out["DATA"] = pd.to_datetime(
        out["ANO"].astype(str) + "-" + out["MES"].astype(str) + "-01",
        errors="coerce",
    )
    return out


@st.cache_data(ttl=3600)
def por_setor() -> pd.DataFrame:
    """Agregado por Setor Censitário — restrito a SP-Capital (CD_MUN=3550308)."""
    df = _safe_read(AGG / "por_setor.parquet")
    if df.empty or "CD_MUN" not in df.columns:
        return df
    return df[df["CD_MUN"].astype("string").str.strip() == SP_CAPITAL_CD_MUN].copy()


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
           dp_cod: Optional[str] = None,
           max_rows: int = 50_000) -> pd.DataFrame:
    # Prefere a base completa (local dev) — resultados exatos por DP/natureza.
    # Cai na amostra quando a base completa não existe (Streamlit Cloud).
    use_full = (PROCESSED_FULL / "sp_dados_criminais").exists()
    path = PROCESSED_FULL / "sp_dados_criminais" if use_full else PROCESSED / "sp_dados_criminais"
    if not path.exists():
        return pd.DataFrame()

    part_dir = path / f"ANO={int(ano)}" / f"MES={int(mes)}"
    if not part_dir.exists():
        return pd.DataFrame()
    parquet_files = sorted(part_dir.rglob("*.parquet"))
    if not parquet_files:
        return pd.DataFrame()

    import pyarrow as pa
    import pyarrow.parquet as pq

    WANT = ["LATITUDE", "LONGITUDE", "NATUREZA_APURADA",
            "NOME_MUNICIPIO", "DATA_OCORRENCIA_BO", "COORDS_VALIDAS", "DpGeoCod"]
    tables = []
    for pfile in parquet_files:
        pf = pq.ParquetFile(str(pfile))
        avail = set(pf.schema_arrow.names)
        cols = [c for c in WANT if c in avail]
        tables.append(pf.read(columns=cols))
    if len(tables) == 1:
        table = tables[0]
    else:
        try:
            table = pa.concat_tables(tables, promote_options="default")
        except TypeError:
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
    if "NOME_MUNICIPIO" in df.columns:
        nm = _norm_natureza(df["NOME_MUNICIPIO"])
        df = df[nm.isin(SP_CAPITAL_NM_MUN_ALIASES)].copy()
    # Filtro por DP: usa DpGeoCod se já presente (amostra); caso contrário
    # faz spatial join leve contra DP.json (base completa local).
    if dp_cod and not df.empty:
        if "DpGeoCod" in df.columns:
            norm_col = _norm_dp_cod(df["DpGeoCod"])
            df = df[norm_col == str(dp_cod).strip()].copy()
        else:
            df = _filter_pontos_by_dp(df, dp_cod)
    if len(df) > max_rows:
        df = df.sample(max_rows, random_state=42)
    return df


def _filter_pontos_by_dp(df: pd.DataFrame, dp_cod: str) -> pd.DataFrame:
    """Spatial join leve: filtra pontos ao polígono da DP selecionada."""
    import json
    import geopandas as gpd
    p = GEO / "DP.json"
    if not p.exists():
        return df
    try:
        with open(p, "r", encoding="utf-8") as f:
            gj = json.load(f)
    except Exception:
        return df
    # Localiza o feature da DP pelo DpGeoCod normalizado
    target = None
    for ft in gj.get("features", []):
        props = ft.get("properties", {}) or {}
        cod = props.get("DpGeoCod")
        try:
            norm = str(int(cod))
        except (TypeError, ValueError):
            norm = str(cod).strip()
        if norm == str(dp_cod).strip():
            target = ft
            break
    if target is None:
        return df
    try:
        gdf_dp = gpd.GeoDataFrame.from_features([target], crs="EPSG:4326")[["geometry"]]
        pts = gpd.GeoDataFrame(
            df.reset_index(drop=True),
            geometry=gpd.points_from_xy(df["LONGITUDE"], df["LATITUDE"]),
            crs="EPSG:4326",
        )
        joined = gpd.sjoin(pts, gdf_dp, how="inner", predicate="within")
        if joined.index.has_duplicates:
            joined = joined[~joined.index.duplicated(keep="first")]
        return joined.drop(columns=["geometry", "index_right"], errors="ignore")
    except Exception:
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
