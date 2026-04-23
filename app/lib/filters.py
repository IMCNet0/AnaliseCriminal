"""Filtros globais exibidos na sidebar de todas as páginas.

Pontos desta rodada:
  • **Persistência entre páginas.** Todos os widgets (Período, Naturezas,
    Recorte) guardam estado em ``st.session_state`` via ``key=``, então
    alternar de Home → Gráficos → Rankings mantém a seleção.
  • **Default do período = ÚLTIMO MÊS com dado disponível.** Não é mais o
    range "últimos 2 anos": o primeiro carregamento da Home abre no mês
    mais recente presente em ``serie_estado`` (ex.: 01/mar/2026–31/mar/2026).
    A partir daí o usuário amplia manualmente.
  • **Default de natureza = TOP-1 por volume.** Pré-seleção no first-run
    da natureza mais frequente na base — garante que o mapa abra com o
    coroplético por Delegacia já renderizado (ver ``_top_natureza_default``).
    Se o usuário limpar o multiselect, a escolha dele é respeitada.
  • **Recortes reduzidos a dois:** Delegacia (DP) e Setor Censitário.
    CPA / BTL / CIA / Município foram retirados da aba lateral a pedido
    do cliente — a análise PMESP agora vive em páginas específicas.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional
from calendar import monthrange

import pandas as pd
import streamlit as st

from . import data


# Pedido do cliente (abr/2026): deixar apenas Delegacia e Setor Censitário
# na aba lateral + Rankings + coroplético da Home. Os recortes PMESP
# continuam disponíveis no código (map_builder exporta helpers) caso
# uma página futura precise, mas NÃO são mais oferecidos ao usuário aqui.
RECORTES = [
    "Delegacia (DP)",     # default — ~1.000 polígonos, granularidade ideal
    "Setor Censitário",   # pesado (~250k polígonos), simplificado em geoparquet
]


MESES_PT = {
    0: "Todos",
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}


# Chaves canônicas no session_state — ficam fora da função para poderem ser
# referenciadas por outras páginas (ex.: Home reseta o map_zoom mas não deve
# zerar os filtros globais).
SS_DATA_INI  = "flt_data_ini"
SS_DATA_FIM  = "flt_data_fim"
SS_NATUREZAS = "flt_naturezas"
SS_RECORTE   = "flt_recorte"


@dataclass
class GlobalFilters:
    """Contrato com o resto do app: períodos via ``data_ini`` / ``data_fim``.

    Os atalhos ``ano_ini`` / ``ano_fim`` / ``mes`` continuam expostos por
    compatibilidade (pontos/hotspot paginam por ANO × MES no parquet).
    """
    data_ini: date
    data_fim: date
    naturezas: list[str]
    recorte: str

    @property
    def ano_ini(self) -> int:
        return int(self.data_ini.year)

    @property
    def ano_fim(self) -> int:
        return int(self.data_fim.year)

    @property
    def mes(self) -> int:
        if (self.data_ini.year == self.data_fim.year
                and self.data_ini.month == self.data_fim.month):
            return int(self.data_ini.month)
        return 0

    def mask_date(self, df: pd.DataFrame) -> pd.Series:
        """Filtro DATA in [data_ini, data_fim].

        - Se o DataFrame tem a coluna ``DATA`` (série estadual), usa-a.
        - Senão, reconstrói DATA do primeiro dia do mês a partir de ANO+MES
          (agregados por recorte).
        """
        d_ini = pd.Timestamp(self.data_ini)
        d_fim = pd.Timestamp(self.data_fim)
        if "DATA" in df.columns:
            s = pd.to_datetime(df["DATA"], errors="coerce")
            return s.between(d_ini, d_fim)
        if {"ANO", "MES"}.issubset(df.columns):
            s = pd.to_datetime(
                df["ANO"].astype("Int64").astype("string") + "-"
                + df["MES"].astype("Int64").astype("string").str.zfill(2) + "-01",
                errors="coerce",
            )
            return s.between(d_ini, d_fim)
        if "ANO" in df.columns:
            return df["ANO"].between(self.data_ini.year, self.data_fim.year)
        return pd.Series(True, index=df.index)

    def mask_natureza(self, df: pd.DataFrame) -> pd.Series:
        if not self.naturezas:
            return pd.Series(True, index=df.index)
        return df["NATUREZA_APURADA"].isin(self.naturezas)

    # --- Janela "mesmo período do ano anterior" ----------------------------
    # Usada pelos gauges YoY em Gráficos: compara o total do intervalo atual
    # com o total do mesmo intervalo do ano anterior.
    def prev_year_window(self) -> tuple[date, date]:
        """Desloca ``(data_ini, data_fim)`` 1 ano pra trás, clampando fev 29."""
        def _shift(d: date) -> date:
            y = d.year - 1
            # Protege 29/fev em ano não-bissexto.
            last_day = monthrange(y, d.month)[1]
            return date(y, d.month, min(d.day, last_day))
        return _shift(self.data_ini), _shift(self.data_fim)


def _latest_month_window() -> tuple[date, date]:
    """Retorna ``(primeiro_dia, ultimo_dia)`` do último mês com dado na série.

    Fallback quando ``serie_estado`` estiver vazio: ano atual × mês atual
    (pior caso: UI abre vazia, mas não quebra).
    """
    try:
        serie = data.serie_estado()
    except Exception:
        serie = pd.DataFrame()
    if serie is not None and not serie.empty and {"ANO", "MES"}.issubset(serie.columns):
        # Ordena por (ano, mes) e pega o mais recente.
        key = (serie["ANO"].astype("Int64"), serie["MES"].astype("Int64"))
        idx = pd.Index(
            list(zip(key[0].fillna(-1), key[1].fillna(-1)))
        ).argmax()
        y = int(serie["ANO"].iloc[idx])
        m = int(serie["MES"].iloc[idx])
    else:
        today = date.today()
        y, m = today.year, today.month
    last_day = monthrange(y, m)[1]
    return date(y, m, 1), date(y, m, last_day)


def _bootstrap_defaults() -> tuple[date, date]:
    """Plant a default no session_state na PRIMEIRA visita (somente).

    Depois disso os widgets assumem o controle via ``key=``, e mudar de
    página/voltar à Home não reset a seleção.
    """
    if SS_DATA_INI in st.session_state and SS_DATA_FIM in st.session_state:
        return st.session_state[SS_DATA_INI], st.session_state[SS_DATA_FIM]
    di, df_ = _latest_month_window()
    st.session_state.setdefault(SS_DATA_INI, di)
    st.session_state.setdefault(SS_DATA_FIM, df_)
    return di, df_


def _echo_session_state() -> None:
    """Workaround do multipage Streamlit: reassinala chaves de widget.

    O problema: quando a mesma `key=` é usada em páginas diferentes, o
    Streamlit marca internamente o valor como "widget state" e o **descarta**
    no rerun da página seguinte se a widget ainda não foi chamada. Resultado
    prático: ao trocar de página, os filtros voltam ao default.

    O fix canônico é auto-atribuir a chave no topo de cada página antes de
    qualquer widget — isso "promove" o valor de widget-state para user-state
    e o Streamlit passa a preservá-lo. Ver docs 1.28+:
    https://docs.streamlit.io/library/advanced-features/widget-behavior
    """
    for k in (SS_DATA_INI, SS_DATA_FIM, SS_NATUREZAS, SS_RECORTE):
        if k in st.session_state:
            st.session_state[k] = st.session_state[k]


def _top_natureza_default() -> list[str]:
    """Pré-seleção inteligente: a natureza mais frequente da série estadual.

    Rationale (abr/2026 rodada #3): o cliente pediu que a Home abra já com o
    coroplético por Delegacia renderizado. Como o coroplético depende de
    ≥1 natureza selecionada, plantamos a top-1 por volume total no
    ``session_state`` no first-run. Se o usuário limpar o multiselect
    depois, a escolha dele é respeitada (o ``if SS_NATUREZAS not in
    st.session_state`` guarda isso).
    """
    try:
        sr = data.serie_estado()
        if sr is None or sr.empty or "NATUREZA_APURADA" not in sr.columns:
            return []
        top1 = (
            sr.groupby("NATUREZA_APURADA", observed=True)["N"]
            .sum().sort_values(ascending=False).head(1).index.tolist()
        )
        return [str(x) for x in top1 if pd.notna(x)]
    except Exception:
        return []


def sidebar_filters(default_naturezas: Optional[list[str]] = None) -> GlobalFilters:
    # Precisa vir ANTES de qualquer widget desta função — senão o echo é tarde
    # demais e o valor já foi descartado pelo Streamlit.
    _echo_session_state()

    st.sidebar.markdown("## Filtros")

    # --- Default inicial = ÚLTIMO MÊS com dado disponível ------------------
    di_default, df_default = _bootstrap_defaults()

    col_a, col_b = st.sidebar.columns(2)
    data_ini = col_a.date_input(
        "Data inicial", value=di_default, key=SS_DATA_INI, format="DD/MM/YYYY",
    )
    data_fim = col_b.date_input(
        "Data final", value=df_default, key=SS_DATA_FIM, format="DD/MM/YYYY",
    )
    if data_ini > data_fim:
        # Silent clamp — evita mask vazio quando o usuário inverte os campos.
        data_ini, data_fim = data_fim, data_ini

    # --- Naturezas (multiselect) — persistida via key ---------------------
    all_naturezas = data.naturezas_disponiveis()
    # Garante que a seed do state exista (só na primeira visita).
    # Se o caller não passou default explícito, plantamos a natureza top-1
    # por volume → Home abre com coroplético por Delegacia já habilitado.
    if SS_NATUREZAS not in st.session_state:
        if default_naturezas is None:
            default_naturezas = _top_natureza_default()
        st.session_state[SS_NATUREZAS] = default_naturezas or []
    # Remove entradas que sumiram do agregado (ex.: natureza renomeada).
    st.session_state[SS_NATUREZAS] = [
        n for n in st.session_state[SS_NATUREZAS] if n in all_naturezas
    ]
    sel_nat = st.sidebar.multiselect(
        "Naturezas (vazio = todas)", all_naturezas,
        key=SS_NATUREZAS,
        help="Escolha uma ou mais naturezas apuradas. Vazio considera todas. "
             "**Os mapas temáticos só aparecem quando pelo menos uma natureza "
             "está selecionada.**",
    )

    # --- Recorte (radio) — persistida via key -----------------------------
    if st.session_state.get(SS_RECORTE) not in RECORTES:
        st.session_state[SS_RECORTE] = RECORTES[0]
    recorte = st.sidebar.radio(
        "Recorte geográfico", RECORTES,
        key=SS_RECORTE,
        help="Unidade de análise geográfica. O ponto-em-polígono é feito em "
             "tempo de ingestão contra o shapefile da camada.",
    )

    return GlobalFilters(
        data_ini=data_ini, data_fim=data_fim,
        naturezas=sel_nat, recorte=recorte,
    )


def sidebar_footer() -> None:
    """Rodapé da sidebar (divisor + créditos). Chamado depois dos controles
    específicos da página pra ficar sempre na parte inferior."""
    st.sidebar.markdown("---")
    st.sidebar.caption("Fonte: SSP-SP · IBGE")
    st.sidebar.caption("Desenvolvido por InsightGeoLab AI")
