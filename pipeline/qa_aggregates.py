"""QA estatístico dos agregados gerados por ``aggregate.py``.

Roda uma bateria de checagens contra ``data/aggregates/`` para detectar:

1. **Consistência de totais** — soma de N em ``serie_estado`` vs. ``por_municipio``,
   ``por_batalhao``, ``por_comando``, ``por_dp``. Divergências revelam pontos
   perdidos em sjoin (esperado, mas o gap não pode ser absurdo) ou bug de
   agregação (não esperado).
2. **Cobertura geoespacial** — % de registros que caíram fora de qualquer
   polígono em cada camada. Se >5%, o geojson provavelmente está desatualizado
   ou com CRS errado.
3. **Continuidade temporal** — meses ausentes dentro do intervalo observado
   (ex.: tem jan/2022 e mar/2022 mas não fev/2022 → buraco de ingestão).
4. **Top naturezas** — lista as 20 maiores para bater visualmente com o painel
   oficial da SSP.
5. **Outliers por município** — z-score mensal por combo (município, natureza).
   Picos acima de |z|>4 são candidatos a erro de data ou reclassificação.
6. **Saneamento de denominadores** — companhias com ``populacao_CIA`` NULL ou 0
   quebram cálculo de taxa por 100k. Idem setores sem ``pop_total``.

Uso:
    python pipeline/qa_aggregates.py

O script imprime um relatório em stdout e grava ``data/aggregates/_qa_report.md``
para anexo no README do projeto ou revisão posterior.
"""
from __future__ import annotations

from pathlib import Path
from textwrap import indent
import logging

import numpy as np
import pandas as pd

from common import AGGREGATES, setup_logging

log = logging.getLogger(__name__)

REPORT_FILE = AGGREGATES / "_qa_report.md"

EXPECTED_FILES = {
    "serie_estado":     ["ANO", "MES", "NATUREZA_APURADA", "N"],
    "cubo_natureza":    ["NATUREZA_APURADA", "N"],
    "cubo_conduta":     ["DESCR_CONDUTA", "N"],
    "por_municipio":    ["ANO", "MES", "NATUREZA_APURADA", "CD_MUN", "NM_MUN", "N"],
    "por_batalhao":     ["ANO", "MES", "NATUREZA_APURADA", "OPM_BTL", "cmdo_BTL", "N"],
    "por_companhia":    ["ANO", "MES", "NATUREZA_APURADA", "OPMCOD_CIA", "OPM_CIA", "btl_CIA", "populacao_CIA", "N"],
    "por_comando":      ["ANO", "MES", "NATUREZA_APURADA", "CMDO", "regiao", "N"],
    "por_dp":           ["ANO", "MES", "NATUREZA_APURADA", "DpGeoCod", "DpGeoDes", "N"],
    "por_setor":        ["ANO", "MES", "NATUREZA_APURADA", "sc_cod", "CD_MUN", "favela_2022", "N"],
}


def load_all() -> dict[str, pd.DataFrame]:
    """Lê todos os agregados esperados; tolera ausência de arquivos opcionais."""
    out: dict[str, pd.DataFrame] = {}
    for name in EXPECTED_FILES:
        path = AGGREGATES / f"{name}.parquet"
        if not path.exists():
            log.warning("Faltando: %s", path.name)
            continue
        df = pd.read_parquet(path, engine="pyarrow")
        out[name] = df
        log.info("  %-18s %9s linhas  %s",
                 name, f"{len(df):,}", list(df.columns))
    return out


# ---------- Checks ----------

def check_totals(dfs: dict[str, pd.DataFrame]) -> list[str]:
    """Compara soma de N entre base estadual e recortes geo."""
    lines = ["## 1. Consistência de totais\n"]
    if "serie_estado" not in dfs:
        lines.append("⚠ serie_estado ausente — skip\n")
        return lines

    total_estado = int(dfs["serie_estado"]["N"].sum())
    lines.append(f"Total estadual (serie_estado): **{total_estado:,}** registros\n")

    comparisons = [
        ("por_municipio", None),  # deve bater 1:1 (não passa por sjoin)
        ("por_batalhao", "sjoin"),
        ("por_companhia", "sjoin"),
        ("por_comando", "sjoin"),
        ("por_dp", "sjoin"),
        ("por_setor", "sjoin"),
    ]
    for name, kind in comparisons:
        if name not in dfs:
            lines.append(f"- {name}: **ausente**")
            continue
        total = int(dfs[name]["N"].sum())
        diff = total - total_estado
        pct = 100 * diff / total_estado if total_estado else 0.0
        flag = ""
        if kind == "sjoin":
            # Tolerância: ~30% de BOs da SSP vêm sem lat/long (campo opcional
            # no boletim). Após deduplicação de overlaps no sjoin, todos os
            # recortes geo devem ter o MESMO total. Alarmamos só se a perda
            # ultrapassar 35% OU exceder o estadual (impossível sem bug).
            if pct < -35 or pct > 0.5:
                flag = " 🚨"
        else:
            if abs(pct) > 0.01:
                flag = " 🚨 (deveria ser idêntico)"
        lines.append(f"- {name:15s} {total:>15,}  ({pct:+6.2f}% vs. estado){flag}")
    lines.append("")
    return lines


def check_geo_coverage(dfs: dict[str, pd.DataFrame]) -> list[str]:
    """% de N que caiu em polígono = inverso do gap de sjoin."""
    lines = ["## 2. Cobertura geoespacial (sjoin)\n"]
    if "serie_estado" not in dfs:
        lines.append("⚠ serie_estado ausente — skip\n")
        return lines
    total = int(dfs["serie_estado"]["N"].sum())
    for name in ["por_batalhao", "por_companhia", "por_comando", "por_dp", "por_setor"]:
        if name not in dfs:
            continue
        matched = int(dfs[name]["N"].sum())
        pct = 100 * matched / total if total else 0.0
        flag = "" if pct >= 70 else " ⚠ cobertura baixa — checar geojson/CRS"
        lines.append(f"- {name:15s} {pct:5.1f}% dos BOs caíram em um polígono{flag}")
    lines.append("")
    return lines


def check_temporal_gaps(dfs: dict[str, pd.DataFrame]) -> list[str]:
    """Meses ausentes dentro do intervalo observado da serie estadual."""
    lines = ["## 3. Continuidade temporal\n"]
    if "serie_estado" not in dfs:
        lines.append("⚠ serie_estado ausente — skip\n")
        return lines
    df = dfs["serie_estado"].dropna(subset=["ANO", "MES"]).copy()
    df["periodo"] = df["ANO"].astype("int").astype(str) + "-" + df["MES"].astype("int").map("{:02d}".format)
    observed = sorted(df["periodo"].unique())
    if not observed:
        lines.append("⚠ Nenhum período encontrado\n")
        return lines

    first = pd.Period(observed[0], freq="M")
    last = pd.Period(observed[-1], freq="M")
    expected = pd.period_range(first, last, freq="M").astype(str).tolist()
    missing = sorted(set(expected) - set(observed))

    lines.append(f"Intervalo: **{observed[0]}** → **{observed[-1]}** ({len(observed)} meses presentes, {len(expected)} esperados)")
    if not missing:
        lines.append("✓ Nenhum mês faltando")
    else:
        lines.append(f"🚨 {len(missing)} mês(es) ausente(s): {', '.join(missing[:12])}"
                     + ("..." if len(missing) > 12 else ""))
    # Vol. por ano (sanidade rápida)
    lines.append("\nVolume por ano:")
    per_year = df.groupby(df["ANO"].astype(int))["N"].sum()
    for y, n in per_year.items():
        lines.append(f"  - {y}: {int(n):,}")
    lines.append("")
    return lines


def check_top_naturezas(dfs: dict[str, pd.DataFrame], k: int = 20) -> list[str]:
    lines = [f"## 4. Top {k} naturezas apuradas (todo o período)\n"]
    if "cubo_natureza" not in dfs:
        lines.append("⚠ cubo_natureza ausente — skip\n")
        return lines
    top = dfs["cubo_natureza"].sort_values("N", ascending=False).head(k).copy()
    total = int(dfs["cubo_natureza"]["N"].sum())
    top["pct"] = 100 * top["N"] / total
    lines.append("| # | Natureza | N | % |")
    lines.append("|---|----------|---:|---:|")
    for i, row in enumerate(top.itertuples(index=False), 1):
        nat = str(row.NATUREZA_APURADA)[:60]
        lines.append(f"| {i} | {nat} | {int(row.N):,} | {row.pct:5.2f}% |")
    lines.append("")
    return lines


def check_outliers(dfs: dict[str, pd.DataFrame], z_thresh: float = 4.0, top: int = 20) -> list[str]:
    """Z-score mensal por (município, natureza); só reporta picos atípicos.

    Para evitar falsos positivos em séries curtas, exige ao menos 12 meses.
    """
    lines = [f"## 5. Outliers por município/natureza (|z| ≥ {z_thresh})\n"]
    if "por_municipio" not in dfs:
        lines.append("⚠ por_municipio ausente — skip\n")
        return lines
    df = dfs["por_municipio"].dropna(subset=["ANO", "MES"]).copy()
    if df.empty:
        lines.append("⚠ sem linhas em por_municipio\n")
        return lines

    grp = df.groupby(["CD_MUN", "NM_MUN", "NATUREZA_APURADA"], observed=True)
    # stats por série (aplica só onde há ≥12 meses e desvio > 0)
    stats = grp["N"].agg(["count", "mean", "std"]).reset_index()
    stats = stats[(stats["count"] >= 12) & (stats["std"].fillna(0) > 0)]
    if stats.empty:
        lines.append("Nenhuma série com histórico suficiente para z-score\n")
        return lines

    merged = df.merge(stats, on=["CD_MUN", "NM_MUN", "NATUREZA_APURADA"], how="inner")
    merged["z"] = (merged["N"] - merged["mean"]) / merged["std"]
    hits = merged[merged["z"].abs() >= z_thresh].sort_values("z", ascending=False).head(top)
    if hits.empty:
        lines.append(f"✓ Nenhum registro com |z| ≥ {z_thresh}")
    else:
        lines.append(f"Top {len(hits)} picos (maior z primeiro):\n")
        lines.append("| Município | Natureza | ANO-MES | N | média | z |")
        lines.append("|-----------|----------|---------|---:|------:|---:|")
        for r in hits.itertuples(index=False):
            ano = int(r.ANO); mes = int(r.MES)
            nat = str(r.NATUREZA_APURADA)[:50]
            nm = str(r.NM_MUN)[:30]
            lines.append(
                f"| {nm} | {nat} | {ano}-{mes:02d} | {int(r.N):,} | {r.mean:.1f} | {r.z:+.2f} |"
            )
    lines.append("")
    return lines


def check_denominators(dfs: dict[str, pd.DataFrame]) -> list[str]:
    """População zerada/ausente bloqueia cálculo de taxa por 100k."""
    lines = ["## 6. Denominadores populacionais\n"]

    if "por_companhia" in dfs:
        df = dfs["por_companhia"]
        # OPMCOD_CIA é o ID único de cada CIA no geojson (360 polígonos).
        # Fallback: (OPM_CIA + btl_CIA) quando OPMCOD não estiver presente
        # (agregados antigos, gerados antes do patch).
        if "OPMCOD_CIA" in df.columns:
            key_cols = ["OPMCOD_CIA"]
        else:
            key_cols = [c for c in ("OPM_CIA", "btl_CIA") if c in df.columns]
        if "populacao_CIA" in df.columns and key_cols:
            # dropna=True (default) exclui o bucket NaN da chave — são pontos
            # que não caíram em nenhum polígono, não uma CIA "sem população".
            pop = df.dropna(subset=key_cols).groupby(
                key_cols, observed=True
            )["populacao_CIA"].first()
            n_total = len(pop)
            n_null = int(pop.isna().sum())
            n_zero = int((pop.fillna(-1) == 0).sum())
            key_lbl = " + ".join(key_cols)
            lines.append(f"- Companhias distintas (por {key_lbl}): **{n_total}**")
            lines.append(f"  - população NULL: {n_null} ({100*n_null/max(n_total,1):.1f}%)"
                         + (" 🚨" if n_null > 0 else ""))
            lines.append(f"  - população zero: {n_zero} ({100*n_zero/max(n_total,1):.1f}%)"
                         + (" 🚨" if n_zero > 0 else ""))
            # Pontos não cobertos (bucket NaN da chave) — relata, não alarma
            unmatched = df[df[key_cols[0]].isna()]["N"].sum()
            if unmatched:
                lines.append(f"  - pontos não atribuídos a nenhuma CIA: {int(unmatched):,} "
                             "(caíram fora dos polígonos do geojson)")
        else:
            lines.append("- por_companhia sem chave identificadora ou populacao_CIA")

    if "por_setor" in dfs:
        df = dfs["por_setor"]
        if "sc_cod" in df.columns:
            n_setores = df["sc_cod"].nunique()
            n_null = int(df["sc_cod"].isna().sum())
            lines.append(f"- Setores censitários distintos: **{n_setores:,}**")
            lines.append(f"  - linhas sem setor (sc_cod NULL): {n_null:,}")

    lines.append("")
    return lines


# ---------- Main ----------

def main() -> int:
    setup_logging()
    if not AGGREGATES.exists():
        log.error("Diretório de agregados não existe: %s", AGGREGATES)
        return 1

    log.info("Lendo agregados em %s", AGGREGATES)
    dfs = load_all()
    if not dfs:
        log.error("Nenhum agregado encontrado — rodar aggregate.py primeiro")
        return 1

    report: list[str] = [
        "# QA dos agregados — data/aggregates/\n",
        f"_Gerado via `pipeline/qa_aggregates.py`_\n",
    ]
    report += check_totals(dfs)
    report += check_geo_coverage(dfs)
    report += check_temporal_gaps(dfs)
    report += check_top_naturezas(dfs)
    report += check_outliers(dfs)
    report += check_denominators(dfs)

    text = "\n".join(report)
    REPORT_FILE.write_text(text, encoding="utf-8")
    print()
    print(text)
    print(f"\n📄 Relatório salvo em: {REPORT_FILE}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
