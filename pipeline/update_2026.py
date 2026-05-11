"""Ingestão incremental: processa apenas os arquivos *_2026.xlsx e re-gera agregados.

Uso:
    python pipeline/update_2026.py
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import RAW_SSP, PROCESSED, setup_logging, read_xlsx_streaming
import ingest_sp_dados_criminais as ssp
import ingest_subtraidos as sub
import aggregate
import aggregate_hora_dia
import build_sample

log = logging.getLogger(__name__)

YEAR = "2026"


def ingest_ssp_2026() -> None:
    out = PROCESSED / "sp_dados_criminais"
    out.mkdir(parents=True, exist_ok=True)
    files = sorted(RAW_SSP.glob(f"SPDadosCriminais_{YEAR}.xlsx"))
    if not files:
        log.error("Nenhum SPDadosCriminais_%s.xlsx encontrado em %s", YEAR, RAW_SSP)
        return
    total = 0
    for p in files:
        total += ssp.ingest_file(p, out)
    log.info("SSP %s: %s registros.", YEAR, f"{total:,}")


def ingest_subtraidos_2026() -> None:
    families = {
        "celulares": (f"CelularesSubtraidos_{YEAR}.xlsx", "celulares_subtraidos"),
        "veiculos":  (f"VeiculosSubtraidos_{YEAR}.xlsx",  "veiculos_subtraidos"),
        "objetos":   (f"ObjetosSubtraidos_{YEAR}.xlsx",   "objetos_subtraidos"),
    }
    for key, (pat, out_name) in families.items():
        out = PROCESSED / out_name
        out.mkdir(parents=True, exist_ok=True)
        files = sorted(RAW_SSP.glob(pat))
        if not files:
            log.warning("Nenhum arquivo para %s %s", key, YEAR)
            continue
        for p in files:
            log.info("Ingerindo %s", p.name)
            total = 0
            for chunk in read_xlsx_streaming(p, chunksize=40_000):
                t = sub.transform_chunk(chunk)
                if t.empty:
                    continue
                t.to_parquet(out, index=False, partition_cols=["ANO", "MES"], engine="pyarrow")
                total += len(t)
                log.info("  ↳ %s linhas acumuladas", f"{total:,}")
            log.info("%s: %s linhas.", out_name, f"{total:,}")


def main() -> None:
    setup_logging()
    log.info("=== 1/4  SPDadosCriminais %s ===", YEAR)
    ingest_ssp_2026()
    log.info("=== 2/4  Subtraídos %s ===", YEAR)
    ingest_subtraidos_2026()
    log.info("=== 3/4  Agregação ===")
    aggregate.main()
    log.info("=== 4/4  Matriz hora×dia ===")
    aggregate_hora_dia.main()
    log.info("=== 5/5  Amostra (mapa de pontos) ===")
    build_sample.main()
    log.info("Atualização 2026 concluída.")


if __name__ == "__main__":
    main()
