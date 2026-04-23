"""Orquestrador: roda ingestão + agregação em sequência.

Uso:
    python pipeline/run_all.py              # tudo
    python pipeline/run_all.py --only ssp   # só a base principal
"""
from __future__ import annotations

import argparse
import logging

from common import setup_logging
import ingest_sp_dados_criminais
import ingest_subtraidos
import aggregate
import aggregate_hora_dia
import prepare_geo

log = logging.getLogger(__name__)


def main() -> None:
    setup_logging()
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--only",
        choices=["geo", "ssp", "subtraidos", "aggregate", "hora_dia"],
        default=None,
    )
    args = ap.parse_args()

    if args.only in (None, "geo"):
        log.info("=== 1/5  Preparação do CENSO (geoparquet simplificado) ===")
        prepare_geo.main()
    if args.only in (None, "ssp"):
        log.info("=== 2/5  SPDadosCriminais ===")
        ingest_sp_dados_criminais.main()
    if args.only in (None, "subtraidos"):
        log.info("=== 3/5  Subtraídos (celulares/veículos/objetos) ===")
        ingest_subtraidos.main()
    if args.only in (None, "aggregate"):
        log.info("=== 4/5  Agregação + sjoin ===")
        aggregate.main()
    if args.only in (None, "hora_dia"):
        log.info("=== 5/5  Matriz Dia × Faixa Hora × DESC_PERIODO ===")
        aggregate_hora_dia.main()
    log.info("Pipeline completo.")


if __name__ == "__main__":
    main()
