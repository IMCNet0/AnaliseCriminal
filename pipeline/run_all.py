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
import prepare_geo

log = logging.getLogger(__name__)


def main() -> None:
    setup_logging()
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["geo", "ssp", "subtraidos", "aggregate"], default=None)
    args = ap.parse_args()

    if args.only in (None, "geo"):
        log.info("=== 1/4  Preparação do CENSO (geoparquet simplificado) ===")
        prepare_geo.main()
    if args.only in (None, "ssp"):
        log.info("=== 2/4  SPDadosCriminais ===")
        ingest_sp_dados_criminais.main()
    if args.only in (None, "subtraidos"):
        log.info("=== 3/4  Subtraídos (celulares/veículos/objetos) ===")
        ingest_subtraidos.main()
    if args.only in (None, "aggregate"):
        log.info("=== 4/4  Agregação + sjoin ===")
        aggregate.main()
    log.info("Pipeline completo.")


if __name__ == "__main__":
    main()
