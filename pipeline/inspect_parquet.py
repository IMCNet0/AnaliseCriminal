"""Inspeciona schemas de um dataset Parquet particionado.

Uso:
    python pipeline/inspect_parquet.py data/processed/sp_dados_criminais
    python pipeline/inspect_parquet.py data/processed/celulares_subtraidos

Para cada coluna, lista os tipos distintos encontrados entre as partições.
Se uma coluna tiver mais de um tipo, isso é *schema drift* — causa comum
de erros ``ArrowInvalid: Integer value X not in range`` na leitura unificada.
"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq


def main(path_str: str) -> int:
    base = Path(path_str)
    if not base.exists():
        print(f"Diretório não existe: {base}")
        return 1

    files = sorted(base.rglob("*.parquet"))
    if not files:
        print(f"Nenhum .parquet encontrado em {base}")
        return 1

    print(f"Arquivos inspecionados: {len(files)} em {base}\n")

    schemas = defaultdict(lambda: defaultdict(list))
    for pfile in files:
        try:
            s = pq.read_schema(pfile)
        except Exception as e:
            print(f"  ✖ erro lendo {pfile.relative_to(base)}: {e}")
            continue
        part = " / ".join(pfile.relative_to(base).parts[:-1])
        for name, type_ in zip(s.names, s.types):
            schemas[name][str(type_)].append(part or "(raiz)")

    drift_cols = {c: tm for c, tm in schemas.items() if len(tm) > 1}

    if not drift_cols:
        print("✓ Nenhum schema drift — todas as colunas têm tipo consistente.")
        print("\nTipos por coluna:")
        for col, tmap in sorted(schemas.items()):
            (t,) = tmap.keys()
            print(f"  {col:30s}  {t}")
        return 0

    print(f"✖ SCHEMA DRIFT detectado em {len(drift_cols)} coluna(s):\n")
    for col, tmap in sorted(drift_cols.items()):
        print(f"  {col}:")
        for t, parts in sorted(tmap.items(), key=lambda kv: -len(kv[1])):
            print(f"    {t:30s}  {len(parts)} partição(ões)")
            for p in parts[:5]:
                print(f"        · {p}")
            if len(parts) > 5:
                print(f"        ... e mais {len(parts) - 5}")
        print()

    print("Solução: apagar as partições divergentes (ou o diretório inteiro)")
    print("e re-ingerir com o pipeline atual, que garante schema estável.")
    return 2


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "data/processed/sp_dados_criminais"
    sys.exit(main(target))
