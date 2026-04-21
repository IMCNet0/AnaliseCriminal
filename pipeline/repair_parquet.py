"""Repara schema drift de datasets Parquet particionados.

Três operações, todas in-place arquivo por arquivo (não mexe no que já
está coerente):

1. **Dictionary → tipo base**: converte colunas ``dictionary<...>`` para
   o ``value_type`` (geralmente ``string``). Resolve drift causado pelo
   pandas ``category`` que escolhe índice int8/int16 por chunk.

2. **Força colunas a string** (``--force-string COL1 COL2 ...``): quando
   uma coluna aparece como ``int64`` em um ano, ``double`` em outro e
   ``string`` em outro (ex.: VERSAO na base da SSP), a única forma de
   unificar é castar tudo a ``string``.

3. **Remove colunas de partição duplicadas**: se o arquivo interno
   contiver ``ANO``/``MES`` (ou qualquer coluna também presente como
   ``NAME=VALUE`` no caminho), remove do conteúdo. O pyarrow reinfere
   a partir do diretório e colide se a coluna existir nos dois lugares.

Uso:
    python pipeline/repair_parquet.py data/processed/sp_dados_criminais
    python pipeline/repair_parquet.py data/processed/celulares_subtraidos --force-string VERSAO
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def partition_cols_from_path(pfile: Path, base: Path) -> list[str]:
    """Extrai nomes de colunas de partição a partir do caminho.

    Ex.: ``data/processed/X/ANO=2023/MES=1/xyz.parquet`` → ``["ANO","MES"]``
    """
    rel = pfile.relative_to(base).parent
    return [p.split("=", 1)[0] for p in rel.parts if "=" in p]


def cast_dict_to_value_type(table: pa.Table) -> tuple[pa.Table, list[str]]:
    changed: list[str] = []
    new_cols: list[pa.ChunkedArray] = []
    new_fields: list[pa.Field] = []
    for name, col in zip(table.schema.names, table.columns):
        field = table.schema.field(name)
        if pa.types.is_dictionary(col.type):
            target = col.type.value_type
            col = col.cast(target)
            field = pa.field(name, target, nullable=field.nullable, metadata=field.metadata)
            changed.append(name)
        new_cols.append(col)
        new_fields.append(field)
    new_schema = pa.schema(new_fields, metadata=table.schema.metadata)
    return pa.Table.from_arrays(new_cols, schema=new_schema), changed


def force_string_cols(table: pa.Table, cols: list[str]) -> tuple[pa.Table, list[str]]:
    """Casta colunas específicas para ``string`` quando existirem na tabela."""
    changed: list[str] = []
    new_cols: list[pa.ChunkedArray] = []
    new_fields: list[pa.Field] = []
    for name, col in zip(table.schema.names, table.columns):
        field = table.schema.field(name)
        if name in cols and not pa.types.is_string(col.type) and not pa.types.is_large_string(col.type):
            # Alguns tipos (int/float) podem ser castados diretamente;
            # dictionary já foi tratado antes desta etapa.
            try:
                col = col.cast(pa.string())
            except pa.ArrowInvalid:
                # fallback: converter via Python (lento, mas robusto)
                import pandas as pd
                s = pd.Series(col.to_pylist(), dtype="object")
                s = s.where(s.notna(), None).astype("string")
                col = pa.array(s.tolist(), type=pa.string())
                col = pa.chunked_array([col])
            field = pa.field(name, pa.string(), nullable=field.nullable, metadata=field.metadata)
            changed.append(name)
        new_cols.append(col)
        new_fields.append(field)
    new_schema = pa.schema(new_fields, metadata=table.schema.metadata)
    return pa.Table.from_arrays(new_cols, schema=new_schema), changed


def drop_columns(table: pa.Table, cols_to_drop: list[str]) -> tuple[pa.Table, list[str]]:
    """Remove colunas se presentes (silencioso se não existirem)."""
    dropped: list[str] = []
    out = table
    for c in cols_to_drop:
        if c in out.schema.names:
            idx = out.schema.get_field_index(c)
            out = out.remove_column(idx)
            dropped.append(c)
    return out, dropped


def repair_file(pfile: Path, base: Path, force_string: list[str]):
    # IMPORTANTE: usar ParquetFile.read() em vez de pq.read_table(pfile).
    # pq.read_table() aplica hive partitioning por padrão — ele vê o caminho
    # ``ANO=2022/MES=1/xxx.parquet`` e tenta injetar ``ANO`` como
    # ``dictionary<int32>`` a partir do diretório. Se o arquivo já tem ANO
    # dentro (caso típico, porque pandas.to_parquet(partition_cols=...)
    # escreveu a coluna ANO também no conteúdo em alguns casos), dá
    # ``Unable to merge: Field ANO has incompatible types: int32 vs
    # dictionary<values=int32, indices=int32>`` já na leitura, antes de
    # conseguir reparar qualquer coisa. ParquetFile.read() lê o arquivo cru.
    table = pq.ParquetFile(str(pfile)).read()
    all_changes: dict[str, list[str]] = {"dict": [], "force_string": [], "dropped_partition": []}

    table, c1 = cast_dict_to_value_type(table)
    all_changes["dict"] = c1

    if force_string:
        table, c2 = force_string_cols(table, force_string)
        all_changes["force_string"] = c2

    partition_names = partition_cols_from_path(pfile, base)
    if partition_names:
        table, c3 = drop_columns(table, partition_names)
        all_changes["dropped_partition"] = c3

    total = sum(len(v) for v in all_changes.values())
    if total == 0:
        return all_changes

    tmp = pfile.with_suffix(pfile.suffix + ".tmp")
    pq.write_table(table, tmp, compression="snappy")
    tmp.replace(pfile)
    return all_changes


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("path", help="Diretório do dataset parquet particionado")
    ap.add_argument(
        "--force-string",
        nargs="+",
        default=[],
        metavar="COL",
        help="Colunas a castar para string (resolve int64/double/string mistos entre anos)",
    )
    args = ap.parse_args()

    base = Path(args.path)
    if not base.exists():
        print(f"Diretório não existe: {base}")
        return 1
    files = sorted(base.rglob("*.parquet"))
    if not files:
        print(f"Nenhum .parquet encontrado em {base}")
        return 1

    print(f"Reparando {len(files)} arquivos em {base}")
    if args.force_string:
        print(f"  Colunas forçadas a string: {args.force_string}")
    rewritten = 0
    touched_dict: set[str] = set()
    touched_force: set[str] = set()
    touched_drop: set[str] = set()
    for i, pfile in enumerate(files, 1):
        try:
            ch = repair_file(pfile, base, args.force_string)
        except Exception as e:
            print(f"  ✖ erro em {pfile.relative_to(base)}: {e}")
            continue
        if any(ch.values()):
            rewritten += 1
            touched_dict.update(ch["dict"])
            touched_force.update(ch["force_string"])
            touched_drop.update(ch["dropped_partition"])
        if i % 50 == 0 or i == len(files):
            print(f"  {i}/{len(files)} processados ({rewritten} reescritos)")

    print()
    print(f"Arquivos reescritos: {rewritten}")
    print(f"Colunas dictionary→base: {sorted(touched_dict) or 'nenhuma'}")
    if args.force_string:
        print(f"Colunas forçadas a string: {sorted(touched_force) or 'nenhuma'}")
    if touched_drop:
        print(f"Colunas de partição removidas do conteúdo: {sorted(touched_drop)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
