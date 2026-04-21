# Schema dos GeoJSON em `data/geo/`

Mapeamento dos arquivos fornecidos pelo projeto e as chaves usadas no código.

| Arquivo | Features | Chave no GeoJSON | Chave após sjoin nos dados | Observações |
|---|---|---|---|---|
| `BTL_PMESP.json` | 97 | `OPM` | `OPM_BTL` | Batalhões PMESP, traz `cmdo`, `g_cmdo` |
| `CIA_PMESP.json` | 360 | `OPM` | `OPM_CIA` | Companhias, traz `populacao`, `qtd_domic`, `area_km2` |
| `CMDO_PMESP.json` | 25 | `cmdo_label` | `CMDO` | Comandos/CPA, traz `regiao`, `DEINTER` |
| `DP.json` | 1.039 | `DpGeoCod` | `DpGeoCod` | Delegacias de Polícia |
| `CENSO.json` | 102.418 (BR) | `sc_cod` | `sc_cod` | Setor censitário IBGE 2022 + `BTL`, `CMDO`, `pop_fem`, `pop_masc`, `favela_2022` |

### Pré-processamento do CENSO.json

O arquivo bruto tem 401 MB e estoura o Streamlit Cloud (limite 1 GB RAM).
Por isso o pipeline gera automaticamente `CENSO_simplified.parquet` contendo:

1. Apenas setores de SP (filtro `CD_UF == "35"`).
2. Colunas essenciais: `sc_cod`, `CD_MUN`, `NM_MUN`, `pop_fem`, `pop_masc`, `favela_2022`, `BTL`, `CMDO`, `GDO_CMDO`, `OPM`, `AREA_KM2`, `geometry`.
3. Geometrias simplificadas (tolerância 0.0001° ≈ 11 m).
4. Escrito como GeoParquet (~10× menor que o JSON, leitura muito mais rápida).

Gerar:

```bash
python pipeline/prepare_geo.py     # ou python pipeline/run_all.py --only geo
```

### Estratégia de *join*

O código **não** casa strings entre a SSP-SP e os geojsons (evita problemas de
grafia "7ºBPM/M" × "07º BPM/M"). Em vez disso, faz **point-in-polygon** a partir
das coordenadas de cada BO, atribuindo o batalhão/companhia/comando/DP/setor
correto via geometria. Isso também funciona em BOs cuja coluna `BTL`/`CIA`/`CMD`
veio vazia.
