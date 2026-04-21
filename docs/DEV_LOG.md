# Dev Log — Portal de Análise Criminal SP (InsightGeoLab AI)

**Sessão:** 2026-04-21
**Autor:** Irai M Carneiro Nt (Sgt PM, 2ª CIPM)
**Repo:** https://github.com/IMCNet0/AnaliseCriminal
**Deploy:** https://insightgeolab-analisecriminal-sp.streamlit.app/

---

## Resumo executivo

Portal público de análise criminal do estado de São Paulo construído sobre os dados abertos da SSP-SP. Stack: Streamlit + GeoPandas + Plotly + scikit-learn + statsmodels. Deploy no Streamlit Community Cloud (plano gratuito, ~1 GB RAM).

Esta sessão cobriu a reta final do projeto: estabilização do pipeline estatístico, validação (QA), preparação do runtime leve, primeiro deploy público, e duas ondas de otimização após o app encontrar limites de memória no Cloud.

---

## Fase 1 — Estabilização do pipeline de agregação

### Problema 1.1: `pq.read_table()` falhava ao combinar partições

**Sintoma.** `pipeline/repair_parquet.py` quebrava com:

```
Unable to merge: Field ANO has incompatible types: int32 vs dictionary<int32>
```

**Causa-raiz.** A partir do pyarrow 14, `pq.read_table()` aplica `partitioning="hive"` por padrão. Ao ler um arquivo `ANO=2022/MES=1/xxx.parquet`, o pyarrow tenta injetar `ANO` e `MES` a partir do caminho como `dictionary<int32>`, enquanto o parquet interno já tem essas colunas como `int32` puro. O merge silenciosamente falha.

**Fix.** Usar `pq.ParquetFile(str(pfile)).read()`, que ignora o caminho e lê apenas o conteúdo do arquivo.

### Problema 1.2: `pd.read_parquet()` sobre dataset particionado

**Sintoma.** `pipeline/aggregate.py::load_base()` quebrava com:

```
NotImplementedError: dictionary<values=int32, indices=int32>
```

**Causa-raiz.** Mesmo mecanismo: `pd.read_parquet()` infere ANO/MES do diretório como dictionary, e pandas não consegue converter pra `Int16`.

**Fix.** Trocar por `pyarrow.dataset` com schema explícito:

```python
partitioning = ds.partitioning(
    pa.schema([("ANO", pa.int32()), ("MES", pa.int32())]),
    flavor="hive",
)
dataset = ds.dataset(str(path), format="parquet", partitioning=partitioning)
table = dataset.to_table(columns=cols)
df = table.to_pandas()
```

Com o schema declarado, o pyarrow respeita int32 puro e o cast para Int16/Int8 funciona.

### Problema 1.3: CIAs aparecendo como 8 em vez de 360

**Sintoma.** QA reportava apenas 8 companhias agregadas.

**Causa-raiz (primeira hipótese, errada).** Achei que era `OPM_CIA` ambíguo. Ele é, mas não é a causa. A SSP-SP usa `OPMCOD` como chave única para cada polígono (9 dígitos). Há 360 OPMCODs únicos para CIAs no estado — cada batalhão tem suas próprias "1ª CIA", "2ª CIA" etc., e só o OPMCOD desambigua.

**Orientação do usuário.** "Considere que o campo `OPMCOD` de `CIA_PMESP` será único para cada polígono!" Decisão de domínio, não dedutível do código.

**Fix.** 
- `pipeline/aggregate.py::LAYERS["companhia"]` — incluir `OPMCOD` no `keep` e renomear para `OPMCOD_CIA`.
- Groupby por `OPMCOD_CIA` (não `OPM_CIA`).
- `app/lib/data.py::por_companhia()` — cria `LABEL_CIA = "{OPM_CIA} / {btl_CIA}"` para UI legível (ex.: "3ªCIA / 7ºBPM/M").
- `app/lib/geo.py` — trocar chave em `LAYERS` e `JOIN_KEYS_IN_DATA` de `"OPM"` para `"OPMCOD"`/`"OPMCOD_CIA"`.
- `app/pages/3_Rankings.py` — usar `LABEL_CIA` como coluna de display.

Salvo na memória: `project_pmesp_keys.md` — armadilhas de chave em cada camada.

### Problema 1.4: Naturezas duplicadas (TRÁFICO vs TRAFICO)

**Causa-raiz.** A base da SSP-SP tem grafias inconsistentes da mesma natureza (com/sem acento, variações de caixa). O filtro da UI recebia uma lista e não casava com a base pontual.

**Fix.** `pipeline/aggregate.py::normalize_natureza()` aplica `unidecode + upper + collapse whitespace`. `pipeline/build_sample.py` aplica a mesma normalização. Em runtime, `app/lib/data.py::_norm_natureza()` renormaliza (idempotente) pra robustez.

### Problema 1.5: Sjoin duplicates (12k em BTL, 520k em CMDO)

**Causa-raiz.** Polígonos de BTL e CMDO se sobrepõem em algumas regiões (Grande SP). Um ponto sobre a fronteira casa com dois polígonos, e o `gpd.sjoin` retorna duas linhas, inflando a contagem.

**Fix.** Deduplicar pelo índice do lado esquerdo (pontos) logo após o sjoin:

```python
if joined.index.has_duplicates:
    n_dup = int(joined.index.duplicated(keep="first").sum())
    log.info("  · %s correspondências duplicadas removidas", f"{n_dup:,}")
    joined = joined[~joined.index.duplicated(keep="first")]
```

---

## Fase 2 — Preparação para deploy

### Estratégia de runtime enxuto

A base pós-ingestão `data/processed/sp_dados_criminais/` tem ~5M pontos e 593 MB — inviável para Streamlit Cloud (limite de 1 GB no repo, cold start lento). Três opções foram consideradas:

| Opção | Prós | Contras |
|---|---|---|
| 1. Desligar mapa de pontos | Repo enxuto (15 MB) | Perde a funcionalidade |
| 2. Amostra commitada | Pontos funcionam | Não é a base completa |
| 3. Google Drive externo | Base completa | Cold start depende de rede |

**Decisão: opção 2.** Pontos individuais continuam funcionando para análises espaciais ao nível de bairro, com perda aceitável de resolução.

### `pipeline/build_sample.py`

Script novo que:
- Lê cada partição `ANO/MES` da base completa via `pyarrow.dataset` com schema explícito.
- Mantém apenas as colunas consumidas pelo runtime (`LATITUDE`, `LONGITUDE`, `NATUREZA_APURADA`, `NOME_MUNICIPIO`, `DATA_OCORRENCIA_BO`, `COORDS_VALIDAS`).
- Filtra pontos com coordenada válida.
- Amostra até 5.000 pontos por partição (sorteio sem reposição, seed fixa).
- Normaliza `NATUREZA_APURADA` antes de escrever.
- Escreve em `data/processed_sample/sp_dados_criminais/ANO=X/MES=Y/part.parquet`.

**Resultado:** 250.000 pontos em 50 partições, **5,4 MB em disco** (snappy + dict encoding entregou 23 B/ponto).

### Ajustes no `.gitignore`

```
data/raw/                         # microdados originais (pesado)
data/processed/                   # base completa (593 MB)
data/geo/CENSO.json               # GeoJSON bruto do censo (402 MB)
data/aggregates/_qa_report.md     # regenerável
*.xlsx / *.xls                    # originais da SSP
*.log / pipeline.log              # logs de execução
* - Copia.* / *_backup.* / *.bak  # backups do Windows
```

### Problema 2.1: `pontos()` quebrando com ArrowNotImplementedError

**Sintoma.** Depois de apontar o `pontos()` para a amostra, toda abertura de partição falhava com `pyarrow.lib.ArrowNotImplementedError`.

**Causa-raiz (dupla).**
1. `pd.read_parquet(filters=...)` sobre dataset Hive usa partition inference com dictionary — mesmo bug da Fase 1.
2. Possível schema drift entre partições (`NATUREZA_APURADA:string` em uma, `null` em outra com todos NaN) — o unify falha ao consolidar múltiplos arquivos.

**Fix.** Ler direto o arquivo da partição específica via `pq.ParquetFile`, bypass total do dataset multi-arquivo:

```python
part_dir = path / f"ANO={int(ano)}" / f"MES={int(mes)}"
parquet_files = sorted(part_dir.rglob("*.parquet"))
tables = []
for pfile in parquet_files:
    pf = pq.ParquetFile(str(pfile))
    cols = [c for c in WANT if c in pf.schema_arrow.names]
    tables.append(pf.read(columns=cols))
table = pa.concat_tables(tables, promote_options="default")
```

Também prefere a amostra e faz fallback para base completa em dev local.

### Problema 2.2: Coropletico não colorindo em DP e Companhia

**Sintoma.** Após carregar o mapa:

```
Exemplos geo: ['130409', '100435', '120427']
Exemplos agregado: ['10007.0', '10101.0', '10102.0']
```

**Causa-raiz.** Os agregados escreveram `DpGeoCod` e `OPMCOD_CIA` como float (para acomodar NaN). Ao virar string, preservaram o `.0` de final. O merge `left_on=DpGeoCod, right_on=DpGeoCod` comparava `"130409"` com `"130409.0"` e nenhum polígono casava.

**Fix.** Em `app/pages/1_Mapa.py`, normalização via `_norm_key()`:

```python
def _norm_key(s: pd.Series) -> pd.Series:
    nums = pd.to_numeric(s, errors="coerce")
    valid = nums.dropna()
    if not valid.empty and (valid == valid.astype("int64")).all():
        return nums.astype("Int64").astype("string")
    return s.astype("string").str.strip().str.upper()
```

Detecta se é inteiro armazenado como float → `Int64 → string` (sem `.0`). Caso contrário, string normalizada.

Acrescentado um warning de diagnóstico na UI que mostra overlap entre as chaves dos dois lados quando zero polígonos recebem valor — facilita achar a próxima regressão.

---

## Fase 3 — Git e primeiro push

### Primeiro commit

- `git init -b main` + remote → `IMCNet0/AnaliseCriminal`
- 97 arquivos, 136 MB no stage
- Maior arquivo: `data/geo/CENSO_simplified.parquet` (39 MB, dentro do limite de 100 MB do GitHub)
- Push comprimiu para 61 MB

### Acidente na colagem no PowerShell

Colar um bloco multi-linha juntou o `git add .` do 3º comando com o remote add do 2º, gerando erro no parser. Lição: no PowerShell, colar comando-a-comando em vez de blocos.

---

## Fase 4 — Deploy no Streamlit Community Cloud

### Deploy 1 — `fiona` + Python 3.14

**Sintoma.**

```
× Failed to download and build fiona==1.10.1
CRITICAL: A GDAL API version must be specified. Provide a path
to gdal-config using a GDAL_CONFIG environment variable.
```

**Causa-raiz (dupla).**
1. O Cloud pegou **Python 3.14.4** (beta), sem wheels prontos para várias libs → instalação partia para compilar do source.
2. `fiona` precisa da lib C do sistema `gdal-config`, que o container do Cloud não tem instalada por default.

**Fix.**
- **`.python-version`** novo com `3.12` — força uma versão estável com wheels prontos para numpy/pandas/pyarrow/geopandas.
- **`requirements.txt`**: remover `fiona` e `rtree`. A partir do GeoPandas 1.0, o engine default é `pyogrio`, que traz GDAL embutido no wheel. `shapely 2` tem `STRtree` nativo, dispensando `rtree`.
- Pinos de sanidade: `streamlit<2`, `pandas<3`, `numpy<3` para evitar major versions em beta.

Verificado com `grep` que nenhum código importa `fiona` ou `rtree` diretamente.

Salvo na memória: `project_streamlit_cloud_geo.md` — receita reutilizável para qualquer projeto geoespacial no Cloud.

### Deploy 2 — Build OK, mas app crashou com OOM

**Sintoma.** Build completou (`🔄 Updated app!`). O app renderizou algumas páginas, e depois de ~1 minuto:

```
❗️ The service has encountered an error while checking the health of the
Streamlit app: Get "http://localhost:8501/healthz": connection reset by peer
```

**Causa-raiz.** Limite de ~1 GB RAM do plano gratuito estourou. Principais consumidores identificados:
- `prophet` + `cmdstanpy` + `stan`: ~400 MB só no import (mesmo com import lazy, o Streamlit Cloud avalia os módulos na primeira request).
- `folium` + `streamlit-folium`: ~40 MB sem uso algum no código.
- Coropletico com Setor Censitário: ~250k polígonos quando renderizado estouram a tela mesmo após simplificação.

**Fix — passe 1: trim de dependências.**
- `requirements.txt`: remover `prophet`, `folium`, `streamlit-folium`.
- `app/lib/stats.py`: nova função `prophet_available()` detecta instalação; `forecast_prophet()` dispara erro amigável se ausente.
- `app/pages/2_Series_Temporais.py`: seletor "Método" mostra só `SARIMA` em produção, com caption explicando.

Economia estimada: ~500 MB.

**Fix — passe 2: enxugar UI pra reduzir carga.**

A pedido do usuário:
- `app/lib/filters.py::RECORTES`: remover "Município" (~645 polígonos) e "Setor Censitário" (~250k polígonos) da lista. Default passou de "Município" (índice 0 anterior) para **"Comando (CPA)"** (~39 polígonos, bem mais leve).
- Slider de período: default passou de "todos os anos disponíveis" para **últimos 2 anos** (≈12-24 meses), reduzindo volume de dados processados na primeira renderização.

Código do recorte e das layers de Município/Setor foi **mantido** em `data.py`, `geo.py`, `3_Rankings.py` — inofensivo em prod (nunca será consultado) e útil em dev local.

---

## Arquitetura final de dados

```
data/
├── raw/                         # (gitignored, 4.4 GB)
│   └── ssp/ + ibge/ + geo/
├── processed/                   # (gitignored, 593 MB)
│   └── sp_dados_criminais/ANO=*/MES=*/*.parquet
├── processed_sample/            # COMMITADO — alimenta drill-down (5.4 MB)
│   └── sp_dados_criminais/ANO=*/MES=*/part.parquet
├── geo/
│   ├── CENSO.json               # (gitignored, 402 MB)
│   ├── CENSO_simplified.parquet # COMMITADO (39 MB)
│   ├── BTL_PMESP.json           # COMMITADO (13 MB)
│   ├── CIA_PMESP.json           # COMMITADO (23 MB)
│   ├── CMDO_PMESP.json          # COMMITADO (9 MB)
│   └── DP.json                  # COMMITADO (29 MB)
└── aggregates/                  # COMMITADO (15 MB total)
    ├── serie_estado.parquet
    ├── por_municipio.parquet
    ├── por_batalhao.parquet
    ├── por_companhia.parquet
    ├── por_comando.parquet
    ├── por_dp.parquet
    ├── por_setor.parquet
    └── cubo_natureza.parquet
```

Repo total: ~140 MB não-comprimido. Após compactação do git: ~61 MB.

---

## Aprendizados (heurísticas para futuros projetos)

**Pyarrow / partitioning.**
- Qualquer código que toca parquet particionado tem que declarar o schema do particionamento explicitamente ou ler arquivo-a-arquivo. A inferência default traz o tipo como `dictionary<int32>` e quebra merges/casts.
- Quando o schema varia entre partições (ex.: coluna toda NaN em uma, string em outra), `pa.concat_tables(..., promote_options="default")` resolve.

**GeoPandas.**
- Desde a 1.0, `pyogrio` é o default e o `fiona` é optional. Em qualquer deploy sem lib C do sistema (Streamlit Cloud, Vercel, etc.), `fiona` sempre falha a compilar. Remover do requirements e migrar para pyogrio resolve.
- `rtree` é redundante desde `shapely 2` com `STRtree` nativo.

**Streamlit Community Cloud.**
- Default do Python em 2026 pegou 3.14 (beta). Pin em `.python-version` com `3.12` evita roleta russa de wheels.
- Limite de ~1 GB RAM é apertado. `prophet` + `stan` sozinhos ocupam ~400 MB. `folium` não-usado é 40 MB. Auditoria do `requirements.txt` recuperou ~500 MB.
- `streamlit-folium`, `folium`, `prophet` devem ser removidos se não forem ativos em runtime. Lazy imports não ajudam no Cloud porque o runtime avalia os módulos.

**Chaves compostas em geo-UI.**
- Float-com-`.0` é um bug silencioso comum em join de chaves inteiras entre GeoDataFrame e DataFrame de agregados. Sempre normalizar antes de fazer merge. `pd.to_numeric(s) + astype(Int64).astype(string)` é o atalho seguro.

**Domínio SSP-SP / PMESP.**
- `OPMCOD` é a chave única para camadas da PMESP por batalhão/companhia. `OPM` sozinho só desambigua em batalhões com prefixo único — em CIA dá 8 grupos em vez de 360.
- BTL e CMDO têm polígonos sobrepostos na Grande SP — sjoin sem dedup infla contagens.
- Naturezas da SSP-SP vêm com acentuação inconsistente — canonicalização via unidecode é obrigatória antes de comparar.

---

## Deliverable final

**URL:** https://insightgeolab-analisecriminal-sp.streamlit.app/

**Páginas:**
1. **Home** — KPIs executivos estaduais.
2. **Mapa** — coropletico por 4 recortes (Comando, DP, Batalhão, Companhia) + drill-down opcional para pontos individuais de um mês.
3. **Séries Temporais** — evolução mensal + decomposição STL + previsão SARIMA.
4. **Rankings** — top-N por recorte, barras horizontais.
5. **Laboratório Estatístico** — outlier detection (z-score, IQR, Isolation Forest), clusterização (KMeans, hierárquico), correlações, testes de hipótese.
6. **Metodologia** — explicação das fontes, transformações e métodos.

**Escopo temporal:** 2022-01 a 2026-02 (50 meses, default: últimos ~2 anos).
**Escopo espacial:** estado de SP inteiro.
**Atualização:** rodar `pipeline/run_all.py` + `pipeline/build_sample.py` quando a SSP-SP publicar novos microdados e dar push.
