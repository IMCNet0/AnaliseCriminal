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

---

## 2026-04-23 — UI overhaul (mapa folium como protagonista)

Após o deploy estabilizar, o cliente pediu uma reestruturação visual profunda:

### Pedidos do cliente
1. Logo InsideGeoLab_logo.PNG no **topo da sidebar** — área superior do corpo fica livre.
2. **Mapa vai pra Home**; gráficos da Home migram para uma nova página "GRÁFICOS".
3. KPIs continuam no topo da Home.
4. Habilitar zoom pelo scroll do mouse.
5. Campo de busca de endereço com zoom automático ao local.
6. Camadas **permanentes** PMESP (sempre visíveis):
   - Comando (CPA) — linha cinza, largura 3.
   - Batalhão PMESP — linha vermelha, largura 2.
   - Companhia PMESP — linha preta, largura 1.
   - Todas com **fundo transparente** e **rótulos halo-branco** com a cor da linha.
7. Toggle exclusivo: **coroplético OU pontos**; remover drill-down.
8. Pontos com **cores distintas por natureza**, tamanho 1.
9. Nova opção: **mapa de hotspot**.

### Decisões de arquitetura
- **Folium + streamlit-folium** voltam ao `requirements.txt`. Cabem no 1 GB do
  Streamlit Cloud porque o Prophet (~400 MB) já havia saído — sobra orçamento.
- Novo módulo centralizador: `app/lib/map_builder.py`. Todas as páginas que
  precisarem de mapa passam por `build_map(modo=..., pts_data=..., choro_data=...)`.
- `branding.py` reescrito: logo via `st.sidebar.image()` no topo + `header()`
  minimalista no corpo (só título + subtítulo da página).
- Rótulos halo-branco: `folium.map.Marker` com `DivIcon` + CSS `text-shadow`
  em 8 direções (mais robusto que `-webkit-text-stroke`, que não funciona
  uniformemente nos navegadores mobile).
- Busca de endereço via **Nominatim** (OSM) com `viewbox`/`bounded=1` pra
  evitar homônimos fora de SP; cache `@st.cache_data(ttl=86400)`.
- Modo pontos: `CircleMarker(radius=1)` com paleta categórica D3; usa
  `MarkerCluster` quando >3k pontos (evita travar browser).
- Modo hotspot: `folium.plugins.HeatMap` — densidade kernel em pixels.

### Estrutura de páginas (pós-overhaul)
```
app/
├── Home.py              → KPIs + MAPA interativo (Home é a nova vitrine)
├── pages/
│   ├── 1_Graficos.py    → (NOVA) série mensal + top-20 naturezas
│   ├── 2_Series_Temporais.py (STL + SARIMA)
│   ├── 3_Rankings.py
│   ├── 4_Laboratorio_Estatistico.py
│   └── 5_Metodologia.py
└── lib/
    ├── branding.py      (reescrito: logo na sidebar)
    ├── map_builder.py   (NOVO: folium orchestrator)
    ├── data.py / filters.py / geo.py / stats.py / downloads.py
```
A antiga `pages/1_Mapa.py` foi **removida** — o mapa agora é a Home.

### Trade-offs & riscos
- **Rótulos PMESP**: 25 Comandos + 97 BTLs + 360 CIAs = 482 DivIcons. Em zoom-out
  isso satura visualmente. Resolvido via checkbox "Exibir rótulos" (default OFF).
- **MarkerCluster**: ativado só acima de 3k pontos — mantém UX de clique
  individual em mês/natureza pequenos.
- **Scroll-zoom**: `scrollWheelZoom=True` no `folium.Map`. Sem `Ctrl` = ainda
  captura o scroll da página; se incomodar, habilita-se `scrollWheelZoom="center"`
  ou plugin de bloqueio explícito.
- **Nominatim rate limit**: 1 req/s público. Cache de 24h no `@st.cache_data`
  absorve uso moderado. Se crescer, trocar por MapTiler/Mapbox com token.

---

## 2026-04-23 — UI refinements (round 3)

Oito ajustes pedidos pelo cliente, agrupados por eixo:

### Navegação + identidade
- **`st.logo()` nativa** (Streamlit 1.35+) substituiu o hack com CSS flex-order
  em `branding.py`. Agora o logo vive no `stSidebarHeader` do próprio Streamlit,
  sempre acima do menu multipage `stSidebarNav`. Limpa e independente do DOM
  interno do Streamlit (que muda entre versões).

### Mapa
- **`fit_bounds` no polígono selecionado.** `pmesp_bounds(recorte, label)` usa
  `gdf.geometry.total_bounds` e passa `(south, west, north, east)` para
  `build_map(fit_bounds=...)` → `fmap.fit_bounds([[s,w],[n,e]])`. Em vez de
  centróide + nível fixo (9/11/13), o leaflet calcula o zoom que enquadra
  exatamente o retângulo — BTL pequeno da capital e BTL grande do interior
  recebem zoom proporcional. Bounds são consumidos e limpos após aplicar
  (`_pending_bounds = None`) pra não sequestrar pan/zoom manual.
- **Filtros hierárquicos CPA → BTL → CIA.** `btl_options_by_cpa(cpa)` e
  `cia_options_by_btl(btl)` filtram pelas colunas parent (`cmdo_label`, `btl`)
  dos GeoJSONs. Quando o pai muda, os filhos são zerados em `st.session_state`
  para evitar combinações inválidas. As helpers funcionam com `None` (sem
  pai → lista completa), simplificando o caso "nenhum" no topo da lista.
- **Bug dos pontos corrigido.** `_inject_zoom_gate` antigo adicionava
  `<script>window["MAP_NAME"].on(...)</script>` no `<html>` do root, mas o
  script rodava ANTES do `<script>` do folium declarar `var MAP_NAME = L.map(...)`
  — então `window.MAP_NAME` era sempre `undefined` e o gate nunca ligava a
  camada, deixando os pontos invisíveis. Substituído por um `branca.element.MacroElement`
  com macro `script(this, kwargs)` — o folium renderiza esse macro DENTRO do
  mesmo `<script>` onde a var do mapa já está declarada, então `m`/`l` referem
  variáveis locais (não `window.X`). Resultado: pontos aparecem a partir do
  zoom 6 como esperado.
- **Ano/Mês globais na sidebar.** Campo "Mês" (0=Todos, 1–12) adicionado ao
  `sidebar_filters()`. `GlobalFilters.mask_date` respeita `self.mes`. Na Home,
  removidos selectbox de ano/mês local — pontos são carregados em loop
  ano × mês a partir dos filtros globais. Menos estado, menos surpresa.
- **Rótulos PMESP via LayerControl.** Todos os DivIcons (CPA + BTL + CIA, ~482
  labels) entram em UM FeatureGroup "🔤 Rótulos PMESP", `show=False`. Usuário
  liga/desliga pelo controle de camadas do próprio mapa — sem rerun do
  Streamlit. Checkbox externa removida da Home.
- **Legenda unificada abaixo do mapa.** `legenda_unificada_html()` renderiza
  um bloco HTML único contendo: (a) linhas indicadoras das 3 camadas PMESP
  com espessura/cor reais, (b) rampa YlOrRd em `linear-gradient` CSS com
  rótulos em `int(round(v))` nos 5 pontos da escala (nada de decimais), e
  (c) cores dos pontos quando no modo "Pontos". A rampa antiga do branca
  (`palette.add_to(fmap)`) foi desligada.

### Arquivos tocados

- `app/lib/branding.py` — `st.logo()` nativa, CSS flex-order removido.
- `app/lib/filters.py` — dataclass `GlobalFilters` ganhou `mes: int`, dict
  `MESES_PT`, novo selectbox na sidebar.
- `app/lib/map_builder.py` — `pmesp_bounds`, `btl_options_by_cpa`,
  `cia_options_by_btl`, `_ZoomGate(MacroElement)`, rótulos num único FG,
  `build_map(fit_bounds=...)`, `legenda_unificada_html`.
- `app/Home.py` — controles hierárquicos com `session_state`, consumo de
  `_pending_bounds` via fit_bounds, remoção do Ano/Mês local, remoção do
  checkbox de rótulos, legenda unificada.

---

## Fase 9 — Refinamentos de UX pós-lançamento (2026-04-23)

14 ajustes solicitados pelo cliente depois do primeiro uso em campo. Agrupei
por área para manter o log navegável.

### Sidebar & branding

- **Logo em largura total.** CSS em `branding.py` zera `max-width/max-height`
  do `stSidebarHeader` e do `stLogo`, força `width: 100%` na `img` interna e
  zera padding dos wrappers. O logo nativo (`st.logo()`) agora ocupa toda a
  largura útil da sidebar — sem hacks de flex-order.
- **Filtros hierárquicos CPA→BTL→CIA migrados pra sidebar.** Toda a cadeia de
  seletores que morava na janela do mapa foi para `sidebar_filters()`. O
  recorte do coroplético também saiu da janela — agora só existe na sidebar,
  e é lido via `f.recorte`. Menos duplicidade, menos estado conflitando.
- **Filtro de período → `date_input`.** `GlobalFilters` virou um dataclass com
  `data_ini: date, data_fim: date`. Properties `ano_ini/ano_fim/mes` mantêm a
  API antiga (backward compat com páginas que ainda pensam em ano+mês).
  `mask_date()` aceita coluna `DATA` direta OU reconstrói a partir de ANO+MES.
  Default = últimos 2 anos do histórico.

### Mapa

- **Camadas de dados acima das PMESP.** `build_map()` agora chama
  `_add_pmesp_layers()` ANTES das camadas de dados (pontos/coroplético/hotspot).
  No Leaflet, ordem de adição = ordem de empilhamento; últimas sobem. Resultado:
  CPA/BTL/CIA ficam como contorno de fundo e os dados ficam por cima, legíveis.
- **Regra de negócio: temáticos só com variáveis.** Home checa
  `tem_natureza = bool(f.naturezas)` antes de tentar carregar pontos/coroplético.
  Sem natureza selecionada → placeholder limpo (mapa base + PMESP). Evita
  consultas caras e mapa "vazio por acidente".
- **Múltiplos indicadores plotados corretamente.** Bug antigo:
  `nat_sel = f.naturezas[0] if f.naturezas else None` — só passava o primeiro.
  Substituído por `for nat in f.naturezas: frames.append(data.pontos(ano, mes, nat))`
  e `pd.concat(frames)` antes de renderizar. Agora o usuário vê todos os
  indicadores que escolheu, cada um com sua cor.
- **Zoom não reseta mais.** Quando `_pending_bounds` dispara (ex.: seleção de
  BTL), calculamos imediatamente o zoom estimado via `zoom_for_bounds(bounds)`
  e gravamos em `st.session_state.map_zoom` ANTES do rerun. Assim, quando o
  Streamlit reconstrói o mapa, o zoom já reflete o novo envelope — sem voltar
  pro default.
  ```python
  def zoom_for_bounds(bounds):
      south, west, north, east = bounds
      lat_diff = max(abs(north - south), 1e-6)
      lon_diff_eq = abs(east - west) * math.cos(math.radians((north+south)/2))
      deg = max(lat_diff, lon_diff_eq)
      return max(5, min(17, int(round(math.log2(360*0.8/deg)))))
  ```
- **Autocomplete inline no endereço.** `geocode_many()` pega top-7 do Nominatim;
  os resultados entram num `<datalist id="endereco-suggestions">` e um
  `<script>` anexa `list="endereco-suggestions"` ao `<input aria-label="Buscar
  endereço">` via `window.parent.document`. O browser renderiza nativamente
  como predição de texto dentro do campo. Fallback via `st.expander` para
  temas que não exibem datalist.

### Navegação & páginas

- **Laboratório Estatístico escondido.** Arquivo renomeado pra
  `_4_Laboratorio_Estatistico.py` — Streamlit ignora `pages/` que comecem com
  `_`. Código preservado; a página só some da navegação.
- **Rankings sem Setor Censitário.** Lista local
  `RECORTES_RANKING = [Município, Comando (CPA), Delegacia (DP), Batalhão PMESP,
  Companhia PMESP]` controla o selectbox da página. Se a sidebar estiver em
  Setor Censitário, um `st.info()` avisa e o default vira Município.
- **Rankings com labels corretos para CPA e BTL.** Bug antigo: label era
  igual à `data_key` (código numérico). `_build_label(df, recorte)` agora
  devolve:
    - Município: `NM_MUN`
    - Comando (CPA): `CMDO` (renomeado de `cmdo_label` no pipeline)
    - Delegacia: `DpGeoDes` (fallback `DpGeoCod`)
    - Batalhão: `OPM_BTL  ·  cmdo_BTL` (inclui o CPA pai → evita ambiguidade
      entre "1ºBPM/M" de comandos diferentes)
    - Companhia: `LABEL_CIA` ou `OPM_CIA  ·  btl_CIA`
- **Gráficos em barras empilhadas.** Todos os charts agora usam
  `px.bar(barmode="stack", color=..., color_discrete_sequence=Set2)`:
    - Gráficos → série mensal empilhada por `NATUREZA_APURADA`.
    - Gráficos → top-20 naturezas horizontal, empilhado por `ANO`.
    - Rankings → barras horizontais, empilhadas por `NATUREZA_APURADA`
      (cada unidade mostra a composição por indicador).
- **STL com 2 casas decimais.** `decomp = decomp.round(2)` antes do plot +
  `hovertemplate` com `%{y:.2f}` + `yaxis=dict(tickformat=".2f")`.

### Verificação

`py_compile` passou em todos os 7 arquivos tocados. Cross-check de imports:
`app/Home.py` usa `sidebar_filters, sidebar_footer, RECORTES` (filters.py
exporta todos) e `build_map, geocode_many, zoom_for_bounds, btl_options_by_cpa,
cia_options_by_btl, pmesp_bounds, pmesp_options` (map_builder.py exporta todos).

### Arquivos tocados

- `app/lib/branding.py` — CSS de largura total do logo.
- `app/lib/filters.py` — dataclass com `data_ini/data_fim`, properties de
  compat, `mask_date()` reconstruindo de ANO+MES, `sidebar_footer()` separado.
- `app/lib/map_builder.py` — `_add_pmesp_layers` antes das data layers,
  `zoom_for_bounds()`.
- `app/Home.py` — reescrita: sidebar consolida filtros, regra de negócio do
  temático, multi-natureza por loop, datalist de endereço, zoom sync.
- `app/pages/1_Graficos.py` — barras empilhadas.
- `app/pages/2_Series_Temporais.py` — STL 2 casas decimais.
- `app/pages/3_Rankings.py` — reescrita com `_build_label()`, exclusão do
  Setor Censitário, stack por natureza.
- `app/pages/4_Laboratorio_Estatistico.py` → `app/pages/_4_...` (ocultado).

---

## Fase 10 — Rodada abril/2026 · 10 refinamentos (2026-04-23)

Dez pedidos reunidos numa rodada. Tudo compilando, ainda pendente rodar o
pipeline `aggregate_hora_dia.py` na máquina do Irai pra materializar o
parquet novo.

### 1. Séries Temporais · primeiro gráfico vira multi-linha (top 5)
`app/pages/2_Series_Temporais.py` · o antigo "Observado" (uma linha única) foi
substituído por `px.line(color="NATUREZA_APURADA")` com as 5 naturezas de
maior total no período filtrado. Quando o usuário filtra, ranqueia dentre as
escolhidas. `ts_total` (soma das 5) passou a alimentar STL e previsão — mantém
as seções seguintes com série única, como antes.

### 2. Top 20 · barras decrescentes + legenda sequencial de anos
`app/pages/1_Graficos.py` · `yaxis=dict(autorange="reversed")` coloca a barra
maior no topo. Legenda agora é ANO (não natureza) com paleta **sequencial
Viridis** via `px.colors.sample_colorscale("Viridis", stops)`, onde `stops`
distribui uniformemente entre os anos presentes — o olho capta a progressão
temporal (2022 → 2026 escuro→claro) em vez de 20 categorias arbitrárias.

### 3. Laboratório Estatístico realmente oculto
`app/pages/_4_Laboratorio_Estatistico.py` → `app/_archived/4_...`. Prefixo
`_` dentro de `pages/` NÃO oculta do Streamlit multipage auto-discovery.
A única forma confiável é mover o arquivo pra fora de `pages/`.

### 4. Filtros persistem entre páginas
`app/lib/filters.py` reescrito. Constantes `SS_DATA_INI`, `SS_DATA_FIM`,
`SS_NATUREZAS`, `SS_RECORTE`. Toda widget (`date_input`, `multiselect`,
`radio`) recebe `key=` correspondente → Streamlit sincroniza com
`st.session_state`. `_bootstrap_defaults()` roda uma única vez na primeira
visita; visitas seguintes preservam a escolha.

### 5. Período padrão = último mês com dado
`_latest_month_window()` em `filters.py` lê `data.serie_estado()`, encontra
`max(ANO, MES)` via `argmax(zip(...))`, devolve `(date(y,m,1),
date(y,m,monthrange(y,m)[1]))`. Se não houver agregado ainda, cai num range
de 12 meses terminando hoje.

### 6. Gauges YoY em Gráficos
`app/pages/1_Graficos.py` · novo header da página. `_sum_range()` soma N
filtrando por intervalo [início, fim] e opcionalmente por natureza;
`_gauge()` monta `go.Indicator(mode="gauge+number+delta")` com:
- Delta relativo em %, cores: verde se queda, vermelho se alta (invertido
  do padrão — queda de crime é boa).
- Bandas no arco: `-50→-10` verde, `-10→0` amarelo, `0→+10` laranja,
  `+10→+50` vermelho.
- Threshold line em `#0C2B4E` na posição do delta para leitura rápida.
Janela de comparação via `f.prev_year_window()` — subtrai 1 do ano de
início e fim, preservando dia (com clamp por `monthrange` pra 29/fev).

### 7. Git commit
Item atendido por esta própria Fase 10 (commit criado após a verificação).

### 8. Matriz temática Dia × Faixa Hora × DESC_PERIODO
**Novo pipeline**: `pipeline/aggregate_hora_dia.py` lê
`PROCESSED/sp_dados_criminais` via `pyarrow.dataset` (mesma partitioning
schema do resto do pipeline), deriva:
- `FAIXA_HORA` via regex `^\s*(\d{1,2})` em `HORA_OCORRENCIA_BO` ("HH:MM") +
  `pd.cut(bins=[-1,5,11,17,23], labels=[...])` → Madrugada/Manhã/Tarde/Noite.
- `DIA_SEMANA` via `pd.to_datetime().dt.weekday.map({0:"Seg",...6:"Dom"})`.
- Normaliza `NATUREZA_APURADA` com o mesmo `unidecode` do `aggregate.py`.
- Grupo: `ANO · MES · NATUREZA · DIA · FAIXA · DESC_PERIODO` → contagem N.
Saída: `data/aggregates/matriz_hora_dia.parquet` (~dezenas de KB, cabe
confortavelmente no Streamlit Cloud).

Integrado em `pipeline/run_all.py` como passo "5/5" com `--only hora_dia`
disponível pra rerun sem repassar o pipeline todo.

Loader: `data.matriz_hora_dia()` em `app/lib/data.py` reconstrói a coluna
`DATA` (1º do mês) pra reaproveitar `f.mask_date()` sem duplicar lógica.

Visualização em `2_Series_Temporais.py`: `px.imshow` YlOrRd com
`text_auto=",d"`, filtro local `DESC_PERIODO` (multiselect — vazio = todas),
expander com tabela detalhada facetada, export via `download_buttons`.

**Observação**: o usuário mencionou "conforme a imagem anexa" mas a imagem
não veio no anexo. Adotei a interpretação padrão: heatmap
Dia×Faixa com filtro de Período. Se a referência visual for outra
(ex.: matriz 3D com DESC_PERIODO nos eixos), refino em rodada futura.

### 9. Apenas Delegacia (DP) e Setor Censitário
`app/lib/filters.py` · `RECORTES = ["Delegacia (DP)", "Setor Censitário"]`
(default DP). Removidos Município, CPA, Batalhão, Companhia da UI —
continuam materializados nos agregados, só não são mais expostos.

`app/pages/3_Rankings.py` · `RECORTES_RANKING` reduzido idem. `_build_label()`
simplificado pra dois ramos (DP via `DpGeoDes`/`DpGeoCod`, Setor via
`sc_cod`).

### 10. Mapa sem camadas PMESP
`app/lib/map_builder.py` · removidas as chamadas para `_add_pmesp_layers()`
em `build_map()` e a seção PMESP de `legenda_unificada_html()`. Helpers
auxiliares mantidos no módulo por retrocompat (ninguém mais os importa,
mas não custam nada). `app/Home.py` passa `with_pmesp_labels=False` e
removeu `fit_bounds`, hierarquia CPA/BTL/CIA da sidebar e
`_pending_bounds`.

### Verificação

`py_compile` OK em `filters.py`, `data.py`, `map_builder.py`, `Home.py`,
`1_Graficos.py`, `2_Series_Temporais.py`, `3_Rankings.py`,
`aggregate_hora_dia.py`, `run_all.py` (9 arquivos).

### Arquivos tocados

- `app/lib/filters.py` — reescrita: `RECORTES` reduzido, `prev_year_window`,
  `_latest_month_window`, keys de session_state em toda widget.
- `app/lib/data.py` — novo loader `matriz_hora_dia()` cacheado.
- `app/lib/map_builder.py` — remoção das chamadas PMESP em `build_map` e
  `legenda_unificada_html`.
- `app/Home.py` — simplificação da sidebar e fluxo de bounds.
- `app/pages/1_Graficos.py` — gauges YoY + Top-20 decrescente + Viridis ANO.
- `app/pages/2_Series_Temporais.py` — multi-linha top 5 + matriz hora×dia.
- `app/pages/3_Rankings.py` — só DP e Setor.
- `app/pages/_4_Laboratorio_Estatistico.py` → `app/_archived/4_...`.
- `pipeline/aggregate_hora_dia.py` — NOVO.
- `pipeline/run_all.py` — passo 5/5 e `--only hora_dia`.

---

## 2026-05-11 — Atualização 2026 + correção de 4 bugs críticos

**Sessão:** 2026-05-11  
**Contexto:** Novos arquivos SSP-SP 2026 carregados em `data/raw/ssp/`. Quatro bugs descobertos durante validação local após a atualização.

---

### A. Pipeline incremental `update_2026.py`

**Problema.** O `run_all.py` processa **todos** os `.xlsx` históricos a cada execução. Como o `to_parquet()` com `partition_cols` usa `existing_data_behavior="overwrite_or_ignore"` (default pyarrow), re-rodar sobre partições existentes **acumula** arquivos parquet dentro das mesmas pastas — duplicando dados na leitura posterior.

**Arquivo duplicado.** `SPDadosCriminais_2026 (1).xlsx` (nome gerado pelo Windows ao baixar um arquivo já existente) estava na pasta e seria capturado pelo glob `SPDadosCriminais_*.xlsx`, duplicando os dados de 2026.

**Fix.**
1. Renomeado `SPDadosCriminais_2026 (1).xlsx` → `_old_SPDadosCriminais_2026_v1.xlsx` (fora do glob).
2. Deletadas somente as partições `ANO=2026` nos 4 datasets processados (`sp_dados_criminais`, `celulares_subtraidos`, `veiculos_subtraidos`, `objetos_subtraidos`).
3. Criado `pipeline/update_2026.py`: chama diretamente `ingest_file()` / `ingest_family()` para os arquivos `*_2026.xlsx`, seguido de `aggregate.main()`, `aggregate_hora_dia.main()` e `build_sample.main()`.

**Resultado:**
| Dataset | Registros |
|---|---|
| SPDadosCriminais 2026 | 279.994 |
| CelularesSubtraidos 2026 | 85.175 |
| VeiculosSubtraidos 2026 | 52.243 |
| ObjetosSubtraidos 2026 | 431.089 |
| Agregação (5,07M linhas base) | ✅ todos os parquets |
| Matriz hora×dia | 28.406 linhas |
| Amostra rebuild | 255.000 pontos · 5,5 MB |

**Arquivo criado:** `pipeline/update_2026.py`

---

### B. Bug: mapa de pontos retornava "sem coordenadas válidas"

**Sintoma.** Modo Pontos e Hotspot: mensagem "Sem pontos com coordenadas válidas" para qualquer período/natureza.

**Diagnóstico.** A SSP-SP grava o município abreviado como `"S.PAULO"` em todos os anos. O filtro de SP-Capital em `data.py::pontos()` comparava com `"SAO PAULO"` (forma completa) — zero match, zero pontos.

```python
# ANTES (sempre vazio)
df = df[nm == SP_CAPITAL_NM_MUN]           # "S.PAULO" != "SAO PAULO"

# DEPOIS
SP_CAPITAL_NM_MUN_ALIASES = {"SAO PAULO", "S.PAULO", "S. PAULO"}
df = df[nm.isin(SP_CAPITAL_NM_MUN_ALIASES)]
```

**Fix em `app/lib/data.py`:**
- Adicionada constante `SP_CAPITAL_NM_MUN_ALIASES`.
- Filtro em `pontos()` trocado de `== SP_CAPITAL_NM_MUN` para `isin(SP_CAPITAL_NM_MUN_ALIASES)`.

**Observação.** O bug existia desde a adoção do recorte SP-Capital (rodada abr/26 #4). Os pontos nunca tinham funcionado no modo Capital.

---

### C. Bug: filtro de Delegacia (DP) retornava "sem dados para a delegacia"

**Sintoma.** Selecionar qualquer DP na sidebar produzia: *"Sem dados para a delegacia XX D.P. YYYY no agregado `por_dp.parquet`. Verifique se o pipeline foi rodado para o período."*

**Diagnóstico.** `por_dp.parquet` grava `DpGeoCod` como `float64` (e.g., `10102.0`). `dp_options()` fazia `.astype("string")` → `"10102.0"` (com `.0`). Esse valor era salvo no `session_state`. Mas `serie_contextual()` usava `_norm_dp_cod()` que converte float → `Int64` → `"10102"` (sem `.0`). Comparação `"10102" == "10102.0"` → **False** → série vazia para toda DP.

**Fix em `app/lib/data.py::dp_options()`:**
```python
# ANTES
.astype({"DpGeoCod": "string", ...})   # "10102.0" — errado

# DEPOIS
out["DpGeoCod"] = _norm_dp_cod(out["DpGeoCod"])  # "10102" — consistente
```
O mesmo normalizador `_norm_dp_cod()` é agora aplicado tanto ao gerar a lista de opções quanto ao filtrar a série, garantindo que o `dp_cod` salvo no `session_state` case perfeitamente com o valor na comparação.

O mesmo fix foi aplicado ao path de fallback (via `DP.json`).

---

### D. Bug: mapa de pontos não fazia recorte por DP

**Sintoma.** Ao selecionar uma DP, os KPIs refletiam só aquela delegacia, mas o mapa de pontos continuava exibindo pontos de toda a Capital.

**Diagnóstico (camada 1).** `mask_dp()` em `filters.py` faz no-op quando `"DpGeoCod" not in df.columns`. A coluna `DpGeoCod` nunca existia nos arquivos de pontos: o spatial join do `aggregate.py` era usado apenas para contar (escrevia só os agregados, nunca de volta ao nível do ponto).

**Fix — parte 1:** `build_sample.py` atualizado para adicionar `DpGeoCod` a cada ponto da amostra via spatial join leve (5k pontos × 94 polígonos de Capital → instante). Funções adicionadas:
- `_build_dp_gdf()` — carrega `DP.json` como GeoDataFrame.
- `_assign_dp(df, gdf_dp)` — sjoin `predicate="within"`, normaliza resultado para string `"10102"` (sem `.0`).

**Diagnóstico (camada 2).** Mesmo com `DpGeoCod` na amostra, a amostra tem ~5.000 pontos/mês de toda a Capital. Para uma DP com KPI=37, a representação esperada é `37 × (5000/72030) ≈ 2,6 pontos` — resulta em 0-1 ponto mostrado, enquanto o KPI mostra 37. Inaceitável para uso em campo.

**Fix — parte 2:** `pontos()` em `data.py` inverteu a prioridade das fontes de dados:
```python
# ANTES: amostra preferida, full como fallback
path = PROCESSED / "sp_dados_criminais"  # amostra (5 MB)

# DEPOIS: base completa preferida, amostra como fallback (Cloud)
use_full = (PROCESSED_FULL / "sp_dados_criminais").exists()
path = PROCESSED_FULL / ... if use_full else PROCESSED / ...
```
Com a base completa (98.915 linhas por partição estadual → ~32.566 Capital → ~7.140 ROUBO-OUTROS Capital → **37 exatos** com sjoin), os pontos batem exatamente com o KPI.

**Fix — parte 3:** Para que o filtro de DP funcione com a base completa (que não tem `DpGeoCod`), adicionado `dp_cod` como parâmetro de `pontos()` e nova função `_filter_pontos_by_dp(df, dp_cod)`:
- Localiza o polígono da DP em `DP.json` pelo código.
- Faz `gpd.sjoin(pts, gdf_dp_single, how="inner")` — 1 polígono contra ~7k pontos: < 1s.
- `Home.py` atualizado: passa `dp_cod=f.dp_cod` para `data.pontos()` e remove o `pts.loc[f.mask_dp(pts)]` posterior (filtro agora acontece dentro de `pontos()`).

**Validação:**
```
Total partição março/2026: 98.915
Capital com coords: 32.566
ROUBO - OUTROS capital: 7.140
ROUBO - OUTROS em Bom Retiro (sjoin): 37  ← bate com o KPI
```

---

### Arquivos modificados nesta sessão

| Arquivo | Tipo | Descrição |
|---|---|---|
| `pipeline/update_2026.py` | **NOVO** | Ingestão incremental só dos arquivos `*_2026.xlsx` |
| `pipeline/build_sample.py` | modificado | Adiciona `DpGeoCod` via sjoin a cada partição da amostra |
| `app/lib/data.py` | modificado | 4 fixes: alias NOME_MUNICIPIO, norm DpGeoCod em dp_options, pontos() prefere base completa, _filter_pontos_by_dp() |
| `app/Home.py` | modificado | Passa `dp_cod=f.dp_cod` para `data.pontos()`; remove mask_dp posterior |

### Heurísticas adicionadas

- **`to_parquet` acumula, não substitui.** Ao rodar o pipeline sobre partições existentes, sempre deletar as partições do período a atualizar antes — ou o pyarrow cria múltiplos arquivos na mesma pasta e a leitura duplica os dados.
- **Abreviações de município da SSP-SP.** O campo `NOME_MUNICIPIO` usa `"S.PAULO"` (não `"SAO PAULO"`). Qualquer filtro string sobre essa coluna precisa cobrir os dois formatos.
- **`float64` com `.0` em chaves inteiras.** Colunas como `DpGeoCod` saem do pipeline como `float64` (para acomodar NaN durante o sjoin). `.astype("string")` gera `"10102.0"`. Sempre normalizar via `_norm_dp_cod()` antes de comparar ou salvar em `session_state`.
- **Amostra ≠ dados para uso em campo.** A amostra de 5k/mês é adequada para mapa exploratório, mas inutilizável para análise por DP pequena. Em ambiente local com base completa disponível, `pontos()` deve preferir a base completa.

