# Portal de Análise Criminal — SP

Dashboard público de análise criminal do estado de São Paulo, construído sobre as bases abertas da SSP-SP.
Stack: **Streamlit + GeoPandas + Plotly/PyDeck + scikit-learn + statsmodels + Prophet**.

## Estrutura

```
.
├── app/                  # Aplicação Streamlit (multipágina)
│   ├── Home.py           # Página inicial (KPIs executivos)
│   ├── pages/            # Páginas adicionais
│   └── lib/              # Módulos internos (data, geo, stats, branding)
├── pipeline/             # Scripts de ingestão + agregação (rodam fora do Streamlit)
├── data/
│   ├── raw/              # Arquivos originais da SSP-SP e IBGE (gitignored)
│   ├── processed/        # Parquet completo pós-ingestão (~600 MB, gitignored)
│   ├── processed_sample/ # Amostra de pontos (5 MB, commitada — alimenta mapa de pontos)
│   ├── geo/              # Shapefiles simplificados das camadas (commitados)
│   └── aggregates/       # Tabelas agregadas consumidas pelo app (commitadas)
├── brand/                # Logo, paleta, tipografia, nome oficial
├── docs/                 # Metodologia, arquitetura, changelog
├── requirements.txt
└── .streamlit/config.toml
```

## Fluxo de dados

1. **Ingestão** (`pipeline/ingest_*.py`): lê `.xlsx` da SSP-SP em streaming, normaliza colunas, trata "NULL" string, converte datas e coordenadas, escreve Parquet particionado por ano/mês.
2. **Join geoespacial** (`pipeline/aggregate.py`): carrega shapefiles (setor censitário, batalhão, companhia, comando), faz point-in-polygon para cada ocorrência e pré-calcula agregados por recorte.
3. **Join IBGE**: adiciona população e indicadores socioeconômicos para cálculo de taxas per capita.
4. **App Streamlit** (`app/`): consome os agregados via `@st.cache_data`. Para mapas de ponto (drill-down), faz lazy-load do Parquet filtrado.

## Rodar localmente

```bash
pip install -r requirements.txt
python pipeline/run_all.py             # 1ª vez: gera data/processed e data/aggregates
python pipeline/build_sample.py        # gera data/processed_sample (amostra do mapa de pontos)
streamlit run app/Home.py
```

## Deploy (Streamlit Community Cloud)

- Repositório: este diretório em um repo GitHub público.
- Secrets: nenhum (dados são 100% públicos).
- Arquivo principal: `app/Home.py`.
- **Importante**: `data/raw/` e `data/processed/` (base completa ~600 MB) ficam fora do git. O runtime lê de `data/processed_sample/` — amostra gerada por `pipeline/build_sample.py` (max 5k pontos/mês, ~5 MB). Shapefiles em `data/geo/` (exceto `CENSO.json`, substituído por `CENSO_simplified.parquet`) e agregados em `data/aggregates/` são commitados.

## Fonte de dados

- **Crimes**: SSP-SP, bases abertas (SPDadosCriminais, CelularesSubtraidos, VeiculosSubtraidos, ObjetosSubtraidos, DadosProdutividade, MDIP). Atualização mensal.
- **Limites geográficos**: IBGE (setor censitário 2022) e PMESP (batalhões, companhias, CPA/comando).
- **Indicadores socioeconômicos**: IBGE (SIDRA / Censo 2022).

## Módulos estatísticos disponíveis no portal

Todos são acionáveis pelo usuário na página `🔬 Laboratório Estatístico`:

- **Detecção de outliers**: z-score, IQR, Isolation Forest.
- **Séries temporais**: decomposição STL, ARIMA, Prophet.
- **Clusterização**: K-means e aglomerativo hierárquico.
- **Associação**: correlações (Pearson/Spearman/Kendall), qui-quadrado, Mann-Whitney, Kruskal-Wallis.
- **Análise exploratória**: estatísticas descritivas, intervalos de confiança, bootstrap.

Detalhes em `docs/METHODOLOGY.md`.
