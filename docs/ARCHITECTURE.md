# Arquitetura

## Camadas

```
┌─────────────────────────────────────────────────────────────────┐
│                   Streamlit App (app/)                          │
│  Home · Mapa · Séries · Rankings · Laboratório · Metodologia    │
└───────────────▲────────────────────────────────▲────────────────┘
                │  @st.cache_data                │  @st.cache_resource
                │                                │
    data/aggregates/*.parquet          data/raw/geo/*.shp
     (leves, ≲100MB total)              (simplificados com tol. 0.0001°)
                ▲
                │  gera via pipeline/aggregate.py
                │
     data/processed/sp_dados_criminais/ANO=.../MES=.../*.parquet
     data/processed/celulares_subtraidos/…  veiculos_…  objetos_…
                ▲
                │  gera via pipeline/ingest_*.py
                │
     data/raw/ssp/*.xlsx          ← usuário/cron baixa mensalmente
```

## Fluxo

1. **Ingestão**: `pipeline/ingest_sp_dados_criminais.py` e `pipeline/ingest_subtraidos.py`.
   Lê os `.xlsx` em streaming, normaliza colunas, trata `"NULL"`, valida coordenadas
   dentro de SP, particiona por ano/mês em Parquet.
2. **Agregação + spatial join**: `pipeline/aggregate.py`. Carrega os shapefiles,
   faz point-in-polygon e escreve tabelas agregadas pré-calculadas
   (por município / batalhão / companhia / comando / setor censitário).
3. **App Streamlit**: lê os agregados via `@st.cache_data(ttl=3600)`. Para drill-down
   no mapa de pontos, faz *lazy-load* da partição `ANO=X/MES=Y` direto do Parquet.

## Decisões-chave

| Decisão | Motivo |
|---|---|
| Parquet particionado | Leitura seletiva via filtros, sem carregar tudo |
| `@st.cache_data` com TTL 1h | Recompila na troca de filtros raros, mas mantém fluidez |
| `@st.cache_resource` para GeoDataFrames | Objetos não hasheáveis → compartilhados entre sessões |
| Simplificação topológica de shapefiles | Renderização fluida mesmo em setor censitário (>30k polígonos) |
| Agregados pré-calculados | Streamlit Cloud tem 1GB RAM — evita carregar os ~50M pontos originais |
| Pontos amostrados (max 50k) | Navegadores não renderizam bem mais do que isso em WebGL |

## Ciclo de atualização mensal

```
1. Baixar os .xlsx mais recentes da SSP-SP → data/raw/ssp/
2. python pipeline/run_all.py
3. git add data/aggregates data/raw/geo
4. git commit -m "Atualização mensal - <YYYY-MM>"
5. git push   # Streamlit Cloud redeploy automático
```

Raw .xlsx **não** vão para o git (pesados demais + não precisam). Processed *point-level*
Parquet fica opcionalmente versionado conforme tamanho.
