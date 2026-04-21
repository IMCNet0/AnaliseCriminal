# Runbook — primeira execução e manutenção

## Pré-requisitos

- Python 3.11+ (Streamlit Cloud roda 3.11 por padrão).
- `pip install -r requirements.txt` em uma venv limpa.

## Primeira execução (desenvolvimento local)

```bash
# 1. Venv
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Confira que os insumos estão no lugar
#    data/raw/ssp/*.xlsx    ← 4.6GB de planilhas SSP-SP
#    data/raw/geo/*.shp     ← shapefiles (setor, batalhão, companhia, comando, municípios)
#    brand/brand.yaml       ← identidade visual (se já tiver)

# 3. Rodar pipeline (demora ~30 min para os 4.6GB, depende da máquina)
python pipeline/run_all.py

# 4. Subir o app
streamlit run app/Home.py
```

## Atualização mensal

A SSP-SP libera novos dados todo mês. Para atualizar:

```bash
# Baixe os .xlsx novos e sobrescreva os antigos em data/raw/ssp/
python pipeline/run_all.py --only ssp
python pipeline/run_all.py --only aggregate
git add data/aggregates
git commit -m "Atualização mensal $(date +%Y-%m)"
git push                  # Streamlit Cloud redeploy automático
```

## Deploy no Streamlit Community Cloud

1. Crie um repositório GitHub público (ou privado, se tiver plano).
2. Verifique o `.gitignore` — ele já exclui `data/raw/` e `data/processed/points/`.
3. Em https://share.streamlit.io/ → **New app**:
   - Repo: `<user>/portal-analise-criminal-sp`
   - Branch: `main`
   - Main file: `app/Home.py`
4. Aguarde o build (~5 min na primeira vez).

### Se o app exceder 1 GB RAM

Diagnóstico: rode `streamlit run app/Home.py` localmente com `@st.cache_data(max_entries=1)`
e veja qual página estoura. Mitigações:

- Amostrar ainda mais os pontos (`max_rows=20_000` em `data.pontos`).
- Simplificar mais os shapefiles (`simplify(tolerance=0.001)` em `geo.py`).
- Quebrar a série estadual em blocos por natureza (carregar só a escolhida).

## Troubleshooting

| Sintoma | Causa provável | Fix |
|---|---|---|
| `FileNotFoundError: data/aggregates/…` | Pipeline não rodou | `python pipeline/run_all.py` |
| Mapa vazio, KPIs com 0 | Filtros muito restritivos | Resete o período na sidebar |
| `KeyError: 'COD_IBGE'` | Shapefile com nome de coluna diferente | Ajuste `JOIN_KEYS` em `app/lib/geo.py` |
| `prophet` demora muito no deploy | Build inicial de stan | Normal: ~4 min na 1ª vez |
| Coordenadas "0,0" aparecendo | Registros sem geocoding na fonte | Já filtrados por `valid_sp_bounds()` |
