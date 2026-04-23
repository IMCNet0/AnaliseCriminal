"""Metodologia e transparência dos dados."""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import streamlit as st
from lib.branding import apply_brand, header

apply_brand("Metodologia · Portal de Análise Criminal")
header("Metodologia", "Fontes, limitações e referências bibliográficas")

st.markdown(
    """
### Fontes de dados

- **Ocorrências criminais**: Secretaria da Segurança Pública de SP (SSP-SP), bases abertas.
  Cada registro é um Boletim de Ocorrência (BO) com data, hora, natureza apurada, coordenadas
  geográficas (quando informadas) e vínculo administrativo (delegacia, seccional, departamento,
  batalhão/companhia/comando PMESP, município/IBGE).
- **Shapefiles**: IBGE (setor censitário) e PMESP (batalhões, companhias, comandos).
- **Indicadores socioeconômicos**: IBGE/SIDRA — população, renda domiciliar, IDH, Gini.

### Tratamento aplicado

1. Leitura em streaming dos `.xlsx` (openpyxl `read_only`) para evitar sobrecarga de memória.
2. Normalização dos nomes de coluna (sem acento, upper, snake).
3. Conversão de "NULL" string para `NaN` real.
4. Validação de coordenadas dentro dos limites do estado de SP (-25.5 a -19.5 lat; -53.5 a -44 lon).
5. *Downcast* de inteiros/floats e transformação de colunas repetitivas em `category`.
6. Escrita em Parquet particionado por **ano/mês**.
7. Agregação pré-calculada por município, setor censitário, batalhão, companhia e comando,
   via *spatial join* ponto-em-polígono.

### Limitações e ressalvas

- As bases de **objetos subtraídos** (celulares, veículos, objetos) são extrações **sem tratamento
  de consistência**, conforme explicita a própria SSP-SP. Elas podem conter registros duplicados
  (várias linhas por BO) e devem ser analisadas de forma **exploratória**, não como estatística
  oficial. Para indicadores oficiais, a base é *SPDadosCriminais*.
- Coordenadas vazias ou fora dos limites do estado são descartadas do mapa de pontos (mas
  contam nos agregados por município/batalhão/comando, quando o vínculo administrativo existe).
- Taxas per capita usam população IBGE do ano mais recente disponível; discrepâncias interanuais
  podem ocorrer em municípios com grande variação demográfica.

### Técnicas estatísticas disponíveis

| Técnica | Uso recomendado |
|---|---|
| **Z-score** / **IQR** | Identificar municípios atípicos em uma natureza específica |
| **Isolation Forest** | Outliers multivariados (considera todas as naturezas juntas) |
| **STL** | Separar tendência, sazonalidade e componente irregular |
| **SARIMA / Prophet** | Projetar meses futuros com intervalo de confiança 95% |
| **K-means / Aglomerativo** | Agrupar municípios por perfil criminal |
| **Pearson/Spearman/Kendall** | Correlação entre naturezas |
| **Mann-Whitney** | Comparar 2 grupos sem assumir normalidade |
| **Kruskal-Wallis** | Comparar k grupos sem assumir normalidade |
| **Qui-quadrado** | Independência entre variáveis categorizadas |
| **Bootstrap** | Intervalo de confiança robusto para qualquer estatística |

### Atualização

O pipeline roda `python pipeline/run_all.py` sempre que a SSP-SP publica novos boletins (mensal).
Última atualização do portal: `{last_update}`.
"""
)
