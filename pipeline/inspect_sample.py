from pathlib import Path
import pandas as pd
from unidecode import unidecode

base = Path("data/processed_sample/sp_dados_criminais")
# pega a partição mais recente disponível
parts = sorted(base.rglob("*.parquet"))
if not parts:
    print("Nenhum arquivo na amostra!")
    raise SystemExit

# lê tudo da amostra
df = pd.read_parquet(base, engine="pyarrow")
print(f"Total linhas amostra: {len(df)}")
print(f"\nColunas: {df.columns.tolist()}")
print(f"\nCOORDS_VALIDAS:\n{df['COORDS_VALIDAS'].value_counts(dropna=False)}")
print(f"\nLATITUDE nao-nulo: {df['LATITUDE'].notna().sum()}")
print(f"\nNOME_MUNICIPIO top-10:")
print(df["NOME_MUNICIPIO"].value_counts().head(10))

# simula o filtro do app
df["_nm"] = df["NOME_MUNICIPIO"].astype("string").map(
    lambda x: None if pd.isna(x) else " ".join(unidecode(str(x)).upper().split())
)
print(f"\nNOME_MUNICIPIO normalizado top-10:")
print(df["_nm"].value_counts().head(10))

capital = df[df["_nm"] == "SAO PAULO"]
print(f"\nLinhas após filtro SAO PAULO: {len(capital)}")
capital_coords = capital[capital["COORDS_VALIDAS"].fillna(False).astype(bool)]
print(f"Linhas com COORDS_VALIDAS=True: {len(capital_coords)}")
capital_notnull = capital_coords.dropna(subset=["LATITUDE","LONGITUDE"])
print(f"Linhas com LAT/LON nao-nulos: {len(capital_notnull)}")

# amostra de 2026 especificamente
print("\n--- ANO=2026 ---")
df26 = df[df.index.get_level_values("ANO") == 2026] if "ANO" in df.index.names else df
# tenta coluna ANO se não for index
cols = pd.read_parquet(parts[0]).columns.tolist()
print(f"Colunas do arquivo: {cols}")
