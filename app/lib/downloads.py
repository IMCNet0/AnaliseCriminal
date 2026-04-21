"""Helpers de exportação: CSV e Excel respeitando os filtros ativos."""
from __future__ import annotations

import io
from datetime import datetime
import pandas as pd
import streamlit as st


def csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def excel_bytes(df: pd.DataFrame, sheet: str = "dados", meta: dict | None = None) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        df.to_excel(w, sheet_name=sheet, index=False)
        if meta:
            pd.DataFrame(meta.items(), columns=["chave", "valor"]).to_excel(
                w, sheet_name="metadata", index=False
            )
    return buf.getvalue()


def download_buttons(df: pd.DataFrame, basename: str, meta: dict | None = None) -> None:
    """Renderiza botões de download CSV + Excel lado a lado."""
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    c1, c2, c3 = st.columns([1, 1, 3])
    with c1:
        st.download_button(
            "⬇️ CSV",
            data=csv_bytes(df),
            file_name=f"{basename}_{ts}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with c2:
        st.download_button(
            "⬇️ Excel",
            data=excel_bytes(df, meta=meta),
            file_name=f"{basename}_{ts}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    with c3:
        st.caption(f"{len(df):,} linhas · filtros aplicados no dataset exportado")
