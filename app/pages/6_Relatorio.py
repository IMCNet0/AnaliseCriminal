"""Relatório Completo — local only.

Gera relatório HTML (interativo, com Plotly CDN) e/ou PDF (imagens estáticas,
pronto para impressão) com todos os indicadores, gráficos, tabelas e insights
baseados nos filtros ativos na sidebar.

Requer kaleido + fpdf2 para a opção PDF:
    pip install -r requirements-local.txt
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import streamlit as st

from lib.branding import apply_brand, header
from lib.filters import sidebar_filters, sidebar_footer
from lib import report as rpt

apply_brand("Relatório · InsightGeoLab AI")
header(
    "Relatório Completo · SP-Capital",
    "Exporta indicadores, gráficos e insights com os filtros ativos · local only",
)

f = sidebar_filters()
sidebar_footer()

# ---------------------------------------------------------------------------
# Status das dependências opcionais
# ---------------------------------------------------------------------------
def _has_key(name: str) -> bool:
    """True se a chave existe em st.secrets ou variável de ambiente."""
    try:
        val = st.secrets.get(name, "")
        if val and val != "COLE_SUA_CHAVE_AQUI":
            return True
    except Exception:
        pass
    return bool(os.environ.get(name))

_anthropic_key  = _has_key("ANTHROPIC_API_KEY")
_gemini_key     = _has_key("GOOGLE_API_KEY")
_anthropic_ok   = False
_gemini_ok      = False
_kaleido_ok     = False
_fpdf2_ok       = False
_pillow_ok      = False

try:
    import anthropic   # type: ignore
    _anthropic_ok = True
except ImportError:
    pass

try:
    from google import genai as _google_genai  # type: ignore  # noqa: F401
    _gemini_ok = True
except ImportError:
    pass

try:
    import kaleido  # type: ignore
    _kaleido_ok = True
except ImportError:
    pass

try:
    from fpdf import FPDF  # type: ignore
    _fpdf2_ok = True
except ImportError:
    pass

try:
    from PIL import Image  # type: ignore
    _pillow_ok = True
except ImportError:
    pass

_pdf_ready      = _kaleido_ok and _fpdf2_ok and _pillow_ok
_claude_active  = _anthropic_key and _anthropic_ok
_gemini_active  = _gemini_key and _gemini_ok

# ---------------------------------------------------------------------------
# Painel de status
# ---------------------------------------------------------------------------
with st.expander("Status das dependências", expanded=False):
    cols = st.columns(5)
    cols[0].metric("kaleido",      "✅ OK" if _kaleido_ok     else "❌ ausente")
    cols[1].metric("fpdf2",        "✅ OK" if _fpdf2_ok       else "❌ ausente")
    cols[2].metric("Pillow",       "✅ OK" if _pillow_ok      else "❌ ausente")
    cols[3].metric("Claude API",   "✅ OK" if _claude_active  else "—")
    cols[4].metric("Gemini API",   "✅ OK" if _gemini_active  else "—")
    if not _pdf_ready:
        st.code("pip install -r requirements-local.txt", language="bash")
    if not _claude_active and not _gemini_active:
        st.caption(
            "Configure **ANTHROPIC_API_KEY** (Claude) ou **GOOGLE_API_KEY** "
            "(Gemini Flash — gratuito) para insights por IA."
        )

# ---------------------------------------------------------------------------
# Resumo dos filtros ativos
# ---------------------------------------------------------------------------
st.markdown("### Filtros aplicados")
c1, c2, c3 = st.columns(3)
c1.metric("Período",
    f"{f.data_ini.strftime('%d/%m/%Y')}",
    f"até {f.data_fim.strftime('%d/%m/%Y')}",
)
c2.metric(
    "Naturezas",
    str(len(f.naturezas)) if f.naturezas else "Todas",
    ", ".join(f.naturezas[:2]) + ("…" if len(f.naturezas) > 2 else "") if f.naturezas else "",
)
c3.metric("Escopo", f.dp_des or "SP-Capital")

if _claude_active:
    st.success("Insights por **Claude Sonnet** (Anthropic).", icon="🤖")
elif _gemini_active:
    st.info("Insights por **Gemini 2.0 Flash** (Google — gratuito).", icon="✨")
else:
    st.caption(
        "Insights gerados por análise de regras. "
        "Configure ANTHROPIC_API_KEY ou GOOGLE_API_KEY para usar IA."
    )

st.divider()

# ---------------------------------------------------------------------------
# Geração
# ---------------------------------------------------------------------------
SS_DATA = "relatorio_data"
SS_HTML = "relatorio_html"
SS_PDF  = "relatorio_pdf"

if st.button("▶ Gerar Relatório", type="primary", use_container_width=True):
    # Limpa resultados anteriores ao regerar
    for k in (SS_DATA, SS_HTML, SS_PDF):
        st.session_state.pop(k, None)

    prog = st.progress(0, text="Coletando dados…")
    try:
        d = rpt.gather(f)
        prog.progress(20, text="Construindo visualizações…")

        figs = rpt.build_figures(d)
        prog.progress(55, text="Gerando insights…")

        insights = rpt.generate_insights(d)
        prog.progress(80, text="Montando HTML…")

        html_bytes = rpt.render_html(f, d, figs, insights).encode("utf-8")
        prog.progress(100, text="Pronto!")
        prog.empty()

        # Congela os filtros junto com os dados para que o PDF use exatamente
        # o mesmo f que gerou o HTML, mesmo se o usuário alterar a sidebar depois.
        st.session_state[SS_DATA] = {"f": f, "d": d, "figs": figs, "insights": insights}
        st.session_state[SS_HTML] = html_bytes

    except Exception as exc:
        prog.empty()
        st.error(f"Erro ao gerar o relatório: {exc}")
        st.stop()

# ---------------------------------------------------------------------------
# Downloads (mostrados enquanto os dados estiverem no session_state)
# ---------------------------------------------------------------------------
if SS_HTML in st.session_state:
    # Usa sempre os filtros congelados no momento da geração — não o sidebar atual.
    _fz      = st.session_state[SS_DATA].get("f", f)   # fz = frozen filters
    d        = st.session_state[SS_DATA]["d"]
    figs     = st.session_state[SS_DATA]["figs"]
    insights = st.session_state[SS_DATA]["insights"]

    total_str = f"{d['total_periodo']:,}".replace(",", ".")
    st.success(
        f"Relatório gerado — **{len(figs)} visualizações** · **{total_str} ocorrências**",
        icon="✅",
    )

    periodo = f"{_fz.data_ini.strftime('%Y%m')}_a_{_fz.data_fim.strftime('%Y%m')}"
    slug    = f"dp{_fz.dp_cod}_" if _fz.dp_cod else ""

    # ---- HTML ---------------------------------------------------------------
    st.download_button(
        label="⬇️ Baixar Relatório HTML  (gráficos interativos)",
        data=st.session_state[SS_HTML],
        file_name=f"relatorio_{slug}{periodo}.html",
        mime="text/html",
        use_container_width=True,
    )
    st.caption("Abra no navegador. Ctrl+P → Salvar como PDF para versão impressa básica.")

    st.divider()

    # ---- PDF ----------------------------------------------------------------
    if not _pdf_ready:
        st.warning(
            "PDF indisponível: instale as dependências abaixo e reinicie o Streamlit.",
            icon="⚠️",
        )
        st.code("pip install -r requirements-local.txt", language="bash")
    else:
        if SS_PDF not in st.session_state:
            if st.button("📄 Gerar PDF  (imagens estáticas, pronto para impressão)",
                         use_container_width=True):
                total_figs = len(figs)
                prog_pdf = st.progress(0, text="Iniciando exportação de gráficos…")
                status_box = st.empty()

                def _on_progress(step, total, label):
                    pct = int(step / max(total, 1) * 100)
                    prog_pdf.progress(pct, text=label)
                    status_box.caption(f"Etapa {step}/{total} — {label}")

                try:
                    pdf_bytes = rpt.render_pdf(_fz, d, figs, insights,
                                               on_progress=_on_progress)
                    prog_pdf.progress(100, text="PDF pronto!")
                    prog_pdf.empty()
                    status_box.empty()
                    st.session_state[SS_PDF] = pdf_bytes
                    st.rerun()
                except Exception as exc:
                    prog_pdf.empty()
                    status_box.empty()
                    st.error(f"Erro ao gerar PDF: {exc}")
        else:
            st.download_button(
                label=f"⬇️ Baixar PDF  ({_fz.dp_des or 'SP-Capital'} · {_fz.data_ini.strftime('%d/%m/%Y')} a {_fz.data_fim.strftime('%d/%m/%Y')})",
                data=st.session_state[SS_PDF],
                file_name=f"relatorio_{slug}{periodo}.pdf",
                mime="application/pdf",
                type="primary",
                use_container_width=True,
            )
            if st.button("↺ Regenerar PDF", use_container_width=False):
                st.session_state.pop(SS_PDF, None)
                st.rerun()

    # -------------------------------------------------------------------------
    # Preview inline
    # -------------------------------------------------------------------------
    st.divider()
    with st.expander("Preview completo do relatório", expanded=False):
        st.markdown("#### Indicadores Principais")
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total no Período", total_str)
        k2.metric(
            f"Total em {d['ultimo_ano']}",
            f"{d['total_ultimo_ano']:,}".replace(",", "."),
            f"{d['delta_yoy']:+.1f}% vs. ano anterior",
            delta_color="inverse",
        )
        k3.metric(
            "Naturezas Distintas",
            str(len(d["nat_counts"])) if not d["nat_counts"].empty else "0",
        )
        k4.metric("Escopo", _fz.dp_des or "SP-Capital")

        fig_labels = {
            "serie_total":     "Tendência Mensal — Total Geral",
            "evolucao_mensal": "Evolução Mensal — Top 5 Naturezas",
            "evol_anual":      "Comparativo Anual por Natureza",
            "top_naturezas":   "Ranking de Naturezas Criminais",
            "yoy":             "Comparação Interanual (YoY)",
            "ranking_dp":      "Ranking por Delegacia (DP)",
            "mapa_dp":         "Distribuição Geográfica por Delegacia (DP)",
            "mapa_pontos":     "Pontos de Ocorrência",
            "mapa_calor":      "Mapa de Calor — Densidade de Ocorrências",
            "matriz":          "Distribuição Dia × Faixa Horária",
        }
        for key, label in fig_labels.items():
            if key in figs:
                st.markdown(f"##### {label}")
                st.plotly_chart(figs[key], use_container_width=True)

        if not d["nat_counts"].empty:
            st.markdown("##### Tabela — Top 20 Naturezas")
            tbl = d["nat_counts"].head(20).copy()
            tbl.columns = ["Natureza", "Ocorrências"]
            tbl["% do Total"] = (tbl["Ocorrências"] / d["total_periodo"] * 100).round(1)
            tbl.index = range(1, len(tbl) + 1)
            st.dataframe(tbl, use_container_width=True)

        if not d["dp_rank"].empty:
            st.markdown("##### Tabela — Top 15 Delegacias")
            _des_col = "DpGeoDes" if "DpGeoDes" in d["dp_rank"].columns else d["dp_rank"].columns[0]
            dp_tbl = d["dp_rank"][[_des_col, "N"]].copy()
            dp_tbl.columns = ["Delegacia", "Ocorrências"]
            dp_tbl.index = range(1, len(dp_tbl) + 1)
            st.dataframe(dp_tbl, use_container_width=True)

        st.markdown("##### Insights Analíticos")
        st.markdown(insights)
