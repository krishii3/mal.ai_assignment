"""Generate architecture.pdf from architecture.md using fpdf2.

fpdf2 chosen over reportlab for zero external deps (pure Python), and
over LaTeX-based tooling to keep the repo self-contained.

Run:
    python docs/generate_pdf.py
"""
from __future__ import annotations

import re
from pathlib import Path

from fpdf import FPDF

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "architecture.md"
OUT = ROOT / "architecture.pdf"

# fpdf2's built-in core fonts only support latin-1. Map the handful of
# typographic Unicode chars we use down to ASCII rather than shipping a
# custom TTF — keeps the repo lightweight.
_UNICODE_MAP = {
    "\u2014": "-",   # em dash
    "\u2013": "-",   # en dash
    "\u2019": "'",   # right single quote
    "\u2018": "'",   # left single quote
    "\u201c": '"',   # left double quote
    "\u201d": '"',   # right double quote
    "\u2026": "...", # ellipsis
    "\u2192": "->",  # right arrow
    "\u2713": "[x]", # check mark
    "\u2022": "*",   # bullet
    "\u00d7": "x",   # multiplication
    "\u00a0": " ",   # non-breaking space
}


def _ascii_safe(text: str) -> str:
    for k, v in _UNICODE_MAP.items():
        text = text.replace(k, v)
    return text.encode("latin-1", errors="replace").decode("latin-1")


class DocPDF(FPDF):
    def header(self) -> None:
        if self.page_no() == 1:
            return
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(120, 120, 120)
        self.cell(0, 6, "Mal - Unified Payment Data Pipeline | Architecture & Migration", align="R")
        self.ln(8)
        self.set_text_color(0, 0, 0)

    def footer(self) -> None:
        self.set_y(-12)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(120, 120, 120)
        self.cell(0, 10, f"Page {self.page_no()}", align="C")


def _emit_inline(pdf: FPDF, text: str) -> None:
    """Render a paragraph, converting **bold**/`code` inline styles."""
    text = _ascii_safe(text)
    tokens = re.split(r"(\*\*[^*]+\*\*|`[^`]+`)", text)
    for tok in tokens:
        if not tok:
            continue
        if tok.startswith("**") and tok.endswith("**"):
            pdf.set_font("Helvetica", "B", 10)
            pdf.write(5, tok[2:-2])
            pdf.set_font("Helvetica", "", 10)
        elif tok.startswith("`") and tok.endswith("`"):
            pdf.set_font("Courier", "", 9)
            pdf.write(5, tok[1:-1])
            pdf.set_font("Helvetica", "", 10)
        else:
            pdf.write(5, tok)
    pdf.ln(5)


def _render_table(pdf: FPDF, lines: list[str]) -> None:
    """Render a markdown table (assumes first line is header, second is ---)."""
    rows = [
        [_ascii_safe(c.strip()) for c in ln.strip("|").split("|")]
        for ln in lines
        if not set(ln.strip()) <= {"|", "-", ":", " "}
    ]
    if not rows:
        return
    cols = len(rows[0])
    usable = pdf.w - pdf.l_margin - pdf.r_margin
    col_w = usable / cols
    pdf.set_font("Helvetica", "B", 9)
    for cell in rows[0]:
        pdf.cell(col_w, 6, cell[:60], border=1)
    pdf.ln()
    pdf.set_font("Helvetica", "", 8)
    for r in rows[1:]:
        max_lines = max(
            1,
            max(len(pdf.multi_cell(col_w, 4, c, split_only=True)) for c in r),
        )
        h = 4 * max_lines
        y0 = pdf.get_y()
        x0 = pdf.get_x()
        for i, cell in enumerate(r):
            x = x0 + i * col_w
            pdf.set_xy(x, y0)
            pdf.multi_cell(col_w, 4, cell, border=1)
        pdf.set_xy(x0, y0 + h)
    pdf.ln(3)


def render() -> Path:
    pdf = DocPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_margins(left=18, top=18, right=18)

    lines = SRC.read_text(encoding="utf-8").splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if stripped.startswith("# "):
            pdf.set_font("Helvetica", "B", 18)
            pdf.multi_cell(0, 8, _ascii_safe(stripped[2:]))
            pdf.ln(1)
        elif stripped.startswith("## "):
            pdf.ln(2)
            pdf.set_font("Helvetica", "B", 13)
            pdf.multi_cell(0, 7, _ascii_safe(stripped[3:]))
            pdf.ln(1)
        elif stripped.startswith("### "):
            pdf.set_font("Helvetica", "B", 11)
            pdf.multi_cell(0, 6, _ascii_safe(stripped[4:]))
        elif stripped.startswith("_") and stripped.endswith("_"):
            pdf.set_font("Helvetica", "I", 9)
            pdf.set_text_color(80, 80, 80)
            pdf.multi_cell(0, 5, _ascii_safe(stripped.strip("_")))
            pdf.set_text_color(0, 0, 0)
            pdf.ln(1)
        elif stripped == "---":
            pdf.ln(2)
            pdf.set_draw_color(180, 180, 180)
            pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
            pdf.ln(3)
        elif stripped.startswith("|"):
            tbl = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                tbl.append(lines[i])
                i += 1
            _render_table(pdf, tbl)
            continue
        elif stripped.startswith("- ") or re.match(r"^\d+\.\s", stripped):
            pdf.set_font("Helvetica", "", 10)
            bullet = "- " if stripped.startswith("- ") else stripped.split(" ", 1)[0] + " "
            body = stripped[2:] if stripped.startswith("- ") else stripped.split(" ", 1)[1]
            pdf.cell(5)
            pdf.write(5, bullet)
            _emit_inline(pdf, body)
        elif stripped == "":
            pdf.ln(2)
        else:
            pdf.set_font("Helvetica", "", 10)
            _emit_inline(pdf, stripped)
        i += 1

    pdf.output(str(OUT))
    return OUT


if __name__ == "__main__":
    path = render()
    print(f"wrote {path}")
