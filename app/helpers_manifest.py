# app/helpers_manifest.py

from datetime import date, datetime
from io import BytesIO
import re

from flask import current_app, render_template
from xhtml2pdf import pisa

from playwright.sync_api import sync_playwright

def generate_pdf_modern(html: str) -> bytes:
    """
    Render HTML to PDF using real Chromium (Playwright).
    This preserves all modern CSS: flex, grid, variables, etc.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()

        # Load HTML directly
        page.set_content(html, wait_until="networkidle")

        # Generate PDF (A4 default)
        pdf_bytes = page.pdf(
            format="A4",
            print_background=True,  # keep colours, header shading
            margin={"top": "12mm", "bottom": "12mm", "left": "12mm", "right": "12mm"},
        )

        browser.close()
        return pdf_bytes


# ----------------- generic HTML -> PDF (xhtml2pdf) -----------------

def generate_manifest_pdf_from_html(html: str) -> bytes:
    """
    Convert manifest HTML (same as preview) to PDF bytes using wkhtmltopdf.
    This preserves your layout and CSS.
    """
    # Adjust path if needed
    wkhtml = r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"

    config = pdfkit.configuration(wkhtmltopdf=wkhtml)
    try:
        pdf_bytes = pdfkit.from_string(html, False, configuration=config)
    except Exception as e:
        current_app.logger.exception("wkhtmltopdf failed while generating manifest PDF")
        raise

    return pdf_bytes

def _html_to_pdf_bytes(html: str) -> bytes:
    """
    Convert an HTML string to PDF bytes using xhtml2pdf.
    Raises RuntimeError if PDF generation fails.
    """
    result = BytesIO()
    pisa_status = pisa.CreatePDF(
        html,
        dest=result,
        encoding="utf-8",
    )
    if pisa_status.err:
        raise RuntimeError("PDF generation failed (xhtml2pdf error)")
    return result.getvalue()


def _sanitize_html_for_pdf(html: str) -> str:
    """
    xhtml2pdf doesn't understand CSS variables like var(--border-color),
    gradients, etc. Replace any var(...) with a safe fallback colour/value.
    """
    # Replace specific known var() usages first if you like:
    html = html.replace("var(--border-color)", "#666666")

    # Generic: any var(...) -> #000000
    html = re.sub(r"var\([^)]+\)", "#000000", html)

    return html


def generate_manifest_pdf_from_html(html: str) -> bytes:
    """
    Build a Passenger Manifest PDF from pre-rendered HTML.
    Used by the APG push route.
    """
    safe_html = _sanitize_html_for_pdf(html)
    return _html_to_pdf_bytes(safe_html)


# ----------------- helpers used by manifest building -----------------


def _parse_dcs_dob(dob_str: str | None):
    """Parse DCS DateOfBirth 'YYYY-MM-DDTHH:MM:SS' -> (date, 'YYYY-MM-DD')."""
    if not dob_str:
        return None, ""
    try:
        d = datetime.fromisoformat(dob_str).date()
    except ValueError:
        try:
            d = datetime.strptime(dob_str[:10], "%Y-%m-%d").date()
        except ValueError:
            return None, ""
    return d, d.strftime("%Y-%m-%d")


def _calc_age(dob: date | None, ref: date | None = None) -> int | None:
    if dob is None:
        return None
    if ref is None:
        ref = date.today()
    years = ref.year - dob.year
    if (ref.month, ref.day) < (dob.month, dob.day):
        years -= 1
    return years


def _format_ssrs(ssrs_list):
    """DCS Ssrs -> 'RQST (PAID Standard Seats), OTHS (PAID REFPRO)'."""
    if not ssrs_list:
        return ""
    parts = []
    for s in ssrs_list:
        code = (s.get("Code") or "").strip()
        txt = (s.get("FreeText") or "").strip()
        if code and txt:
            parts.append(f"{code} ({txt})")
        elif code:
            parts.append(code)
        elif txt:
            parts.append(txt)
    return ", ".join(parts)


def _seat_sort_key(seat: str | None):
    """Sort seats like 1A, 2B, 10C -> (row, letter)."""
    if not seat:
        return (999, "Z")
    seat = seat.strip().upper()
    num = ""
    suf = ""
    for ch in seat:
        if ch.isdigit():
            num += ch
        else:
            suf += ch
    try:
        row = int(num) if num else 999
    except ValueError:
        row = 999
    return (row, suf or "Z")
