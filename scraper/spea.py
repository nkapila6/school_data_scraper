"""
SPEA (Sharjah) scraper.

Fetches tuition fees from per-school fee PDFs and transport fees from
individual school websites (SPEA has no centralised transport data).

Curriculum IDs used in the SPEA listing URL:
  0=MoE  1=American  2=British  3=Indian  4=Pakistani
  5=SABIS  6=Australian  7=Pilipinas  8=French  9=German
"""

import html as htmlmod
import io
import re
import time
from urllib.parse import quote

from .common import curl_get, html_to_text, resolve_grade

BASE        = "https://spea.shj.ae"
LISTING_URL = f"{BASE}/en/educational-institutions/schools/?page={{page}}"
CURR_URL    = f"{BASE}/en/educational-institutions/schools/?curriculum={{curr}}&page={{page}}"
DETAIL_URL  = f"{BASE}/en/educational-institutions/schools/{{sid}}"

CURRICULA: dict[str, str] = {
    "0": "MoE",
    "1": "American",
    "2": "British",
    "3": "Indian",
    "4": "Pakistani",
    "5": "SABIS",
    "6": "Australian",
    "7": "Pilipinas",
    "8": "French",
    "9": "German",
}

FIELDNAMES = [
    "school_name", "curriculum", "spea_rating", "area",
    "tuition_nursery_ecc",
    "tuition_fs1",
    "tuition_fs2",
    "tuition_primary",
    "tuition_secondary_y7_9",
    "tuition_gcse_y10_11",
    "tuition_sixth_form_y12_13",
    "fee_currency",
    "fee_period",
    "spea_lowest_fee",
    "spea_highest_fee",
    "transport_fee_min_aed",
    "transport_fee_max_aed",
    "transport_notes",
    "source_url",
    "pdf_url",
    "notes",
]

# ---------------------------------------------------------------------------
# Hardcoded transport fees — sourced from individual school websites.
# SPEA does not publish transport fees centrally. (2025-2026 academic year)
# Format: exact school name → (min_aed, max_aed, source_note)
# ---------------------------------------------------------------------------

TRANSPORT_DATA: dict[str, tuple] = {
    "Gems Cambridge International Private School": (
        4830, 6390,
        "gemscambridgeschool-sharjah.com — Muwailah AED 4,830 · Sharjah AED 5,350 · Dubai AED 5,870–6,390",
    ),
    "Scholars Int. Academy Pvt. School. LLC": (
        5250, 7800,
        "sia.ae — Sharjah/Ajman AED 5,250 · Dubai Ghusais/Mirdif AED 5,450–5,750 · Arabian Ranches AED 7,800",
    ),
    "Pace British School L.L.C": (
        4000, 5200,
        "pacebritish.com — National Paints AED 4,000 · Sharjah AED 4,300 · Ajman AED 4,500 · Dubai AED 4,800–5,200",
    ),
    "Ibn Seena English High School L.L.C.": (
        3400, 3750,
        "ibnseenaschool.net PDF — Sharjah AED 3,400 · Ajman AED 3,750",
    ),
    "Brilliant Int. Private School": (
        3250, 4000,
        "bips.ae — Sharjah AED 3,250 · Ajman AED 3,750 · Dubai (Silicon Oasis/Mirdif) AED 4,000",
    ),
    "Rosary School LLC": (
        4000, 4100,
        "rosaryschoolshj.com — Sharjah AED 4,000 · Al Zahia/Al Jaddah AED 4,100",
    ),
    "Shj. British Int .Pvt. School": (
        4500, 5500,
        "sharjahbritishinternationalschool.com — Sharjah AED 4,500 · Ajman AED 4,700 · Dubai AED 5,000 · UAQ AED 5,500",
    ),
    "Victoria English Pvt. School LLC": (
        4500, 5000,
        "edarabia.com — Sharjah AED 4,500 · Dubai AED 5,000",
    ),
}


# ---------------------------------------------------------------------------
# School list
# ---------------------------------------------------------------------------

def collect_school_ids(curriculum_ids: list[str] | None = None) -> list[tuple[str, str]]:
    """
    Return (school_id, curriculum_label) pairs from SPEA.

    Pass *curriculum_ids* (e.g. ["2"] for British only) to filter by
    curriculum; omit for all 96 schools across every curriculum.
    """
    results: dict[str, str] = {}

    if curriculum_ids:
        sources = [
            (CURR_URL.replace("{curr}", cid), CURRICULA.get(cid, cid))
            for cid in curriculum_ids
        ]
    else:
        sources = [(LISTING_URL, "")]

    for base_url, label in sources:
        page = 1
        while True:
            html = curl_get(base_url.replace("{page}", str(page)))
            ids  = [i for i in re.findall(r"/en/educational-institutions/schools/(\d+)", html)
                    if i not in ("", "1")]
            new  = [i for i in dict.fromkeys(ids) if i not in results]
            if not new:
                break
            for sid in new:
                results[sid] = label
            if f"page={page + 1}" not in html:
                break
            page += 1
            time.sleep(0.3)

    return list(results.items())


# ---------------------------------------------------------------------------
# Per-school fetch + parse  (called from the thread pool)
# ---------------------------------------------------------------------------

def fetch_school(
    sid: str,
    curriculum_hint: str = "",
    include_transport: bool = True,
) -> dict:
    """
    Fetch and parse a single SPEA school. Returns a row dict ready for CSV.
    Safe to call from multiple threads simultaneously.
    """
    url = DETAIL_URL.format(sid=sid)
    page_html = curl_get(url)
    if not page_html:
        return make_row({"name": f"School {sid}"}, {}, sid, curriculum_hint)

    info  = _parse_detail_page(page_html, sid)
    fees  = {}

    if info["fee_pdf_url"]:
        pdf_bytes = curl_get(info["fee_pdf_url"], referer=url, binary=True)
        if pdf_bytes and len(pdf_bytes) >= 500:
            fees = _parse_fee_pdf(pdf_bytes)
        else:
            info.setdefault("notes", "PDF download failed")

    transport_data = _get_transport(info["name"]) if include_transport else None
    return make_row(info, fees, sid, curriculum_hint, transport_data)


# ---------------------------------------------------------------------------
# Detail page parsing
# ---------------------------------------------------------------------------

def _parse_detail_page(html: str, school_id: str) -> dict:
    text = html_to_text(html)
    info: dict = {}

    m = re.search(r"<title>(.*?)</title>", html)
    info["name"] = m.group(1).strip() if m else f"School {school_id}"

    m = re.search(r"\bArea\b\s+(.+?)\s+(?:Phone Number|Email|School ID|Established)", text)
    info["area"] = m.group(1).strip() if m else ""

    m = re.search(
        r"Evaluation\s+(Outstanding|Very Good|Good|Acceptable|Weak|Very Weak|Not Reviewed)",
        text, re.IGNORECASE,
    )
    info["rating"] = m.group(1).strip() if m else ""

    m = re.search(
        r"Curriculum\s+([\w ]{2,25}?)\s+(?:Evaluation|Accreditation|School ID)",
        text, re.IGNORECASE,
    )
    info["curriculum"] = m.group(1).strip().title() if m else ""

    # Fee PDF — first /media/*.pdf that isn't an inspection report
    fee_pdf = None
    for href in re.findall(r'href=["\'](/media/[^"\']+\.pdf)["\']', html):
        low = htmlmod.unescape(href).lower()
        if not any(x in low for x in ["report", "spr", "2023", "2024en", "inspection"]):
            fee_pdf = BASE + quote(htmlmod.unescape(href), safe="/:.-_~")
            break
    info["fee_pdf_url"] = fee_pdf

    return info


# ---------------------------------------------------------------------------
# PDF fee parsing
# ---------------------------------------------------------------------------

_GRADE_PREFIX = re.compile(
    r"^(FS\d|Y\d{1,2}|KG\d?|Pre[\s-]?KG|Nursery|\d{1,2})$", re.IGNORECASE
)


def _parse_fee_pdf(pdf_bytes: bytes) -> dict:
    """
    Two PDF layouts exist on SPEA:

    Format A (Arabic approval letter) — grade label is the LAST cell:
        [total_fee, uniform, tuition, grade_label]

    Format B (speaschoolfees_NNN.pdf) — grade label is FIRST:
        [grade_label, school_fees, uniform]

    Detection: if cells[0] matches a known grade prefix → Format B.
    """
    try:
        import pdfplumber

        fees: dict = {}
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                for table in (page.extract_tables() or []):
                    for row in table:
                        cells = [c.strip() for c in row if c is not None and c.strip()]
                        if len(cells) < 2:
                            continue

                        if _GRADE_PREFIX.match(cells[0]):       # Format B
                            grade_label = cells[0]
                            fee_val = next(
                                (float(c.replace(",", "")) for c in cells[1:]
                                 if re.match(r"^\d[\d,]+$", c)),
                                None,
                            )
                        else:                                    # Format A
                            grade_label = cells[-1]
                            fee_val = None
                            for c in ([cells[2]] if len(cells) > 2 else []) + [cells[0]]:
                                if re.match(r"^\d[\d,]+$", c):
                                    fee_val = float(c.replace(",", ""))
                                    break

                        if not fee_val or fee_val < 500:
                            continue
                        col = resolve_grade(grade_label)
                        if col and (col not in fees or fee_val > fees[col]):
                            fees[col] = fee_val

                # Text fallback when table extraction returns nothing
                if not fees:
                    raw = page.extract_text() or ""
                    for m in re.finditer(           # Format A text: "22440 FS1"
                        r"(\d[\d,]+)\s+(FS\d|Y\d{1,2}|KG\d?|Pre[\s-]?KG|Nursery|Grade\s*\d+)",
                        raw, re.IGNORECASE,
                    ):
                        col = resolve_grade(m.group(2))
                        amt = float(m.group(1).replace(",", ""))
                        if col and amt >= 500 and (col not in fees or amt > fees[col]):
                            fees[col] = amt
                    for m in re.finditer(           # Format B text: "FS1 9670"
                        r"\b(FS\d|Y\d{1,2}|KG\d?|\d{1,2})\s+(\d[\d,]+)",
                        raw, re.IGNORECASE,
                    ):
                        col = resolve_grade(m.group(1))
                        amt = float(m.group(2).replace(",", ""))
                        if col and amt >= 500 and (col not in fees or amt > fees[col]):
                            fees[col] = amt
        return fees
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Transport lookup
# ---------------------------------------------------------------------------

def _get_transport(school_name: str) -> tuple | None:
    """Exact match first, then partial match against TRANSPORT_DATA."""
    data = TRANSPORT_DATA.get(school_name)
    if data:
        return data
    name_l = school_name.lower()
    for key, val in TRANSPORT_DATA.items():
        if key.lower() in name_l or name_l in key.lower():
            return val
    return None


# ---------------------------------------------------------------------------
# Row builder
# ---------------------------------------------------------------------------

def make_row(info: dict, fees: dict, school_id: str,
             curriculum_hint: str = "", transport_data=None) -> dict:
    all_fees = [v for v in fees.values() if v]
    td = transport_data
    curriculum = info.get("curriculum") or curriculum_hint
    return {
        "school_name":               info["name"],
        "curriculum":                curriculum,
        "spea_rating":               info.get("rating", ""),
        "area":                      info.get("area", ""),
        "tuition_nursery_ecc":       fees.get("tuition_nursery_ecc", ""),
        "tuition_fs1":               fees.get("tuition_fs1", ""),
        "tuition_fs2":               fees.get("tuition_fs2", ""),
        "tuition_primary":           fees.get("tuition_primary", ""),
        "tuition_secondary_y7_9":    fees.get("tuition_secondary_y7_9", ""),
        "tuition_gcse_y10_11":       fees.get("tuition_gcse_y10_11", ""),
        "tuition_sixth_form_y12_13": fees.get("tuition_sixth_form_y12_13", ""),
        "fee_currency":              "AED" if fees else "",
        "fee_period":                "Annual",
        "spea_lowest_fee":           min(all_fees) if all_fees else "",
        "spea_highest_fee":          max(all_fees) if all_fees else "",
        "transport_fee_min_aed":     td[0] if td else "",
        "transport_fee_max_aed":     td[1] if td else "",
        "transport_notes":           td[2] if td else "",
        "source_url":                DETAIL_URL.format(sid=school_id),
        "pdf_url":                   info.get("fee_pdf_url", ""),
        "notes":                     info.get("notes", "") or ("" if fees else "No fee data extracted"),
    }
