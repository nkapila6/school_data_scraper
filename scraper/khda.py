"""
KHDA (Dubai) scraper.

Fetches tuition fees by grade, DSIB rating, area, curriculum, and transport
fees for every school in the KHDA directory. Everything is extracted from
static HTML — no browser automation required.
"""

import re
import time

from .common import curl_get, html_to_text, resolve_grade

BASE    = "https://web.khda.gov.ae"
DETAIL  = f"{BASE}/en/Education-Directory/Schools/School-Details"
LISTING = f"{BASE}/en/Education-Directory/schools"

FIELDNAMES = [
    "school_name", "curriculum", "dsib_rating", "area",
    "tuition_nursery_ecc",
    "tuition_fs1",
    "tuition_fs2",
    "tuition_primary",
    "tuition_secondary_y7_9",
    "tuition_gcse_y10_11",
    "tuition_sixth_form_y12_13",
    "fee_currency",
    "fee_period",
    "khda_lowest_fee",
    "khda_highest_fee",
    "khda_average_fee",
    "transport_fee_min_aed",
    "transport_fee_max_aed",
    "source_url",
    "notes",
]


# ---------------------------------------------------------------------------
# School list
# ---------------------------------------------------------------------------

def build_school_list() -> list[dict]:
    """
    Return all schools from the KHDA directory as a list of dicts:
      [{"name": ..., "khda_id": ..., "center_id": ...}, ...]

    All 230+ schools are present on a single static HTML page.
    """
    html = curl_get(LISTING)
    pairs = re.findall(
        r'href="[^"]*Id=(\d+)&(?:amp;)?CenterID=(\d+)[^"]*"[^>]*>\s*([^\n<]{5,80}?)\s*</a>',
        html,
    )
    seen, schools = set(), []
    for khda_id, center_id, name in pairs:
        key = (khda_id, center_id)
        if key not in seen:
            seen.add(key)
            schools.append({"name": name.strip(), "khda_id": khda_id, "center_id": center_id})
    return schools


# ---------------------------------------------------------------------------
# Per-school fetch + parse  (called from the thread pool)
# ---------------------------------------------------------------------------

def fetch_school(school: dict, include_transport: bool = True) -> dict:
    """
    Fetch and parse a single KHDA school. Returns a row dict ready for CSV.
    Safe to call from multiple threads simultaneously.
    """
    name      = school["name"]
    khda_id   = school["khda_id"]
    center_id = school["center_id"]
    url       = f"{DETAIL}?Id={khda_id}&CenterID={center_id}"

    html = curl_get(url)
    if not html or "AED" not in html:
        time.sleep(1)
        html = curl_get(url)

    if not html or "AED" not in html:
        return make_row(name, "", {}, {}, "", "", (None, None), url, "Page load failed")

    fees, summary, dsib, area, curriculum, transport = _parse_page(html)

    if not include_transport:
        transport = (None, None)

    notes = "" if (fees or summary) else "No fee data on KHDA page"
    return make_row(name, curriculum, fees, summary, dsib, area, transport, url, notes)


# ---------------------------------------------------------------------------
# Page parsing
# ---------------------------------------------------------------------------

def _parse_page(html: str) -> tuple:
    """
    Parse a KHDA school detail page (static HTML).

    Returns (fees, summary, dsib_rating, area, curriculum, transport).
    """
    text = html_to_text(html)

    # DSIB overall rating
    dsib_m = re.search(
        r"(Outstanding|Very good|Good|Acceptable|Weak|Very weak)\s+Overall Rating",
        text, re.IGNORECASE,
    )
    dsib_rating = dsib_m.group(1) if dsib_m else ""

    # Area
    area_m = re.search(
        r"Location\s+(.+?)\s+(?:Text|Phone|Email|Website|school)",
        text, re.IGNORECASE,
    )
    area = area_m.group(1).strip() if area_m else ""

    # Curriculum — read from the structured HTML card to avoid false matches
    # against other school names that appear in the page sidebar.
    # Pattern: <span>UK (13 Y)</span><strong>Curriculum </strong>
    curr_m = re.search(r"<span>([^<]+)</span>\s*<strong>Curriculum", html)
    if curr_m:
        curriculum = re.sub(r"\s*\(\d+\s*Y\)", "", curr_m.group(1)).strip()
    else:
        curriculum = ""

    # Summary fees (shown at top of page)
    summary = {}
    for key, pattern in [
        ("lowest",  r"([\d,]+)\s+arrow_downward\s+Lowest"),
        ("highest", r"([\d,]+)\s+arrow_upward\s+Highest"),
        ("average", r"([\d,]+)\s+swap_horiz\s+Average"),
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            summary[key] = float(m.group(1).replace(",", ""))

    # Grade-by-grade fees
    # Static HTML text format: "GRADE 5 AED 25,953" / "YEAR 10 AED 46,350"
    fees = {}
    grade_aed_re = re.compile(
        r"(year\s*\d+|grade\s*\d+|fs\s*\d+|kg\s*\d*"
        r"|nursery|ecc|pre[\s-]?primary|pre[\s-]?kg"
        r"|foundation\s+(?:stage\s+)?\d+)"
        r"\s+AED\s+([\d,]+)",
        re.IGNORECASE,
    )
    for m in grade_aed_re.finditer(text):
        col = resolve_grade(m.group(1))
        if col is None:
            continue
        val = float(m.group(2).replace(",", ""))
        if val >= 500 and (col not in fees or val > fees[col]):
            fees[col] = val

    # Transport fee via fact-sheet URL embedded in the page
    transport = _transport_from_html(html)

    return fees, summary, dsib_rating, area, curriculum, transport


def _transport_from_html(school_html: str) -> tuple:
    """
    Grab the first data-factsheet URL, fetch it, and parse the transport fee.
    Returns (min_fee, max_fee) or (None, None).
    """
    m = re.search(r'data-factsheet="([^"]+)"', school_html)
    if not m:
        return None, None
    fs_text = html_to_text(curl_get(m.group(1)))
    return _parse_transport(fs_text)


def _parse_transport(text: str) -> tuple:
    m = re.search(r"Transport\s+([\d,]+)\s+to\s+([\d,]+)", text, re.IGNORECASE)
    if m:
        return float(m.group(1).replace(",", "")), float(m.group(2).replace(",", ""))
    m = re.search(r"Transport\s+([\d,]+)", text, re.IGNORECASE)
    if m:
        fee = float(m.group(1).replace(",", ""))
        return fee, fee
    return None, None


# ---------------------------------------------------------------------------
# Row builder
# ---------------------------------------------------------------------------

def make_row(
    name, curriculum, fees, summary, dsib_rating, area,
    transport, source_url, notes=""
) -> dict:
    min_t, max_t = transport if transport else (None, None)
    return {
        "school_name":               name,
        "curriculum":                curriculum,
        "dsib_rating":               dsib_rating,
        "area":                      area,
        "tuition_nursery_ecc":       fees.get("tuition_nursery_ecc", ""),
        "tuition_fs1":               fees.get("tuition_fs1", ""),
        "tuition_fs2":               fees.get("tuition_fs2", ""),
        "tuition_primary":           fees.get("tuition_primary", ""),
        "tuition_secondary_y7_9":    fees.get("tuition_secondary_y7_9", ""),
        "tuition_gcse_y10_11":       fees.get("tuition_gcse_y10_11", ""),
        "tuition_sixth_form_y12_13": fees.get("tuition_sixth_form_y12_13", ""),
        "fee_currency":              "AED" if (fees or summary) else "",
        "fee_period":                "Annual",
        "khda_lowest_fee":           summary.get("lowest", ""),
        "khda_highest_fee":          summary.get("highest", ""),
        "khda_average_fee":          summary.get("average", ""),
        "transport_fee_min_aed":     "" if min_t is None else min_t,
        "transport_fee_max_aed":     "" if max_t is None else max_t,
        "source_url":                source_url,
        "notes":                     notes,
    }
