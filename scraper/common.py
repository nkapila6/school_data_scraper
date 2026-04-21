"""
Shared utilities: HTTP, HTML stripping, and grade-to-column mapping.
"""

import re
import subprocess


def curl_get(url, referer=None, binary=False, timeout=25):
    """Fetch *url* with curl. Returns bytes when *binary=True*, else str."""
    cmd = [
        "curl", "-sL",
        "-A", (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "--max-time", str(timeout),
    ]
    if referer:
        cmd += ["-H", f"Referer: {referer}"]
    cmd.append(url)
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=timeout + 10)
        return result.stdout if binary else result.stdout.decode("utf-8", errors="replace")
    except Exception:
        return b"" if binary else ""


def html_to_text(html):
    """Strip tags and collapse whitespace into a single-line string."""
    h = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL)
    h = re.sub(r"<style[^>]*>.*?</style>",   "", h,    flags=re.DOTALL)
    h = re.sub(r"data:[^\"' ]+", "", h)
    t = re.sub(r"<[^>]+>", " ", h)
    return re.sub(r"\s+", " ", t).strip()


# ---------------------------------------------------------------------------
# Grade label → output column mapping
# Covers British (FS/Year), American (Grade), CBSE (plain numbers), KG, etc.
# ---------------------------------------------------------------------------

GRADE_COL: dict[str, str] = {
    # Nursery / ECC / Pre-KG / KG
    "nursery":            "tuition_nursery_ecc",
    "pre-kg":             "tuition_nursery_ecc",
    "pre kg":             "tuition_nursery_ecc",
    "pre-primary":        "tuition_nursery_ecc",
    "pre primary":        "tuition_nursery_ecc",
    "ecc":                "tuition_nursery_ecc",
    "kg":                 "tuition_nursery_ecc",
    "kg1":                "tuition_nursery_ecc",
    "kg 1":               "tuition_nursery_ecc",
    "kg2":                "tuition_nursery_ecc",
    "kg 2":               "tuition_nursery_ecc",
    # Foundation Stage
    "fs1":                "tuition_fs1",
    "fs 1":               "tuition_fs1",
    "fs2":                "tuition_fs2",
    "fs 2":               "tuition_fs2",
    "foundation 1":       "tuition_fs1",
    "foundation 2":       "tuition_fs2",
    "foundation stage 1": "tuition_fs1",
    "foundation stage 2": "tuition_fs2",
    # Primary — Year/Grade 1-6
    **{k: "tuition_primary" for k in [
        "y1", "year 1", "grade 1", "1",
        "y2", "year 2", "grade 2", "2",
        "y3", "year 3", "grade 3", "3",
        "y4", "year 4", "grade 4", "4",
        "y5", "year 5", "grade 5", "5",
        "y6", "year 6", "grade 6", "6",
    ]},
    # Secondary — Year/Grade 7-9
    **{k: "tuition_secondary_y7_9" for k in [
        "y7", "year 7", "grade 7", "7",
        "y8", "year 8", "grade 8", "8",
        "y9", "year 9", "grade 9", "9",
    ]},
    # GCSE — Year/Grade 10-11
    **{k: "tuition_gcse_y10_11" for k in [
        "y10", "year 10", "grade 10", "10",
        "y11", "year 11", "grade 11", "11",
    ]},
    # Sixth Form — Year/Grade 12-13
    **{k: "tuition_sixth_form_y12_13" for k in [
        "y12", "year 12", "grade 12", "12",
        "y13", "year 13", "grade 13", "13",
    ]},
}


def resolve_grade(label: str) -> str | None:
    """Normalise a grade label to a GRADE_COL column name, or return None."""
    key = label.strip().lower()
    col = GRADE_COL.get(key)
    if col:
        return col
    m = re.match(r"(grade|year)\s*(\d+)", key)
    if m:
        return GRADE_COL.get(f"{m.group(1)} {m.group(2)}")
    return None
