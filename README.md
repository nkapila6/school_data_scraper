# school-data-scraper

Scrapes school tuition and transport fees from:
- **KHDA** — Dubai (230 schools, all curricula)
- **SPEA** — Sharjah (96 schools, all curricula)

Output is a CSV per authority with per-grade tuition fees, authority rating, area, and transport fee range.

---

## Setup

```bash
uv sync
```

Requires `curl` on your PATH (pre-installed on macOS/Linux).

---

## Usage

```bash
# All schools
uv run scraper khda
uv run scraper spea
uv run scraper both          # runs KHDA then SPEA

# Filter by curriculum
uv run scraper khda --curriculum British
uv run scraper spea --curriculum Indian

# Filter by school name or SPEA numeric ID
uv run scraper khda --school "GEMS Wellington"
uv run scraper spea --school "Pace British"
uv run scraper spea --school 384

# Options
uv run scraper khda --output dubai.csv     # custom output file
uv run scraper spea --resume               # skip already-scraped schools
uv run scraper khda --workers 10           # parallel workers (default: 5)
uv run scraper spea --no-transport         # skip transport fees
```

Output files: `output_khda.csv` and `output_spea.csv` (created in the working directory).

---

## Output columns

| Column | Description |
|---|---|
| `school_name` | School name as listed by the authority |
| `curriculum` | e.g. British, American, Indian, MoE |
| `dsib_rating` / `spea_rating` | Authority inspection rating |
| `area` | District / area |
| `tuition_nursery_ecc` | Annual tuition — Nursery/KG/ECC |
| `tuition_fs1` / `tuition_fs2` | Foundation Stage 1 & 2 |
| `tuition_primary` | Highest fee in Years 1–6 / Grades 1–6 |
| `tuition_secondary_y7_9` | Years 7–9 / Grades 7–9 |
| `tuition_gcse_y10_11` | Years 10–11 / Grades 10–11 |
| `tuition_sixth_form_y12_13` | Years 12–13 / Grades 12–13 |
| `khda_lowest_fee` / `khda_highest_fee` | KHDA summary range |
| `spea_lowest_fee` / `spea_highest_fee` | SPEA fee range from PDF |
| `transport_fee_min_aed` | Cheapest transport route (annual AED) |
| `transport_fee_max_aed` | Most expensive route (annual AED) |
| `transport_notes` | Source and route detail (SPEA only) |
| `source_url` | Authority detail page |
| `pdf_url` | Fee PDF URL (SPEA only) |

---

## SPEA curricula

`MoE`, `American`, `British`, `Indian`, `Pakistani`, `SABIS`, `Australian`, `Pilipinas`, `French`, `German`

---

## Notes

- **KHDA transport fees** are fetched from the KHDA fact-sheet pages (well-structured, ~90% coverage).
- **SPEA transport fees** are not published centrally; data for 8 British schools was manually sourced from school websites (2025–2026 academic year).
- All fees are in **AED, annual**.
- The scraper uses only `curl` — no headless browser or API keys required.
