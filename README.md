# Bosch Drill Catalog Crawler (Germany)

Crawls bosch-professional.com/de/de to build a product catalog for all drill-category products, conforming to `schema_v4.json`.

## Scope

| Subtype | Family Prefix | Source |
|---|---|---|
| DRILL_DRIVER | GSR, EXSR, GBM | Cordless drill/drivers + corded drills |
| IMPACT_DRILL | GSB, EXSB | Impact drill/drivers |
| ROTARY_HAMMER | GBH, EXBH | SDS-plus + SDS-max rotary hammers |
| ANGLE_DRILL | GWB | Angle drills |
| DRYWALL_SCREWDRIVER | GTB, GRD | Drywall screwdrivers |

Excluded: GSH (demolition hammers), GDE (dust extraction), GMA/GHT (accessories).

## Setup

```bash
cd jay/pim/crawler

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
playwright install chromium
```

## Usage

```bash
# Full crawl — discover + extract all drill products
python crawl_drills_de.py

# Just list discovered products (no detail extraction)
python crawl_drills_de.py --list-only

# Re-run extraction from cached HTML (no re-fetching)
python crawl_drills_de.py --use-cache

# Test single product
python crawl_drills_de.py --product-url "https://www.bosch-professional.com/de/de/products/gsr-18v-90-c-06019K6000"

# Verbose logging
python crawl_drills_de.py -v
```

## Output

| File | Description |
|---|---|
| `jay/pim/catalog_drills_de.json` | Product catalog (JSON array conforming to schema_v4) |
| `jay/pim/catalog_drills_de_meta.json` | Crawl metadata: counts, errors, timing |
| `jay/pim/crawler/.cache/` | Cached HTML pages (auto-created, gitignore) |

## How It Works

1. **Discovery** — Crawls 2 verified category pages (drills + rotary hammers), paginates through all pages, extracts product URLs via regex on raw HTML
2. **Filtering** — Classifies each product by family prefix (GSR/GBH/etc.) and excludes non-drill items (accessories, demolition hammers)
3. **Extraction** — For each product detail page:
   - Parses JSON-LD structured data (ProductGroup with offers/variants)
   - Extracts spec table key-value pairs from raw HTML
   - Collects images, PDFs, pricing from all SKU variants
   - Maps German spec labels to schema fields
4. **Validation** — Every product validates via Pydantic against schema_v4
5. **Output** — Writes catalog JSON + meta summary

## Data Quality

Each product record includes (when available on the source page):
- All SKU variants with individual GTINs and prices
- Full technical specs (voltage, torque, RPM, weight, noise, vibration)
- Lieferumfang (delivery contents) per variant
- Product images and PDF documentation links
- ML-ready flattened feature dict
