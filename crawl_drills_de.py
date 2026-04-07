#!/usr/bin/env python3
"""
Bosch Professional Drill Catalog Crawler (Germany Market)

Builds a complete product catalog for all drill-category products on
bosch-professional.com/de/de, conforming to schema_v4.json.

Strategy:
  1. Use a verified category URL registry (not guessing selectors)
  2. Fetch each category page, extract product URLs from page content
  3. For each product page: extract JSON-LD + raw page text via WebFetch-style parsing
  4. Map extracted data to pydantic models, validate, and write JSON

Usage:
    python crawl_drills_de.py                  # full crawl
    python crawl_drills_de.py --use-cache      # re-extract from cached pages
    python crawl_drills_de.py --product-url URL # test single product
    python crawl_drills_de.py --list-only       # just discover & list products
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn
from rich.table import Table

from models import (
    Article, ArticleFlags, ArticleType, Availability, Classification,
    Commercial, Compatibility, Dimensions, Document, DocumentType,
    Ids, Lifecycle, LifecycleStatus, Media, ML, Name, Product,
    RawAttribute, Source, SourceSystem, TechSpecs,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://www.bosch-professional.com"

# Verified category URLs for the DE market drill family.
# Each entry: (category_url, subtype_id, category_path)
CATEGORY_REGISTRY: list[tuple[str, str, list[str]]] = [
    # Drill Drivers (GSR, EXSR) — cordless drill/drivers
    (
        f"{BASE_URL}/de/de/bohr-schlagbohrmaschinen-101355-ocs-c/",
        "DRILL_DRIVER",
        ["Power Tools", "Drilling", "Drill Drivers"],
    ),
    # Rotary Hammers (GBH, EXBH) — SDS-plus and SDS-max
    (
        f"{BASE_URL}/de/de/bohr-schlaghaemmer-101339-ocs-c/",
        "ROTARY_HAMMER",
        ["Power Tools", "Drilling", "Rotary Hammers"],
    ),
]

# Product family prefix -> subtype override mapping.
# This lets us correctly classify products that land in a broad category.
FAMILY_SUBTYPE_MAP: dict[str, str] = {
    "GSR":  "DRILL_DRIVER",
    "EXSR": "DRILL_DRIVER",
    "GBM":  "DRILL_DRIVER",        # corded drill machines
    "GSB":  "IMPACT_DRILL",         # impact drill/drivers (Schlagbohrschrauber)
    "EXSB": "IMPACT_DRILL",
    "GWB":  "ANGLE_DRILL",          # angle drills
    "GTB":  "DRYWALL_SCREWDRIVER",  # drywall screwdrivers
    "GRD":  "DRYWALL_SCREWDRIVER",  # drywall screwdrivers (cordless)
    "GMA":  "ACCESSORY",            # adapter attachment — skip
    "GBH":  "ROTARY_HAMMER",        # rotary hammers
    "EXBH": "ROTARY_HAMMER",
    "GSH":  "DEMOLITION_HAMMER",    # demolition-only — skip for drill catalog
    "GDE":  "DUST_EXTRACTION",      # dust extraction accessory — skip
    "GHT":  "ACCESSORY",            # trolley — skip
}

# Families to INCLUDE in the drill catalog
DRILL_FAMILIES = {"DRILL_DRIVER", "IMPACT_DRILL", "HAMMER_DRILL", "ROTARY_HAMMER", "ANGLE_DRILL", "DRYWALL_SCREWDRIVER"}

# Families to EXCLUDE (accessories, demolition-only, dust extraction)
SKIP_FAMILIES = {"ACCESSORY", "DEMOLITION_HAMMER", "DUST_EXTRACTION"}

CACHE_DIR = Path(__file__).parent / ".cache"
OUTPUT_DIR = Path(__file__).parent.parent
OUTPUT_FILE = OUTPUT_DIR / "catalog_drills_de.json"
META_FILE = OUTPUT_DIR / "catalog_drills_de_meta.json"

REQUEST_DELAY_S = 2.5
PAGE_TIMEOUT_MS = 30_000

console = Console()
log = logging.getLogger("crawler")


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def _cache_path(url: str) -> Path:
    h = hashlib.sha256(url.encode()).hexdigest()[:16]
    slug = re.sub(r"[^a-zA-Z0-9]", "_", url.split("/")[-1] or url.split("/")[-2])[:60]
    return CACHE_DIR / f"{slug}_{h}.html"


def cache_save(url: str, html: str) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _cache_path(url).write_text(html, encoding="utf-8")


def cache_load(url: str) -> str | None:
    p = _cache_path(url)
    return p.read_text(encoding="utf-8") if p.exists() else None


# ---------------------------------------------------------------------------
# Browser
# ---------------------------------------------------------------------------

def make_context(browser: Browser) -> BrowserContext:
    return browser.new_context(
        locale="de-DE",
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
    )


def accept_cookies(page: Page) -> None:
    """Dismiss cookie consent if present."""
    try:
        for sel in [
            "#onetrust-accept-btn-handler",
            "button:has-text('Alle akzeptieren')",
            "button:has-text('Accept All')",
            "[data-testid='cookie-accept-all']",
        ]:
            btn = page.locator(sel)
            if btn.count() > 0 and btn.first.is_visible():
                btn.first.click(timeout=3000)
                page.wait_for_timeout(1000)
                return
    except Exception:
        pass


def fetch_page(page: Page, url: str, *, use_cache: bool = False) -> str:
    """Navigate to URL, return full HTML. Uses cache if available."""
    if use_cache:
        cached = cache_load(url)
        if cached:
            log.debug("Cache hit: %s", url)
            return cached

    log.info("Fetching: %s", url)
    page.goto(url, wait_until="networkidle", timeout=PAGE_TIMEOUT_MS)
    accept_cookies(page)

    # Scroll down to trigger lazy-loaded content
    page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
    page.wait_for_timeout(1500)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)

    html = page.content()
    cache_save(url, html)
    time.sleep(REQUEST_DELAY_S)
    return html


# ---------------------------------------------------------------------------
# Step 1: Discover product URLs from category pages
# ---------------------------------------------------------------------------

def discover_products_from_category(
    page: Page,
    category_url: str,
    *,
    use_cache: bool = False,
) -> list[dict[str, str]]:
    """
    Fetch category listing pages and extract product URLs.
    Returns list of {url, name, sku}.
    """
    products: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    # The Bosch site paginates: /page/1/, /page/2/, etc.
    page_num = 0
    while True:
        page_num += 1
        url = category_url if page_num == 1 else f"{category_url}page/{page_num}/"
        log.info("Category page %d: %s", page_num, url)

        try:
            html = fetch_page(page, url, use_cache=use_cache)
        except Exception as e:
            log.warning("Failed to load category page %d: %s", page_num, e)
            break

        # Extract product URLs using regex on the raw HTML.
        # Bosch uses absolute URLs: https://www.bosch-professional.com/de/de/products/<slug>-<sku>
        matches = re.findall(
            r'href="(https://www\.bosch-professional\.com/de/de/products/([a-zA-Z0-9\-]+)-(\w{10}))"',
            html,
        )

        new_count = 0
        for full_url, slug, sku in matches:
            if full_url not in seen_urls:
                seen_urls.add(full_url)
                name = slug.replace("-", " ").upper()
                products.append({"url": full_url, "name": name, "sku": sku})
                new_count += 1

        log.info("  Found %d new products (total: %d)", new_count, len(products))

        # If no new products found on this page, we've exhausted pagination
        if new_count == 0:
            break

    return products


def discover_all_products(page: Page, *, use_cache: bool = False) -> list[dict]:
    """Discover products from all category pages, deduplicate, and filter."""
    all_products: list[dict] = []
    seen_urls: set[str] = set()

    for cat_url, default_subtype, cat_path in CATEGORY_REGISTRY:
        console.print(f"  Scanning: [cyan]{cat_url}[/cyan]")
        found = discover_products_from_category(page, cat_url, use_cache=use_cache)
        for p in found:
            if p["url"] not in seen_urls:
                seen_urls.add(p["url"])
                p["default_subtype"] = default_subtype
                p["category_path"] = cat_path
                all_products.append(p)

    # Filter: determine family from slug and exclude non-drill items
    filtered: list[dict] = []
    for p in all_products:
        family = _infer_family_from_slug(p["url"])
        subtype = FAMILY_SUBTYPE_MAP.get(family, p.get("default_subtype", "UNKNOWN"))

        if subtype in SKIP_FAMILIES:
            log.info("  Skipping %s (%s / %s)", p["name"], family, subtype)
            continue

        p["family"] = family
        p["subtype"] = subtype
        filtered.append(p)

    return filtered


def _infer_family_from_slug(url: str) -> str:
    """Extract the product family prefix (GSR, GBH, etc.) from URL slug."""
    # URL pattern: /products/<slug>-<sku>
    match = re.search(r"/products/([a-z]+)", url)
    if match:
        prefix = match.group(1).upper()
        # Handle multi-part prefixes
        for known in ["EXSR", "EXSB", "EXBH", "GSR", "GSB", "GBM", "GBH", "GSH",
                       "GWB", "GTB", "GRD", "GMA", "GDE", "GHT"]:
            if prefix.startswith(known.lower().replace("-", "")) or prefix == known.lower():
                return known
        # Fallback: first 3 uppercase letters
        return prefix[:3]
    return "UNKNOWN"


# ---------------------------------------------------------------------------
# Step 2: Extract product detail
# ---------------------------------------------------------------------------

def _safe_float(val: str | None) -> float | None:
    """Parse a float from a German-formatted string."""
    if not val:
        return None
    # Remove units, keep digits, dots, commas, minus
    cleaned = re.sub(r"[^\d.,\-]", "", val.strip())
    cleaned = cleaned.replace(",", ".")
    # If multiple dots (e.g. "1.600"), remove thousands separator
    parts = cleaned.split(".")
    if len(parts) > 2:
        cleaned = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def extract_json_ld(html: str) -> list[dict]:
    """Extract all JSON-LD blocks from raw HTML."""
    results = []
    for match in re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.DOTALL):
        try:
            data = json.loads(match.group(1))
            if isinstance(data, list):
                results.extend(data)
            else:
                results.append(data)
        except json.JSONDecodeError:
            continue
    return results


def extract_product_json_ld(html: str) -> dict | None:
    """Find the Product or ProductGroup JSON-LD."""
    for item in extract_json_ld(html):
        if isinstance(item, dict) and item.get("@type") in ("Product", "ProductGroup"):
            return item
    return None


def extract_variants(json_ld: dict) -> list[dict]:
    """
    Extract product variants from JSON-LD ProductGroup.
    Bosch DE uses: ProductGroup.hasVariant[] -> each has sku, gtin13, name, offers[].
    Returns normalized list of {sku, gtin13, name, price, currency, availability}.
    """
    variants = json_ld.get("hasVariant", [])
    results = []
    for v in variants:
        if not isinstance(v, dict):
            continue
        entry: dict[str, Any] = {
            "sku": v.get("sku", ""),
            "gtin13": v.get("gtin13", ""),
            "name": v.get("name", ""),
            "description": v.get("description", ""),
            "price": None,
            "currency": None,
            "availability": "",
        }
        # offers is a list of Offer objects
        offers = v.get("offers", [])
        if isinstance(offers, list) and offers:
            o = offers[0]
            entry["price"] = _safe_float(str(o.get("price", "")))
            entry["currency"] = o.get("priceCurrency", "EUR")
            entry["availability"] = o.get("availability", "")
        elif isinstance(offers, dict):
            entry["price"] = _safe_float(str(offers.get("price", "")))
            entry["currency"] = offers.get("priceCurrency", "EUR")
            entry["availability"] = offers.get("availability", "")
        results.append(entry)

    # Fallback: check top-level offers if no hasVariant
    if not results:
        offers = json_ld.get("offers")
        if isinstance(offers, dict):
            offers = [offers]
        if isinstance(offers, list):
            for o in offers:
                if isinstance(o, dict):
                    results.append({
                        "sku": o.get("sku", json_ld.get("sku", "")),
                        "gtin13": o.get("gtin13", ""),
                        "name": o.get("name", ""),
                        "description": "",
                        "price": _safe_float(str(o.get("price", ""))),
                        "currency": o.get("priceCurrency", "EUR"),
                        "availability": o.get("availability", ""),
                    })
    return results


def extract_spec_pairs(html: str) -> dict[str, str]:
    """
    Extract technical spec key-value pairs from the page HTML.

    Bosch DE uses this DOM structure:
        <div class="table__body-row">
            <div class="table__body-cell"><span>KEY</span></div>
            <div class="table__body-cell"><span>VALUE</span></div>
        </div>
    """
    specs: dict[str, str] = {}

    # Primary pattern: Bosch table__body-row with two table__body-cell children
    for m in re.finditer(
        r'<div\s+class="table__body-row">\s*'
        r'<div\s+class="table__body-cell">\s*<span>(.*?)</span>\s*</div>\s*'
        r'<div\s+class="table__body-cell">\s*<span>(.*?)</span>',
        html, re.DOTALL,
    ):
        key = re.sub(r"<[^>]+>", "", m.group(1)).strip()
        val = re.sub(r"<[^>]+>", "", m.group(2)).strip()
        # Handle checkmark icons as "Yes"
        if not val and "icon-checkmark" in m.group(2):
            val = "Yes"
        if key and val and len(key) < 120:
            specs[key] = val

    # Fallback: also check for icon-checkmark in the value cell
    if not specs:
        for m in re.finditer(
            r'<div\s+class="table__body-row">(.*?)</div>\s*</div>',
            html, re.DOTALL,
        ):
            spans = re.findall(r'<span>(.*?)</span>', m.group(1), re.DOTALL)
            if len(spans) >= 2:
                key = re.sub(r"<[^>]+>", "", spans[0]).strip()
                val = re.sub(r"<[^>]+>", "", spans[1]).strip()
                if key and val:
                    specs[key] = val

    # Secondary: standard <tr>/<td> tables
    if not specs:
        for m in re.finditer(
            r"<t[hd][^>]*>\s*(.*?)\s*</t[hd]>\s*<td[^>]*>\s*(.*?)\s*</td>",
            html, re.DOTALL,
        ):
            key = re.sub(r"<[^>]+>", "", m.group(1)).strip()
            val = re.sub(r"<[^>]+>", "", m.group(2)).strip()
            if key and val and len(key) < 100:
                specs[key] = val

    return specs


def parse_spec_value(specs: dict[str, str], *labels: str) -> str | None:
    """Find first matching key in specs (case-insensitive partial match)."""
    for label in labels:
        for key, val in specs.items():
            if label.lower() in key.lower():
                return val
    return None


def extract_product_detail(
    page: Page,
    url: str,
    meta: dict,
    *,
    use_cache: bool = False,
) -> Product | None:
    """
    Fetch a product detail page and build a Product model.

    Args:
        page: Playwright page
        url: Product detail URL
        meta: Dict with pre-known info (family, subtype, sku, category_path)
    """
    try:
        html = fetch_page(page, url, use_cache=use_cache)
    except Exception as e:
        log.error("Failed to fetch %s: %s", url, e)
        return None

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ---- JSON-LD ----
    json_ld = extract_product_json_ld(html)
    if not json_ld:
        log.warning("No JSON-LD found for %s", url)

    # ---- Product name ----
    display_name = ""
    if json_ld:
        display_name = json_ld.get("name", "")
    if not display_name:
        m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.DOTALL)
        if m:
            display_name = re.sub(r"<[^>]+>", "", m.group(1)).strip()
    if not display_name:
        log.warning("No product name for %s — skipping", url)
        return None

    # ---- Description (short) ----
    description = ""
    if json_ld:
        description = json_ld.get("description", "")

    # ---- Variants / SKU articles ----
    variants = extract_variants(json_ld) if json_ld else []
    log.info("  %s — %d variants found", display_name, len(variants))

    # ---- Spec table ----
    specs = extract_spec_pairs(html)
    log.info("  Specs: %d pairs extracted", len(specs))

    # ---- Build articles from variants ----
    articles: list[Article] = []
    all_skus: list[str] = []
    all_gtins: list[str] = []
    base_price: float | None = None

    for variant in variants:
        sku = variant.get("sku", "")
        gtin = variant.get("gtin13", "")
        price = variant.get("price")
        avail_url = variant.get("availability", "")
        var_name = variant.get("name", "")
        var_desc = variant.get("description", "")

        if sku:
            all_skus.append(sku)
        if gtin:
            all_gtins.append(gtin)
        if price and (base_price is None or price < base_price):
            base_price = price

        # Determine article type: bare tool (cheapest/no battery mention) vs kit
        name_lower = (var_name + " " + var_desc).lower()
        has_battery = any(kw in name_lower for kw in [
            "akku", "batterie", "battery", "li-ion", "procore", "ladegerät", "charger",
        ])
        has_case = any(kw in name_lower for kw in ["l-boxx", "koffer", "case"])

        if has_battery:
            art_type = ArticleType.KIT
        elif has_case and not has_battery:
            art_type = ArticleType.KIT  # case variant is still a kit
        else:
            art_type = ArticleType.BARE_TOOL

        # Parse delivery contents from variant name (Bosch encodes contents in name)
        contents: list[str] = []
        if var_desc:
            for item in re.split(r"[,;•·\n]", var_desc):
                item = item.strip()
                if item and len(item) > 2:
                    contents.append(item)
        if not contents and has_battery:
            # Parse from variant name like "GSR 18V-90 C in L-BOXX 136 mit 2 x Li-Ion-Akku 4,0 Ah, Schnellladegerät"
            after_in = re.split(r"\bin\b|\bmit\b", var_name, flags=re.IGNORECASE)
            if len(after_in) > 1:
                parts = ", ".join(after_in[1:])
                for item in re.split(r"[,;]", parts):
                    item = item.strip()
                    if item and len(item) > 2:
                        contents.append(item)

        articles.append(Article(
            articleType=art_type,
            sku=sku or None,
            gtin=gtin or None,
            flags=None,
            skuContents=contents if contents else None,
            skuDescription=var_name or None,
            rawAttributes=None,
        ))

    # If no articles from offers, create a minimal one
    if not articles:
        articles.append(Article(
            articleType=ArticleType.BARE_TOOL,
            sku=meta.get("sku"),
        ))
        if meta.get("sku"):
            all_skus.append(meta["sku"])

    # ---- Tech specs ----
    voltage_str = parse_spec_value(specs, "Akkuspannung", "Nennspannung", "Spannung", "Battery voltage", "Nennaufnahmeleistung")
    voltage = _safe_float(voltage_str)

    torque_str = parse_spec_value(specs, "Drehmoment", "Max. Drehmoment", "Torque")
    torque = None
    if torque_str:
        # May be "36/64/- Nm" — take the highest number
        nums = [_safe_float(x) for x in re.findall(r"[\d.,]+", torque_str)]
        nums = [n for n in nums if n is not None and n > 0]
        torque = max(nums) if nums else None

    rpm_str = parse_spec_value(specs, "Leerlaufdrehzahl", "Drehzahl", "No-load speed", "Nenndrehzahl")
    rpm_values: list[float] = []
    if rpm_str:
        # German format: "0 – 630 / 0 – 2.100 min-1"
        # Remove thousands separator (dot in German numbers like 2.100)
        clean_rpm = re.sub(r"(\d)\.(\d{3})", r"\1\2", rpm_str)
        nums = [_safe_float(x) for x in re.findall(r"[\d]+", clean_rpm)]
        rpm_values = sorted(set(n for n in nums if n is not None and n > 0))

    weight_str = parse_spec_value(specs, "Gewicht exkl", "Gewicht", "Weight", "Werkzeuggewicht")
    weight = _safe_float(weight_str)

    noise_str = parse_spec_value(specs, "Schalldruckpegel", "Schallleistungspegel", "Schallpegel", "Noise")
    noise = _safe_float(noise_str)

    vibration_str = parse_spec_value(specs, "Schwingungsemissionswert", "Schwingungsemission", "Vibration")
    vibration_val = _safe_float(vibration_str)

    impact_str = parse_spec_value(specs, "Schlagzahl", "Impact rate", "Einzelschlagstärke", "Schlagenergie")
    impact_rate = _safe_float(impact_str)

    # Dimensions from packaging or tool
    dim_str = parse_spec_value(specs, "Abmessung", "Dimension", "Maße", "Kopflänge")
    dimensions = None
    if dim_str:
        dim_nums = re.findall(r"[\d.,]+", dim_str.replace(",", "."))
        if len(dim_nums) >= 3:
            dimensions = Dimensions(
                length=_safe_float(dim_nums[0]),
                width=_safe_float(dim_nums[1]),
                height=_safe_float(dim_nums[2]),
            )

    # Power source inference
    family = meta.get("family", "")
    subtype = meta.get("subtype", "UNKNOWN")
    is_cordless = bool(re.search(r"18V|12V|akku|cordless", display_name, re.IGNORECASE))
    power_source = "Cordless" if is_cordless else "Corded"
    if not voltage and power_source == "Corded":
        voltage = 230.0

    # Platform
    platform = None
    if is_cordless:
        v_match = re.search(r"(\d+)\s*V", display_name)
        if v_match:
            platform = f"{v_match.group(1)}V"
        elif voltage and voltage < 50:
            platform = f"{int(voltage)}V"
    else:
        platform = "Corded"

    # Heavy Duty flag
    heavy_duty = "HEAVY DUTY" in display_name.upper() or "heavy duty" in html.lower()[:5000]

    # Set flag on all articles
    if heavy_duty:
        for art in articles:
            art.flags = ArticleFlags(heavyDuty=True)

    # ---- Features ----
    features: list[str] = []
    # Extract from common highlight patterns in the HTML
    for pattern in [
        r'class="[^"]*(?:highlight|feature|benefit|usp)[^"]*"[^>]*>\s*<[^>]*>\s*(.*?)\s*<',
        r'<li[^>]*class="[^"]*(?:highlight|feature|benefit)[^"]*"[^>]*>(.*?)</li>',
    ]:
        for m in re.finditer(pattern, html, re.DOTALL | re.IGNORECASE):
            text = re.sub(r"<[^>]+>", "", m.group(1)).strip()
            if text and len(text) > 5 and text not in features:
                features.append(text)

    # ---- Images ----
    images: list[str] = []
    # Extract from JSON-LD first
    if json_ld:
        img = json_ld.get("image")
        if isinstance(img, str):
            images.append(img)
        elif isinstance(img, list):
            images.extend([i for i in img if isinstance(i, str)])

    # Also extract from HTML — Bosch uses ocsmedia image URLs
    for m in re.finditer(r'(https://www\.bosch-professional\.com/[^"]*?/(?:application-image|product-image)/[^"]+\.(?:png|jpg|jpeg))', html):
        url_img = m.group(1)
        if url_img not in images:
            images.append(url_img)

    # ---- Documents ----
    documents: list[Document] = []
    for m in re.finditer(r'href="([^"]*\.pdf[^"]*)"', html):
        href = m.group(1)
        full_href = href if href.startswith("http") else f"{BASE_URL}{href}"
        href_lower = full_href.lower()
        if "anleitung" in href_lower or "manual" in href_lower or "betrieb" in href_lower:
            doc_type = DocumentType.MANUAL
        elif "datenblatt" in href_lower or "datasheet" in href_lower:
            doc_type = DocumentType.DATASHEET
        elif "explosion" in href_lower or "ersatzteil" in href_lower:
            doc_type = DocumentType.EXPLODED_VIEW
        else:
            doc_type = DocumentType.OTHER
        if not any(d.ref == full_href for d in documents):
            documents.append(Document(type=doc_type, ref=full_href))

    # ---- Availability ----
    avail = Availability.UNKNOWN
    if any("InStock" in str(v.get("availability", "")) for v in variants):
        avail = Availability.IN_STOCK
    elif any("OutOfStock" in str(v.get("availability", "")) for v in variants):
        avail = Availability.OUT_OF_STOCK

    # ---- Compatibility ----
    compat = None
    if is_cordless and platform:
        sys_compat = [f"Bosch Professional {platform}"]
        if platform == "18V":
            sys_compat.append("AMPShare")
        compat = Compatibility(
            batteryPlatform=f"Bosch Professional {platform} System",
            systemCompatibility=sys_compat,
        )

    # ---- Classification ----
    category_ids = ["POWER_TOOLS", "DRILLS"]
    if subtype == "ROTARY_HAMMER":
        category_ids.append("ROTARY_HAMMERS")
    elif subtype == "IMPACT_DRILL":
        category_ids.append("IMPACT_DRILLS")
    elif subtype == "ANGLE_DRILL":
        category_ids.append("ANGLE_DRILLS")
    elif subtype == "DRILL_DRIVER":
        category_ids.append("DRILL_DRIVERS")
    elif subtype == "DRYWALL_SCREWDRIVER":
        category_ids.append("DRYWALL_SCREWDRIVERS")

    # ---- Raw attributes from spec table ----
    raw_attrs = [RawAttribute(key=k, textValue=v) for k, v in specs.items()]

    # Attach raw attributes to first article
    if raw_attrs and articles:
        articles[0].rawAttributes = raw_attrs

    # ---- Build product number ----
    product_number = meta.get("sku", "")
    # Also try from JSON-LD
    if json_ld and not product_number:
        product_number = json_ld.get("sku", "")

    # ---- Bare tool number (from spec table) ----
    bare_tool_nr = parse_spec_value(specs, "Bestellnummer", "Bare tool", "Artikelnummer")

    # ---- Build product ----
    product = Product(
        source=Source(
            system=SourceSystem.WEB,
            sourceProductId=product_number,
            ingestedAt=now_iso,
        ),
        ids=Ids(
            productNumber=product_number,
            manufacturer="BOSCH",
            bareToolNumber=bare_tool_nr,
            skus=sorted(set(all_skus)) if all_skus else None,
            gtins=sorted(set(all_gtins)) if all_gtins else None,
        ),
        name=Name(
            display=display_name,
            localized={"de": display_name},
        ),
        updatedAt=now_iso,
        lifecycle=Lifecycle(status=LifecycleStatus.ACTIVE),
        classification=Classification(
            productTypeId=subtype,
            categoryIds=category_ids,
            categoryPath=meta.get("category_path", ["Power Tools", "Drilling"]),
            segment="Power Tools",
            family=family,
            platform=platform,
        ),
        articles=articles,
        techSpecs=TechSpecs(
            powerSource=power_source,
            voltageV=voltage,
            rpm=rpm_values if rpm_values else None,
            torqueNm=torque,
            impactRateBpm=impact_rate,
            weightKg=weight,
            dimensionsMm=dimensions,
            noiseDb=noise,
            vibration=vibration_val,
        ),
        compatibility=compat,
        targetUsers=["Professional"],
        features=features if features else None,
        media=Media(
            primaryImage=images[0] if images else None,
            images=images if len(images) > 1 else None,
            documents=documents if documents else None,
        ),
        commercial=Commercial(
            msrp=base_price,
            currency="EUR" if base_price else None,
            availability=avail,
        ),
        ml=ML(
            features={
                "voltage": voltage,
                "torque": torque,
                "weight": weight,
                "rpm_max": max(rpm_values) if rpm_values else None,
                "impact_rate": impact_rate,
                "toolType": subtype.lower(),
                "platform": platform,
                "heavyDuty": heavy_duty,
                "powerSource": power_source.lower(),
                "targetSegment": "professional",
            }
        ),
    )

    return product


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Crawl Bosch Professional DE drill catalog")
    parser.add_argument("--use-cache", action="store_true", help="Use cached HTML pages")
    parser.add_argument("--product-url", type=str, help="Crawl a single product URL (test mode)")
    parser.add_argument("--list-only", action="store_true", help="Only discover and list products, don't extract details")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    console.print("\n[bold cyan]━━━ Bosch Professional Drill Catalog Crawler (DE) ━━━[/bold cyan]\n")

    products: list[Product] = []
    errors: list[dict] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = make_context(browser)
        page = ctx.new_page()
        page.set_default_timeout(PAGE_TIMEOUT_MS)

        if args.product_url:
            # --- Single product test mode ---
            console.print(f"[bold]Test mode:[/bold] {args.product_url}")
            sku_match = re.search(r"(\w{10})$", args.product_url)
            meta = {
                "sku": sku_match.group(1) if sku_match else "",
                "family": _infer_family_from_slug(args.product_url),
                "subtype": "UNKNOWN",
                "category_path": ["Power Tools", "Drilling"],
            }
            meta["subtype"] = FAMILY_SUBTYPE_MAP.get(meta["family"], "UNKNOWN")

            product = extract_product_detail(page, args.product_url, meta, use_cache=args.use_cache)
            if product:
                products.append(product)
                console.print(f"\n[green]Success:[/green] {product.name.display}")
                console.print(f"  Type: {product.classification.productTypeId}")
                console.print(f"  Voltage: {product.techSpecs.voltageV}V")
                console.print(f"  Torque: {product.techSpecs.torqueNm} Nm")
                console.print(f"  Weight: {product.techSpecs.weightKg} kg")
                console.print(f"  Price: €{product.commercial.msrp}")
                console.print(f"  Articles: {len(product.articles)}")
                console.print(f"  SKUs: {product.ids.skus}")
                console.print(f"  GTINs: {product.ids.gtins}")
                console.print(f"  Images: {len(product.media.images or [])}")
                console.print(f"  Docs: {len(product.media.documents or [])}")
                console.print(f"  Specs: {len(product.articles[0].rawAttributes or [])}")
            else:
                console.print("[red]Failed to extract product[/red]")

        else:
            # --- Full crawl ---
            console.print("[bold]Step 1/2:[/bold] Discovering products from category pages...\n")
            discovered = discover_all_products(page, use_cache=args.use_cache)

            console.print(f"\n[green]Discovered {len(discovered)} drill products[/green] (after filtering)\n")

            # Show discovery summary
            subtype_counts: dict[str, int] = {}
            for p in discovered:
                st = p.get("subtype", "UNKNOWN")
                subtype_counts[st] = subtype_counts.get(st, 0) + 1

            summary = Table(title="Discovered Products by Subtype")
            summary.add_column("Subtype", style="cyan")
            summary.add_column("Count", style="green", justify="right")
            for st, count in sorted(subtype_counts.items()):
                summary.add_row(st, str(count))
            summary.add_row("[bold]TOTAL[/bold]", f"[bold]{len(discovered)}[/bold]")
            console.print(summary)

            if args.list_only:
                console.print("\n[bold]Product list:[/bold]")
                for i, p in enumerate(discovered, 1):
                    console.print(f"  {i:3d}. [{p['subtype']:<22s}] {p['name']:<40s} {p['sku']}")
                    console.print(f"       {p['url']}")
                browser.close()
                return

            console.print(f"\n[bold]Step 2/2:[/bold] Extracting product details...\n")

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("Crawling...", total=len(discovered))

                for i, item in enumerate(discovered, 1):
                    short_name = item["name"][:45]
                    progress.update(task, description=f"[{i}/{len(discovered)}] {short_name}")

                    product = extract_product_detail(
                        page, item["url"], item, use_cache=args.use_cache,
                    )
                    if product:
                        products.append(product)
                    else:
                        errors.append({
                            "url": item["url"],
                            "name": item["name"],
                            "sku": item.get("sku", ""),
                            "error": "extraction_failed",
                        })

                    progress.advance(task)

        browser.close()

    # ---- Write output ----
    if products:
        catalog = [json.loads(p.model_dump_json(exclude_none=True)) for p in products]
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_FILE.write_text(json.dumps(catalog, indent=2, ensure_ascii=False), encoding="utf-8")
        console.print(f"\n[green]Catalog written:[/green] {OUTPUT_FILE}")

        # Subcategory breakdown
        sub_counts: dict[str, int] = {}
        for p in products:
            tid = p.classification.productTypeId if p.classification else "UNKNOWN"
            sub_counts[tid] = sub_counts.get(tid, 0) + 1

        meta_data = {
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "totalProducts": len(products),
            "totalErrors": len(errors),
            "subtypeCounts": sub_counts,
            "sources": [c[0] for c in CATEGORY_REGISTRY],
            "errors": errors,
        }
        META_FILE.write_text(json.dumps(meta_data, indent=2, ensure_ascii=False), encoding="utf-8")
        console.print(f"[green]Meta written:[/green]   {META_FILE}")

    # ---- Summary ----
    console.print()
    table = Table(title="Crawl Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    table.add_row("Products extracted", str(len(products)))
    table.add_row("Errors", str(len(errors)))
    table.add_row("Output file", str(OUTPUT_FILE))
    console.print(table)

    if errors:
        console.print("\n[yellow]Failed products:[/yellow]")
        for err in errors:
            console.print(f"  - {err['sku']} {err['name']}: {err['url']}")

    sys.exit(1 if errors and not products else 0)


if __name__ == "__main__":
    main()
