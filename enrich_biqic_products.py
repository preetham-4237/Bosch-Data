#!/usr/bin/env python3
"""
BIQIC Product Enrichment Crawler
=================================
Reads unique product numbers from BIQIC warranty claims, checks against
existing PIM catalog, then crawls Bosch websites to enrich every product
with full schema_v4 data including mandatory ML features.

Data sources (priority):
  1. bosch-professional.com — full product pages (JSON-LD + spec tables)
  2. boschtoolservice.com — spare parts API fallback
  3. Existing product_catalog_v4.json — PIM baseline to merge with

Usage:
    python enrich_biqic_products.py --limit 5          # test with 5 products
    python enrich_biqic_products.py --workers 4         # parallel crawl
    python enrich_biqic_products.py --resume            # resume from checkpoint
    python enrich_biqic_products.py --use-cache         # re-extract from cache
    python enrich_biqic_products.py -v                  # verbose logging
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from playwright.async_api import async_playwright, Browser as AsyncBrowser, BrowserContext as AsyncBrowserContext, Page as AsyncPage
from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn

# ---------------------------------------------------------------------------
# Add crawler module to path for model reuse
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
CRAWLER_DIR = Path(__file__).parent
sys.path.insert(0, str(CRAWLER_DIR))

from models import (
    Article, ArticleFlags, ArticleType, Availability, Classification,
    Commercial, Compatibility, Dimensions, Document, DocumentType,
    Ids, Lifecycle, LifecycleStatus, Media, ML, Name, Product,
    RawAttribute, Source, SourceSystem, TechSpecs,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BIQIC_CSV = PROJECT_ROOT / "data" / "analysis" / "sp_in_matno_check.csv"
PIM_CATALOG = PROJECT_ROOT / "data" / "catalog" / "product_catalog_v4.json"
OUTPUT_FILE = PROJECT_ROOT / "data" / "catalog" / "enriched_catalog_v4.json"
META_FILE = PROJECT_ROOT / "data" / "catalog" / "enriched_catalog_meta.json"
CHECKPOINT_FILE = CRAWLER_DIR / ".enrichment_checkpoint.json"
CACHE_DIR = CRAWLER_DIR / ".cache"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = "https://www.bosch-professional.com"
SPARE_PARTS_API = "https://www.boschtoolservice.com/gb/en/bosch-pt/spareparts/api/search/results"

# Multi-country search: try these in order, stop on first hit.
# English-speaking countries first (easier spec parsing), then DE (biggest catalog).
SEARCH_COUNTRIES = [
    ("gb", "en"),  # UK — English
    ("in", "en"),  # India — English, wide coverage
    ("au", "en"),  # Australia — English
    ("ph", "en"),  # Philippines — English, best coverage in tests
    ("de", "de"),  # Germany — biggest Bosch market
]

REQUEST_DELAY_S = 2.5
PAGE_TIMEOUT_MS = 30_000

console = Console()
log = logging.getLogger("enricher")

# Thread-safe counters
_stats = {
    "total": 0,
    "in_catalog": 0,
    "enriched_web": 0,
    "enriched_web_cached": 0,
    "fallback_api": 0,
    "pim_techdata_direct": 0,
    "pim_techdata_model": 0,
    "no_data": 0,
    "ml_filled": 0,
    "errors": 0,
}

# ---------------------------------------------------------------------------
# Tool type inference from product name/number
# ---------------------------------------------------------------------------
TOOL_TYPE_MAP = {
    # Power Tools — Drilling
    "GSR": "drill_driver", "EXSR": "drill_driver", "GBM": "drill_driver",
    "GSB": "impact_drill", "EXSB": "impact_drill",
    "GBH": "rotary_hammer", "EXBH": "rotary_hammer",
    "GWB": "angle_drill",
    "GTB": "drywall_screwdriver", "GRD": "drywall_screwdriver",
    # Power Tools — Impact
    "GDR": "impact_driver", "GDX": "impact_driver", "GDT": "impact_driver",
    "GDS": "impact_wrench",
    # Power Tools — Grinding / Cutting
    "GWS": "angle_grinder", "EXWS": "angle_grinder",
    "GWX": "angle_grinder",  # X-LOCK grinders
    # Power Tools — Sawing
    "GKS": "circular_saw", "GKT": "circular_saw",
    "GST": "jigsaw",
    "GSA": "reciprocating_saw",
    "GCM": "miter_saw", "GTM": "miter_saw",
    "GTS": "table_saw",
    "GCB": "band_saw",
    # Power Tools — Sanding / Planing
    "GEX": "sander", "GSS": "sander", "GBS": "sander",
    "GHO": "planer",
    # Power Tools — Routing
    "GOF": "router", "GKF": "router",
    # Power Tools — Multi-tool
    "GOP": "multitool",
    # Power Tools — Demolition
    "GSH": "demolition_hammer",
    # Power Tools — Heat / Glue
    "GHG": "heat_gun", "GKP": "glue_gun",
    # Measurement
    "GLM": "laser_measure", "GLL": "laser_level", "GRL": "laser_level",
    "GCL": "laser_level", "GTC": "thermal_camera", "GIS": "thermal_detector",
    "GMS": "detector", "GWM": "measure",
    # Garden
    "AHS": "hedge_trimmer", "ART": "grass_trimmer", "AKE": "chainsaw",
    "GKE": "chainsaw", "AXT": "shredder",
    "ARM": "lawn_mower", "ALB": "blower",
    "AQT": "pressure_washer", "GHP": "pressure_washer",
    "EHP": "pressure_washer", "AFS": "brushcutter",
    # Vacuum / Dust
    "GAS": "vacuum", "GDE": "dust_extraction",
    # Blower
    "GBL": "blower",
    # Battery / Charger
    "GAL": "charger", "GBA": "battery",
    # Misc professional
    "GLL": "laser_level", "GPB": "radio",
    "GLI": "work_light", "GCY": "accessory",
}


# Map spare parts API productGroup to tool type
PRODUCT_GROUP_MAP = {
    "drill driver": "drill_driver", "drill/driver": "drill_driver",
    "impact drill": "impact_drill", "hammer drill": "impact_drill",
    "rotary hammer": "rotary_hammer",
    "angle grinder": "angle_grinder", "grinder": "angle_grinder",
    "circular saw": "circular_saw",
    "jigsaw": "jigsaw", "jig saw": "jigsaw",
    "reciprocating saw": "reciprocating_saw", "sabre saw": "reciprocating_saw",
    "miter saw": "miter_saw", "mitre saw": "miter_saw",
    "table saw": "table_saw",
    "band saw": "band_saw",
    "sander": "sander", "orbital sander": "sander",
    "planer": "planer",
    "router": "router", "palm router": "router",
    "multitool": "multitool", "multi tool": "multitool", "oscillating": "multitool",
    "demolition hammer": "demolition_hammer", "breaker": "demolition_hammer",
    "heat gun": "heat_gun", "hot air gun": "heat_gun",
    "glue gun": "glue_gun",
    "laser measure": "laser_measure", "rangefinder": "laser_measure",
    "laser level": "laser_level", "line laser": "laser_level", "rotation laser": "laser_level",
    "detector": "detector",
    "vacuum": "vacuum", "dust extractor": "vacuum",
    "blower": "blower",
    "chainsaw": "chainsaw", "chain saw": "chainsaw",
    "hedge trimmer": "hedge_trimmer",
    "grass trimmer": "grass_trimmer", "line trimmer": "grass_trimmer",
    "lawn mower": "lawn_mower",
    "pressure washer": "pressure_washer", "high-pressure": "pressure_washer",
    "impact driver": "impact_driver",
    "impact wrench": "impact_wrench",
    "drywall": "drywall_screwdriver",
    "radio": "radio",
    "work light": "work_light", "floodlight": "work_light",
    "charger": "charger",
    "battery": "battery",
    "shredder": "shredder", "chopper": "shredder",
    "scarifier": "scarifier",
    "brushcutter": "brushcutter",
    "shredder": "shredder", "quiet shredder": "shredder",
    "marble": "marble_cutter", "marble cutting": "marble_cutter",
    "nibbler": "nibbler", "shear": "shear",
    "bench grinder": "bench_grinder",
}

# Accessory type classification from product names
ACCESSORY_TYPE_MAP = {
    "drill bit": "drill_bit", "hss drill": "drill_bit", "metal drill": "drill_bit",
    "hammer drill bit": "hammer_drill_bit", "sds": "hammer_drill_bit",
    "saw blade": "saw_blade", "circular saw blade": "saw_blade",
    "jigsaw blade": "jigsaw_blade", "sabre saw blade": "recipro_blade", "reciprocating": "recipro_blade",
    "cutting disc": "cutting_disc", "cut-off": "cutting_disc",
    "grinding disc": "grinding_disc", "grinding wheel": "grinding_disc",
    "flap disc": "flap_disc",
    "diamond": "diamond_disc",
    "sanding": "sanding_sheet", "sandpaper": "sanding_sheet", "sanding pad": "sanding_pad",
    "hole saw": "hole_saw", "holesaw": "hole_saw",
    "chisel": "chisel",
    "screwdriver bit": "screwdriver_bit", "impact bit": "screwdriver_bit",
    "nut driver": "nut_driver", "socket": "nut_driver",
    "brush": "brush", "wire brush": "brush",
    "router bit": "router_bit",
    "dust bag": "dust_bag",
    "l-boxx": "case", "carrying case": "case",
    "charger": "charger", "battery": "battery",
    "guide rail": "guide_rail", "fsn": "guide_rail",
    "adapter": "adapter", "reduction ring": "adapter",
    "staple": "staple", "nail": "nail",
}


def _infer_tool_type_from_group(product_group: str) -> str:
    """Map spare parts API productGroup to tool type."""
    if not product_group:
        return "other"
    pg = product_group.lower()
    for keyword, tool_type in PRODUCT_GROUP_MAP.items():
        if keyword in pg:
            return tool_type
    return "other"


def _infer_accessory_type(name: str) -> str | None:
    """Classify accessories from product name."""
    if not name:
        return None
    nl = name.lower()
    for keyword, acc_type in ACCESSORY_TYPE_MAP.items():
        if keyword in nl:
            return acc_type
    return None


def infer_tool_type(name: str, product_number: str) -> str | None:
    """Infer tool type from product name or description."""
    name_upper = name.upper().strip()
    # Try matching known Bosch family prefixes in the name
    for prefix, tool_type in sorted(TOOL_TYPE_MAP.items(), key=lambda x: -len(x[0])):
        # Match prefix at start of name or after space
        if re.search(rf'\b{prefix}\b', name_upper):
            return tool_type
    # Try the mat_no description from BIQIC (column B)
    for prefix, tool_type in sorted(TOOL_TYPE_MAP.items(), key=lambda x: -len(x[0])):
        if name_upper.startswith(prefix):
            return tool_type
    # For accessories (26xx) and spare parts (16xx), try accessory classification
    if product_number[:2] in ("26", "16", "25"):
        acc_type = _infer_accessory_type(name)
        if acc_type:
            return acc_type
    return "other"


def infer_platform(voltage: float | None, name: str) -> str | None:
    """Infer battery platform from voltage and name."""
    if voltage and voltage <= 50:
        return f"{int(voltage)}V"
    if re.search(r'(\d+)\s*V', name):
        v = int(re.search(r'(\d+)\s*V', name).group(1))
        if v <= 50:
            return f"{v}V"
    if voltage and voltage > 50:
        return "corded"
    return None


# ---------------------------------------------------------------------------
# techData German key → techSpecs parsing (from gsp_to_catalog.py)
# ---------------------------------------------------------------------------
_BATTERY_VOLTAGE_KEYS = ["AKKUSPANNUNG", "AKKU_SPANNUNG", "NENNSPANNUNG",
                         "KOMPATIBLE_AKKU_SPANNUNG", "BATTERY_VOLTAGE"]
_MAINS_VOLTAGE_KEYS = ["SPANNUNG"]
_VOLTAGE_KEYS = _BATTERY_VOLTAGE_KEYS + _MAINS_VOLTAGE_KEYS

_WEIGHT_KEYS = ["GEWICHT", "MASCHINENGEWICHT", "GEWICHT_OHNE_AKKU",
                "GEWICHT_CA", "GEWICHT_NACH_EPTA", "GEWICHT_N_EPTA",
                "MSCHINENGEWICHT", "MASCHINENGEWICHT_OHNE_BATTERIE"]

_RPM_KEYS = ["LEERLAUFDREHZAHL", "LEERLAUFDREHZAHL_1_GANG_2_GANG",
             "LEERLAUFDREHZAHL_1_GANG", "LEERLAUFDREHZAHL_BIS",
             "REVOLUTION_RATE_NO_LOAD", "NENNDREHZAHL"]

_TORQUE_KEYS = ["DREHMOMENT_MAX", "MAX_DREHMOMENT", "NENNDREHMOMENT",
                "DREHMOMENT_MAX_HARTER_SCHRAUBFALL", "KURZSCHLUSSDREHMOMENT",
                "LOSBRECHMOMENT_MAX", "MAX_DREHMOMENT_WEICHER_HARTER_SCHRAUBFALL",
                "DREHMOMENT_WEICH_HART_MAX"]

_BATTERY_AH_KEYS = ["AKKUKAPAZITAET", "AKKU_KAPAZITAET", "BATTERY_CAPACITY"]

_IMPACT_KEYS = ["SCHLAGZAHL", "SCHLAGZAHL_BEI_NENNDREHZAHL",
                "NENNSCHLAGZAHL", "HUBZAHL_BEI_LEERLAUF",
                "SCHLAGZAHL_BEI_LEERLAUFDREHZAHL", "SCHLAGZAHL_MAX"]

_INPUT_POWER_KEYS = ["NENNAUFNAHMELEISTUNG", "NENNAUFNAHME",
                     "NENNLEISTUNG_AUFNAHME", "MAX_AUFNAHMELEISTUNG"]

_OUTPUT_POWER_KEYS = ["ABGABELEISTUNG", "NENNLEISTUNG_ABGABE",
                      "ABGABELEISTUNG_MAX", "MOTORLEISTUNG"]


def _td_first_value(tech_data: dict, keys: list[str]) -> str | None:
    """Return the .value from the first matching techData key that has data."""
    for k in keys:
        if k in tech_data:
            v = tech_data[k].get("value", "").strip()
            if v:
                return v
    return None


def _td_parse_number(raw: str) -> float | None:
    """Parse a single number from German-formatted techData value.
    Handles: '5.000' (=5000), '1,6' (=1.6), ranges '0 – 420' (takes max).
    """
    if not raw or not raw.strip():
        return None
    # For ranges like "0 – 2.800", split and take max
    parts = re.split(r'\s*[–\-]\s*', raw.strip())
    nums = []
    for part in parts:
        m = re.search(r'[\d]+[.,\d]*', part)
        if not m:
            continue
        token = m.group()
        # European thousand separator: "5.000" → 5000
        if re.match(r'^\d{1,3}(\.\d{3})+$', token):
            nums.append(float(token.replace('.', '')))
        else:
            if ',' in token and '.' not in token:
                token = token.replace(',', '.')
            try:
                nums.append(float(token))
            except ValueError:
                pass
    return max(nums) if nums else None


def _td_parse_rpm_list(raw: str) -> list[float]:
    """Parse RPM values — may be single, range, or multi-gear separated by '/'."""
    if not raw:
        return []
    segments = raw.split('/')
    results = []
    for seg in segments:
        val = _td_parse_number(seg)
        if val is not None and val > 0:
            results.append(val)
    return results


def parse_pim_tech_data(tech_data: dict) -> dict:
    """Parse a PIM techData dict (German keys) into a structured specs dict.

    Returns dict with keys: voltageV, weightKg, torqueNm, rpm, impactRateBpm,
    batteryAh, inputPowerW, outputPowerW, powerSource, dimensionsMm.
    Only non-None values are included.
    """
    result = {}

    # Voltage
    v_str = _td_first_value(tech_data, _VOLTAGE_KEYS)
    if v_str:
        v = _td_parse_number(v_str)
        if v is not None:
            result["voltageV"] = v

    # Weight
    w_str = _td_first_value(tech_data, _WEIGHT_KEYS)
    if w_str:
        w = _td_parse_number(w_str)
        if w is not None:
            result["weightKg"] = w

    # RPM
    rpm_str = _td_first_value(tech_data, _RPM_KEYS)
    if rpm_str:
        rpms = _td_parse_rpm_list(rpm_str)
        if rpms:
            result["rpm"] = rpms

    # Torque
    t_str = _td_first_value(tech_data, _TORQUE_KEYS)
    if t_str:
        t = _td_parse_number(t_str)
        if t is not None:
            result["torqueNm"] = t

    # Battery Ah
    ah_str = _td_first_value(tech_data, _BATTERY_AH_KEYS)
    if ah_str:
        ah = _td_parse_number(ah_str)
        if ah is not None:
            result["batteryAh"] = ah

    # Impact rate
    imp_str = _td_first_value(tech_data, _IMPACT_KEYS)
    if imp_str:
        imp = _td_parse_number(imp_str)
        if imp is not None:
            result["impactRateBpm"] = imp

    # Input power
    ip_str = _td_first_value(tech_data, _INPUT_POWER_KEYS)
    if ip_str:
        ip = _td_parse_number(ip_str)
        if ip is not None:
            result["inputPowerW"] = ip

    # Output power
    op_str = _td_first_value(tech_data, _OUTPUT_POWER_KEYS)
    if op_str:
        op = _td_parse_number(op_str)
        if op is not None:
            result["outputPowerW"] = op

    # Power source inference
    has_batt = any(k in tech_data for k in _BATTERY_VOLTAGE_KEYS + _BATTERY_AH_KEYS)
    has_mains = any(k in tech_data for k in _MAINS_VOLTAGE_KEYS + _INPUT_POWER_KEYS)
    if has_batt:
        result["powerSource"] = "Cordless"
    elif has_mains:
        result["powerSource"] = "Corded"

    # Dimensions
    dims = {}
    for key, dim in [("LAENGE", "length"), ("BREITE", "width"), ("HOEHE", "height")]:
        if key in tech_data:
            v = _td_parse_number(tech_data[key].get("value", ""))
            if v is not None:
                dims[dim] = v
    if dims:
        result["dimensionsMm"] = dims

    return result


def infer_price_range(msrp: float | None) -> str | None:
    """Bucket price into ranges."""
    if msrp is None:
        return None
    if msrp < 100:
        return "budget"
    if msrp < 300:
        return "mid"
    if msrp < 600:
        return "premium"
    return "ultra"


# ---------------------------------------------------------------------------
# Cache helpers
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
# Browser helpers
# ---------------------------------------------------------------------------
def make_context(browser: Browser) -> BrowserContext:
    return browser.new_context(
        locale="en-GB",
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
    )


def accept_cookies(page: Page) -> None:
    try:
        for sel in [
            "#onetrust-accept-btn-handler",
            "button:has-text('Accept All')",
            "button:has-text('Alle akzeptieren')",
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
    if use_cache:
        cached = cache_load(url)
        if cached:
            return cached
    page.goto(url, wait_until="networkidle", timeout=PAGE_TIMEOUT_MS)
    accept_cookies(page)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
    page.wait_for_timeout(1500)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)
    html = page.content()
    cache_save(url, html)
    time.sleep(REQUEST_DELAY_S)
    return html


# ---------------------------------------------------------------------------
# Extraction functions (reused from crawl_drills_de.py)
# ---------------------------------------------------------------------------
def _safe_float(val: str | None) -> float | None:
    if not val:
        return None
    cleaned = re.sub(r"[^\d.,\-]", "", val.strip())
    cleaned = cleaned.replace(",", ".")
    parts = cleaned.split(".")
    if len(parts) > 2:
        cleaned = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def extract_json_ld(html: str) -> list[dict]:
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
    for item in extract_json_ld(html):
        if isinstance(item, dict) and item.get("@type") in ("Product", "ProductGroup"):
            return item
    return None


def extract_variants(json_ld: dict) -> list[dict]:
    variants = json_ld.get("hasVariant", [])
    results = []
    for v in variants:
        if not isinstance(v, dict):
            continue
        entry: dict[str, Any] = {
            "sku": v.get("sku", ""), "gtin13": v.get("gtin13", ""),
            "name": v.get("name", ""), "description": v.get("description", ""),
            "price": None, "currency": None, "availability": "",
        }
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
    if not results:
        offers = json_ld.get("offers")
        if isinstance(offers, dict):
            offers = [offers]
        if isinstance(offers, list):
            for o in offers:
                if isinstance(o, dict):
                    results.append({
                        "sku": o.get("sku", json_ld.get("sku", "")),
                        "gtin13": o.get("gtin13", ""), "name": o.get("name", ""),
                        "description": "",
                        "price": _safe_float(str(o.get("price", ""))),
                        "currency": o.get("priceCurrency", "EUR"),
                        "availability": o.get("availability", ""),
                    })
    return results


def extract_spec_pairs(html: str) -> dict[str, str]:
    specs: dict[str, str] = {}
    for m in re.finditer(
        r'<div\s+class="table__body-row">\s*'
        r'<div\s+class="table__body-cell">\s*<span>(.*?)</span>\s*</div>\s*'
        r'<div\s+class="table__body-cell">\s*<span>(.*?)</span>',
        html, re.DOTALL,
    ):
        key = re.sub(r"<[^>]+>", "", m.group(1)).strip()
        val = re.sub(r"<[^>]+>", "", m.group(2)).strip()
        if not val and "icon-checkmark" in m.group(2):
            val = "Yes"
        if key and val and len(key) < 120:
            specs[key] = val
    if not specs:
        for m in re.finditer(r"<t[hd][^>]*>\s*(.*?)\s*</t[hd]>\s*<td[^>]*>\s*(.*?)\s*</td>", html, re.DOTALL):
            key = re.sub(r"<[^>]+>", "", m.group(1)).strip()
            val = re.sub(r"<[^>]+>", "", m.group(2)).strip()
            if key and val and len(key) < 100:
                specs[key] = val
    return specs


def parse_spec_value(specs: dict[str, str], *labels: str) -> str | None:
    for label in labels:
        for key, val in specs.items():
            if label.lower() in key.lower():
                return val
    return None


# ---------------------------------------------------------------------------
# Search bosch-professional.com — multi-country, by model name
# ---------------------------------------------------------------------------
def search_bosch_professional(
    page: Page, product_number: str, *, use_cache: bool = False,
    model_name: str | None = None,
) -> str | None:
    """Search bosch-professional.com across multiple countries using model name.

    Tries model name first (higher hit rate), falls back to product number.
    Iterates through SEARCH_COUNTRIES until a product page is found.
    """
    search_terms = []
    if model_name and model_name != product_number:
        search_terms.append(model_name)
    search_terms.append(product_number)

    for country, lang in SEARCH_COUNTRIES:
        search_url_base = f"{BASE_URL}/{country}/{lang}/searchfrontend/?q="
        for term in search_terms:
            search_url = f"{search_url_base}{term}"
            try:
                html = fetch_page(page, search_url, use_cache=use_cache)
            except Exception as e:
                log.debug("Search failed for %s on %s/%s: %s", term, country, lang, e)
                continue

            # Look for product links in search results
            matches = re.findall(
                rf'href="(https://www\.bosch-professional\.com/{country}/{lang}/products/[a-zA-Z0-9\-]+-\w{{10}})[^"]*"',
                html,
            )
            if not matches:
                matches = re.findall(
                    r'href="(https://www\.bosch-professional\.com/\w+/\w+/products/[a-zA-Z0-9\-]+-\w{10})[^"]*"',
                    html,
                )

            if matches:
                log.info("Found product page for %s via %s/%s search '%s'", product_number, country, lang, term)
                return matches[0]

    return None


# ---------------------------------------------------------------------------
# Spare parts API fallback
# ---------------------------------------------------------------------------
def query_spare_parts_api(product_number: str) -> dict | None:
    """Query boschtoolservice.com spare parts API."""
    try:
        resp = requests.get(
            SPARE_PARTS_API,
            params={"q": product_number},
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
        )
        if resp.status_code == 200:
            data = resp.json()
            return data
    except Exception as e:
        log.debug("Spare parts API failed for %s: %s", product_number, e)
    return None


def build_product_from_spare_api(product_number: str, api_data: dict, baseline: dict | None) -> dict | None:
    """Build a rich product dict from spare parts API response.

    The API returns structured data for 36xx/06xx products:
      tradeName, productDescription, productGroup, applicationScope,
      businessSegment, voltage, localizedValues, url
    """
    inner = api_data.get("data", api_data)
    products_list = []
    spares_list = []
    if isinstance(inner, dict):
        products_list = inner.get("products", [])
        spares_list = inner.get("spareparts", [])
        if not products_list and not spares_list:
            products_list = inner.get("results", [])
    elif isinstance(inner, list):
        products_list = inner

    # Try products first (better data for 36xx / 06xx)
    matched = None
    for p in (products_list or []):
        if not isinstance(p, dict):
            continue
        pn = str(p.get("productNumber", p.get("number", "")))
        if product_number in pn or pn in product_number:
            matched = p
            break
    if not matched and products_list and isinstance(products_list[0], dict):
        matched = products_list[0]  # best guess

    if matched:
        lv = matched.get("localizedValues", {}).get("en", {})
        trade_name = matched.get("tradeName", "") or matched.get("type", "")
        description = lv.get("productDescription", "")
        product_group = lv.get("productGroup", "")
        application = lv.get("applicationScope", "")
        area = lv.get("area", "")
        segment = lv.get("businessSegment", "")
        power_source_hint = lv.get("powerSource", "")
        voltage_str = matched.get("voltage1", "")
        voltage = _safe_float(voltage_str) if voltage_str else None

        if not trade_name and baseline:
            trade_name = baseline.get("name", {}).get("display", "")

        if not trade_name and not description:
            return None

        return {
            "name": trade_name,
            "description": description,
            "category": product_group,
            "application": application,
            "area": area,
            "segment": segment,
            "power_source_hint": power_source_hint,
            "voltage": voltage,
            "source": "spare_parts_api",
        }

    # Fallback: try spareparts results
    for s in (spares_list or []):
        if not isinstance(s, dict):
            continue
        sname = s.get("name", s.get("title", s.get("description", "")))
        if sname:
            return {
                "name": sname,
                "description": "",
                "category": s.get("category", s.get("productGroup", "")),
                "application": "",
                "area": "",
                "segment": "",
                "power_source_hint": "",
                "voltage": None,
                "source": "spare_parts_api",
            }

    # Last resort: use baseline name
    if baseline:
        bname = baseline.get("name", {}).get("display", "")
        if bname:
            return {
                "name": bname,
                "description": "",
                "category": "",
                "application": "",
                "area": "",
                "segment": "",
                "power_source_hint": "",
                "voltage": None,
                "source": "spare_parts_api",
            }

    return None


# ---------------------------------------------------------------------------
# Extract full product from bosch-professional.com detail page
# ---------------------------------------------------------------------------
def extract_from_product_page(
    page: Page, url: str, product_number: str, *, use_cache: bool = False,
) -> dict | None:
    """Extract full product data from a bosch-professional.com product page."""
    try:
        html = fetch_page(page, url, use_cache=use_cache)
    except Exception as e:
        log.error("Failed to fetch %s: %s", url, e)
        return None
    return _extract_product_data_from_html(html, url, product_number)


def _extract_product_data_from_html(html: str, url: str, product_number: str) -> dict | None:
    """Pure HTML parsing — no browser needed. Used by both sync and async paths."""
    json_ld = extract_product_json_ld(html)
    if not json_ld:
        log.debug("No JSON-LD for %s", url)

    # Product name
    display_name = ""
    if json_ld:
        display_name = json_ld.get("name", "")
    if not display_name:
        m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.DOTALL)
        if m:
            display_name = re.sub(r"<[^>]+>", "", m.group(1)).strip()
    if not display_name:
        return None

    description = json_ld.get("description", "") if json_ld else ""

    # Variants
    variants = extract_variants(json_ld) if json_ld else []

    # Specs
    specs = extract_spec_pairs(html)

    # Articles
    articles = []
    all_skus = []
    all_gtins = []
    base_price = None

    for variant in variants:
        sku = variant.get("sku", "")
        gtin = variant.get("gtin13", "")
        price = variant.get("price")
        var_name = variant.get("name", "")
        var_desc = variant.get("description", "")

        if sku:
            all_skus.append(sku)
        if gtin:
            all_gtins.append(gtin)
        if price and (base_price is None or price < base_price):
            base_price = price

        name_lower = (var_name + " " + var_desc).lower()
        has_battery = any(kw in name_lower for kw in ["akku", "battery", "li-ion", "procore", "charger"])

        articles.append({
            "articleType": "KIT" if has_battery else "BARE_TOOL",
            "sku": sku or None,
            "gtin": gtin or None,
            "skuDescription": var_name or None,
        })

    if not articles:
        articles.append({"articleType": "UNKNOWN", "sku": product_number})
        all_skus.append(product_number)

    # Tech specs parsing
    voltage_str = parse_spec_value(specs, "Battery voltage", "Akkuspannung", "Nennspannung", "Voltage", "Rated voltage")
    voltage = _safe_float(voltage_str)

    torque_str = parse_spec_value(specs, "Torque", "Drehmoment", "Max. torque", "Max. Drehmoment")
    torque = None
    if torque_str:
        nums = [_safe_float(x) for x in re.findall(r"[\d.,]+", torque_str)]
        nums = [n for n in nums if n is not None and n > 0]
        torque = max(nums) if nums else None

    rpm_str = parse_spec_value(specs, "No-load speed", "Leerlaufdrehzahl", "Speed", "Drehzahl")
    rpm_values = []
    if rpm_str:
        clean_rpm = re.sub(r"(\d)\.(\d{3})", r"\1\2", rpm_str)
        clean_rpm = re.sub(r"(\d),(\d{3})", r"\1\2", clean_rpm)
        nums = [_safe_float(x) for x in re.findall(r"[\d]+", clean_rpm)]
        rpm_values = sorted(set(n for n in nums if n is not None and n > 0))

    weight_str = parse_spec_value(specs, "Weight", "Gewicht", "Tool weight")
    weight = _safe_float(weight_str)
    # Fix weight unit: values > 100 are likely in grams, convert to kg
    if weight is not None and weight > 100:
        weight = weight / 1000.0

    noise_str = parse_spec_value(specs, "Sound pressure", "Schalldruckpegel", "Noise")
    noise = _safe_float(noise_str)

    vibration_str = parse_spec_value(specs, "Vibration", "Schwingung")
    vibration_val = _safe_float(vibration_str)

    impact_str = parse_spec_value(specs, "Impact rate", "Schlagzahl", "Blow rate")
    impact_rate = _safe_float(impact_str)

    battery_ah_str = parse_spec_value(specs, "Battery capacity", "Akkukapazität", "Capacity")
    battery_ah = _safe_float(battery_ah_str)

    input_power_str = parse_spec_value(specs, "Rated input power", "Nennaufnahmeleistung", "Input power")
    input_power = _safe_float(input_power_str)

    output_power_str = parse_spec_value(specs, "Output power", "Abgabeleistung", "Rated output power")
    output_power = _safe_float(output_power_str)

    dim_str = parse_spec_value(specs, "Dimension", "Abmessung", "Size", "Length x width x height")
    dimensions = None
    if dim_str:
        dim_nums = re.findall(r"[\d.,]+", dim_str.replace(",", "."))
        if len(dim_nums) >= 3:
            dimensions = {"length": _safe_float(dim_nums[0]), "width": _safe_float(dim_nums[1]), "height": _safe_float(dim_nums[2])}

    # Power source
    is_cordless = bool(re.search(r"\d+\s*V|akku|cordless|battery", display_name, re.IGNORECASE))
    power_source = "Cordless" if is_cordless else "Corded"

    # Platform
    platform = infer_platform(voltage, display_name)

    # Heavy duty
    heavy_duty = "HEAVY DUTY" in display_name.upper() or "heavy duty" in html.lower()[:5000]

    # Images
    images = []
    if json_ld:
        img = json_ld.get("image")
        if isinstance(img, str):
            images.append(img)
        elif isinstance(img, list):
            images.extend([i for i in img if isinstance(i, str)])
    for m in re.finditer(r'(https://www\.bosch-professional\.com/[^"]*?/(?:application-image|product-image)/[^"]+\.(?:png|jpg|jpeg))', html):
        url_img = m.group(1)
        if url_img not in images:
            images.append(url_img)

    # Documents
    documents = []
    for m in re.finditer(r'href="([^"]*\.pdf[^"]*)"', html):
        href = m.group(1)
        full_href = href if href.startswith("http") else f"{BASE_URL}{href}"
        href_lower = full_href.lower()
        if "manual" in href_lower or "anleitung" in href_lower:
            doc_type = "MANUAL"
        elif "datasheet" in href_lower or "datenblatt" in href_lower:
            doc_type = "DATASHEET"
        elif "explosion" in href_lower or "ersatzteil" in href_lower:
            doc_type = "EXPLODED_VIEW"
        else:
            doc_type = "OTHER"
        if full_href not in [d["ref"] for d in documents]:
            documents.append({"type": doc_type, "ref": full_href})

    # Features from HTML
    features = []
    for pattern in [
        r'class="[^"]*(?:highlight|feature|benefit|usp)[^"]*"[^>]*>\s*<[^>]*>\s*(.*?)\s*<',
        r'<li[^>]*class="[^"]*(?:highlight|feature|benefit)[^"]*"[^>]*>(.*?)</li>',
    ]:
        for m in re.finditer(pattern, html, re.DOTALL | re.IGNORECASE):
            text = re.sub(r"<[^>]+>", "", m.group(1)).strip()
            if text and len(text) > 5 and text not in features:
                features.append(text)

    # Availability
    avail = "UNKNOWN"
    if any("InStock" in str(v.get("availability", "")) for v in variants):
        avail = "IN_STOCK"
    elif any("OutOfStock" in str(v.get("availability", "")) for v in variants):
        avail = "OUT_OF_STOCK"

    # Compatibility
    compatibility = None
    if is_cordless and platform:
        sys_compat = [f"Bosch Professional {platform}"]
        if platform == "18V":
            sys_compat.append("AMPShare")
        compatibility = {
            "batteryPlatform": f"Bosch Professional {platform} System",
            "systemCompatibility": sys_compat,
        }

    # Classification
    tool_type = infer_tool_type(display_name, product_number)
    family = ""
    fm = re.match(r'([A-Z]{2,4})', display_name.split()[0] if display_name else "")
    if fm:
        family = fm.group(1)

    # Boolean feature flags from specs + HTML
    html_lower = html.lower()[:10000]
    name_lower = display_name.lower()

    brushless = "brushless" in name_lower or "ec motor" in name_lower or "-ec " in name_lower
    variable_speed = bool(parse_spec_value(specs, "Speed preselection", "Drehzahlvorwahl", "Variable speed"))
    kickback = "kickback" in html_lower or "anti-rotation" in html_lower or "rückschlag" in html_lower
    e_clutch = "electronic clutch" in html_lower or "elektronische kupplung" in html_lower
    dust_extraction = "dust" in name_lower or "absaug" in name_lower or bool(parse_spec_value(specs, "Dust extraction", "Absaug"))
    led_light = "led" in html_lower[:3000] and ("light" in html_lower[:3000] or "licht" in html_lower[:3000])
    bluetooth = "bluetooth" in name_lower or "connected" in name_lower or "connectivity" in html_lower[:5000]

    # ML features (MANDATORY)
    ml_features = {
        "voltage": voltage,
        "torque": torque,
        "weight": weight,
        "rpmMax": max(rpm_values) if rpm_values else None,
        "impactRate": impact_rate,
        "batteryAh": battery_ah,
        "inputPowerW": input_power,
        "outputPowerW": output_power,
        "toolType": tool_type,
        "platform": platform,
        "powerSource": power_source.lower(),
        "targetSegment": "professional",
        "motorType": "brushless" if brushless else "brushed" if voltage and voltage < 50 else None,
        "performanceTier": None,
        "priceRange": infer_price_range(base_price),
        "heavyDuty": heavy_duty,
        "bluetooth": bluetooth,
        "brushless": brushless,
        "variableSpeed": variable_speed,
        "kickbackProtection": kickback,
        "electronicClutch": e_clutch,
        "dustExtraction": dust_extraction,
        "ledLight": led_light,
    }

    # Raw spec attributes
    raw_attrs = [{"key": k, "textValue": v} for k, v in specs.items()]

    return {
        "display_name": display_name,
        "description": description,
        "articles": articles,
        "all_skus": all_skus,
        "all_gtins": all_gtins,
        "voltage": voltage,
        "torque": torque,
        "rpm_values": rpm_values,
        "weight": weight,
        "noise": noise,
        "vibration": vibration_val,
        "impact_rate": impact_rate,
        "battery_ah": battery_ah,
        "input_power": input_power,
        "output_power": output_power,
        "dimensions": dimensions,
        "power_source": power_source,
        "platform": platform,
        "heavy_duty": heavy_duty,
        "images": images,
        "documents": documents,
        "features": features,
        "base_price": base_price,
        "avail": avail,
        "compatibility": compatibility,
        "tool_type": tool_type,
        "family": family,
        "ml_features": ml_features,
        "raw_attrs": raw_attrs,
        "source_url": url,
    }


# ---------------------------------------------------------------------------
# Build schema_v4 product dict
# ---------------------------------------------------------------------------
def build_product_dict(
    product_number: str,
    baseline: dict | None,
    web_data: dict | None,
    spare_data: dict | None,
    pim_tech_data: dict | None = None,
) -> dict:
    """Assemble a schema_v4-conformant product dict from all sources.

    Args:
        pim_tech_data: Pre-parsed techData from PIM (either direct match or
                       model-name sibling). Dict with keys like voltageV, weightKg, etc.
    """
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Start with baseline from PIM catalog if available
    display_name = ""
    localized = {}
    if baseline:
        display_name = baseline.get("name", {}).get("display", "")
        localized = baseline.get("name", {}).get("localized", {})

    # Web data overrides
    source_system = "PIM"
    if web_data:
        source_system = "WEB_CRAWL"
        display_name = web_data["display_name"] or display_name
        if web_data["display_name"]:
            localized["en"] = web_data["display_name"]
    elif spare_data and spare_data.get("name"):
        source_system = "SPARE_API"
        display_name = spare_data["name"] or display_name
        if spare_data.get("description"):
            localized["en_description"] = spare_data["description"]

    if not display_name:
        display_name = product_number  # last resort

    # ── Tech specs — layer sources: web > PIM techData > spare API > baseline ──
    tech_specs = {}

    # Layer 1: PIM techData (parsed from German keys) — mechanical specs base
    if pim_tech_data:
        tech_specs.update(pim_tech_data)

    # Layer 2: Baseline techSpecs from existing catalog
    if baseline and baseline.get("techSpecs"):
        for k, v in baseline["techSpecs"].items():
            if k not in tech_specs:
                tech_specs[k] = v

    # Layer 3: Parse techData from baseline raw (if not already parsed via pim_tech_data)
    if not pim_tech_data and baseline:
        raw_td = baseline.get("source", {}).get("raw", {}).get("techData", {})
        if raw_td:
            parsed = parse_pim_tech_data(raw_td)
            for k, v in parsed.items():
                if k not in tech_specs:
                    tech_specs[k] = v

    # Layer 4: Spare API data (voltage + power source)
    if spare_data:
        if spare_data.get("voltage") is not None and "voltageV" not in tech_specs:
            tech_specs["voltageV"] = spare_data["voltage"]
        # Power source inference from spare API hints
        if "powerSource" not in tech_specs:
            ps_hint = spare_data.get("power_source_hint", "")
            area = spare_data.get("area", "")
            if "cordless" in area.lower() or "cordless" in ps_hint.lower():
                tech_specs["powerSource"] = "Cordless"
            elif spare_data.get("voltage") and spare_data["voltage"] > 50:
                tech_specs["powerSource"] = "Corded"
            elif spare_data.get("voltage") and spare_data["voltage"] <= 50:
                tech_specs["powerSource"] = "Cordless"

    # Layer 5: Web data overrides everything (richest source)
    if web_data:
        if web_data["power_source"]:
            tech_specs["powerSource"] = web_data["power_source"]
        if web_data["voltage"] is not None:
            tech_specs["voltageV"] = web_data["voltage"]
        if web_data["battery_ah"] is not None:
            tech_specs["batteryAh"] = web_data["battery_ah"]
        if web_data["rpm_values"]:
            tech_specs["rpm"] = web_data["rpm_values"]
        if web_data["torque"] is not None:
            tech_specs["torqueNm"] = web_data["torque"]
        if web_data["impact_rate"] is not None:
            tech_specs["impactRateBpm"] = web_data["impact_rate"]
        if web_data["weight"] is not None:
            tech_specs["weightKg"] = web_data["weight"]
        if web_data["dimensions"]:
            tech_specs["dimensionsMm"] = web_data["dimensions"]
        if web_data["noise"] is not None:
            tech_specs["noiseDb"] = web_data["noise"]
        if web_data["vibration"] is not None:
            tech_specs["vibration"] = web_data["vibration"]
        if web_data.get("input_power") is not None:
            tech_specs["inputPowerW"] = web_data["input_power"]
        if web_data.get("output_power") is not None:
            tech_specs["outputPowerW"] = web_data["output_power"]

    # Articles
    articles = [{"articleType": "UNKNOWN"}]
    if web_data and web_data["articles"]:
        articles = web_data["articles"]
    elif baseline and baseline.get("articles"):
        articles = baseline["articles"]

    # ── ML features (MANDATORY) ──
    ml_features = None
    if web_data and web_data.get("ml_features"):
        ml_features = web_data["ml_features"]
        # Backfill from PIM techData for any nulls in web-scraped features
        if pim_tech_data:
            if ml_features.get("weight") is None and "weightKg" in tech_specs:
                ml_features["weight"] = tech_specs["weightKg"]
            if ml_features.get("torque") is None and "torqueNm" in tech_specs:
                ml_features["torque"] = tech_specs["torqueNm"]
            if ml_features.get("rpmMax") is None and "rpm" in tech_specs and tech_specs["rpm"]:
                ml_features["rpmMax"] = max(tech_specs["rpm"])
            if ml_features.get("inputPowerW") is None and "inputPowerW" in tech_specs:
                ml_features["inputPowerW"] = tech_specs["inputPowerW"]
            if ml_features.get("outputPowerW") is None and "outputPowerW" in tech_specs:
                ml_features["outputPowerW"] = tech_specs["outputPowerW"]
    else:
        # Build from whatever data we have (techSpecs now includes parsed PIM techData)
        voltage = tech_specs.get("voltageV")
        rpm = tech_specs.get("rpm", [])
        tool_type = infer_tool_type(display_name, product_number)
        # Use spare API category for better tool type when name inference fails
        if tool_type == "other" and spare_data and spare_data.get("category"):
            tool_type = _infer_tool_type_from_group(spare_data["category"])
        # Use spare API description for tool type as last resort
        if tool_type == "other" and spare_data and spare_data.get("description"):
            tool_type = infer_tool_type(spare_data["description"], product_number)
        platform = infer_platform(voltage, display_name)
        ps = tech_specs.get("powerSource", "").lower() or None
        name_lower = display_name.lower()
        brushless = "brushless" in name_lower or "-ec " in name_lower or name_lower.endswith("-ec")

        # Motor type: check for EC suffix in model name (e.g. GSR 18V-60 EC)
        motor_type = None
        if brushless:
            motor_type = "brushless"
        elif voltage and voltage <= 50:
            motor_type = "brushed"  # default for cordless non-brushless

        # Target segment from spare API or prefix
        target_segment = None
        if spare_data and spare_data.get("segment"):
            seg = spare_data["segment"].lower()
            if "blue" in seg or "professional" in seg:
                target_segment = "professional"
            elif "green" in seg or "diy" in seg or "home" in seg:
                target_segment = "diy"
            elif "garden" in seg or "lawn" in seg:
                target_segment = "garden"
        if not target_segment:
            p4 = product_number[:4]
            if p4 in ("3601", "0601", "3611", "0611"):
                target_segment = "professional"
            elif p4 in ("3603", "0603"):
                target_segment = "diy"
            elif p4 in ("3600", "0600"):
                target_segment = "garden"

        ml_features = {
            "voltage": voltage,
            "torque": tech_specs.get("torqueNm"),
            "weight": tech_specs.get("weightKg"),
            "rpmMax": max(rpm) if rpm else None,
            "impactRate": tech_specs.get("impactRateBpm"),
            "batteryAh": tech_specs.get("batteryAh"),
            "inputPowerW": tech_specs.get("inputPowerW"),
            "outputPowerW": tech_specs.get("outputPowerW"),
            "toolType": tool_type,
            "platform": platform,
            "powerSource": ps,
            "targetSegment": target_segment,
            "motorType": motor_type,
            "performanceTier": None,
            "priceRange": None,
            "heavyDuty": "heavy duty" in name_lower,
            "bluetooth": "bluetooth" in name_lower or "connected" in name_lower,
            "brushless": brushless,
            "variableSpeed": False,
            "kickbackProtection": False,
            "electronicClutch": False,
            "dustExtraction": "dust" in name_lower or "absaug" in name_lower,
            "ledLight": False,
        }

    # Assemble product
    product = {
        "source": {
            "system": source_system,
            "sourceProductId": product_number,
            "ingestedAt": now_iso,
        },
        "ids": {
            "productNumber": product_number,
            "manufacturer": "BOSCH",
        },
        "name": {
            "display": display_name,
        },
        "updatedAt": now_iso,
        "articles": articles,
    }

    if localized:
        product["name"]["localized"] = localized

    if tech_specs:
        product["techSpecs"] = tech_specs

    # Media — disabled for now (not needed for recommendation engine)
    # if web_data and (web_data["images"] or web_data["documents"]):
    #     media = {}
    #     if web_data["images"]:
    #         media["primaryImage"] = web_data["images"][0]
    #         if len(web_data["images"]) > 1:
    #             media["images"] = web_data["images"]
    #     if web_data["documents"]:
    #         media["documents"] = web_data["documents"]
    #     product["media"] = media

    # Commercial
    if web_data and web_data["base_price"]:
        product["commercial"] = {
            "msrp": web_data["base_price"],
            "currency": "EUR",
            "availability": web_data["avail"],
        }

    # Classification
    if web_data:
        product["classification"] = {
            "productTypeId": web_data.get("tool_type", "other"),
            "segment": "Power Tools",
            "family": web_data.get("family", ""),
            "platform": web_data.get("platform"),
        }
    elif spare_data:
        product["classification"] = {
            "productTypeId": ml_features.get("toolType", "other"),
            "segment": spare_data.get("area", "") or spare_data.get("category", ""),
            "family": "",
            "platform": ml_features.get("platform"),
        }
        if spare_data.get("category"):
            product["classification"]["productGroup"] = spare_data["category"]
        if spare_data.get("application"):
            product["classification"]["applicationScope"] = spare_data["application"]

    # Compatibility
    if web_data and web_data.get("compatibility"):
        product["compatibility"] = web_data["compatibility"]

    # Features
    if web_data and web_data.get("features"):
        product["features"] = web_data["features"]

    # Lifecycle
    product["lifecycle"] = {"status": "ACTIVE"}

    # Target users
    if web_data:
        product["targetUsers"] = ["Professional"]

    # ML (mandatory)
    product["ml"] = {"features": ml_features}

    # Raw source data for traceability
    if web_data and web_data.get("raw_attrs"):
        product["source"]["raw"] = {"specTable": {a["key"]: a["textValue"] for a in web_data["raw_attrs"]}}
    elif baseline and baseline.get("source", {}).get("raw"):
        product["source"]["raw"] = baseline["source"]["raw"]

    return product


# ---------------------------------------------------------------------------
# Async browser helpers (for parallel processing)
# ---------------------------------------------------------------------------
async def async_make_context(browser: AsyncBrowser) -> AsyncBrowserContext:
    return await browser.new_context(
        locale="en-GB",
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
    )


async def async_accept_cookies(page: AsyncPage) -> None:
    try:
        for sel in [
            "#onetrust-accept-btn-handler",
            "button:has-text('Accept All')",
            "button:has-text('Alle akzeptieren')",
            "[data-testid='cookie-accept-all']",
        ]:
            btn = page.locator(sel)
            if await btn.count() > 0 and await btn.first.is_visible():
                await btn.first.click(timeout=3000)
                await page.wait_for_timeout(1000)
                return
    except Exception:
        pass


async def async_fetch_page(page: AsyncPage, url: str, *, use_cache: bool = False) -> str:
    if use_cache:
        cached = cache_load(url)
        if cached:
            return cached
    await page.goto(url, wait_until="networkidle", timeout=PAGE_TIMEOUT_MS)
    await async_accept_cookies(page)
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
    await page.wait_for_timeout(1500)
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await page.wait_for_timeout(1500)
    html = await page.content()
    cache_save(url, html)
    await asyncio.sleep(REQUEST_DELAY_S)
    return html


async def async_search_bosch_professional(
    page: AsyncPage, product_number: str, *, use_cache: bool = False,
    model_name: str | None = None,
) -> str | None:
    """Search bosch-professional.com across multiple countries using model name."""
    search_terms = []
    if model_name and model_name != product_number:
        search_terms.append(model_name)
    search_terms.append(product_number)

    for country, lang in SEARCH_COUNTRIES:
        search_url_base = f"{BASE_URL}/{country}/{lang}/searchfrontend/?q="
        for term in search_terms:
            search_url = f"{search_url_base}{term}"
            try:
                html = await async_fetch_page(page, search_url, use_cache=use_cache)
            except Exception as e:
                log.debug("Search failed for %s on %s/%s: %s", term, country, lang, e)
                continue

            matches = re.findall(
                rf'href="(https://www\.bosch-professional\.com/{country}/{lang}/products/[a-zA-Z0-9\-]+-\w{{10}})[^"]*"',
                html,
            )
            if not matches:
                matches = re.findall(
                    r'href="(https://www\.bosch-professional\.com/\w+/\w+/products/[a-zA-Z0-9\-]+-\w{10})[^"]*"',
                    html,
                )
            if matches:
                log.info("Found product page for %s via %s/%s search '%s'", product_number, country, lang, term)
                return matches[0]

    return None


async def async_extract_from_product_page(
    page: AsyncPage, url: str, product_number: str, *, use_cache: bool = False,
) -> dict | None:
    try:
        html = await async_fetch_page(page, url, use_cache=use_cache)
    except Exception as e:
        log.warning("Product page failed for %s: %s", product_number, e)
        return None
    # Reuse sync extraction logic (pure HTML parsing, no browser needed)
    return _extract_product_data_from_html(html, url, product_number)


# ---------------------------------------------------------------------------
# Process a single product (async — one page per product)
# ---------------------------------------------------------------------------
async def async_process_product(
    browser: AsyncBrowser,
    semaphore: asyncio.Semaphore,
    product_number: str,
    pim_index: dict,
    use_cache: bool,
    progress_callback,
    model_web_cache: dict | None = None,
    pim_techdata_by_model: dict | None = None,
) -> dict:
    """Process one product number with concurrency limited by semaphore.

    Args:
        model_web_cache: Shared dict {model_name: web_data} to avoid re-crawling
                         the same model across product number variants.
        pim_techdata_by_model: Dict {model_name: parsed_techdata} for model-name
                               sibling matching from PIM.
    """
    async with semaphore:
        ctx = await async_make_context(browser)
        page = await ctx.new_page()
        page.set_default_timeout(PAGE_TIMEOUT_MS)

        baseline = pim_index.get(product_number)
        web_data = None
        spare_data = None
        source_type = "no_data"
        pim_tech_data = None

        # Get model name from baseline or spare API (will be fetched below)
        model_name = None
        if baseline:
            model_name = baseline.get("name", {}).get("display", "").strip() or None
            if model_name == product_number:
                model_name = None

        try:
            # Step 0: Get spare parts API data first (we need model_name for search)
            api_result = await asyncio.to_thread(query_spare_parts_api, product_number)
            if api_result:
                spare_data = build_product_from_spare_api(product_number, api_result, baseline)
                if spare_data:
                    source_type = "fallback_api"
                    # Get model name from spare API tradeName
                    if not model_name and spare_data.get("name"):
                        model_name = spare_data["name"]

            # Step 1: Check model-name web cache (another variant already crawled this model)
            if model_web_cache is not None and model_name and model_name in model_web_cache:
                cached_web = model_web_cache[model_name]
                if cached_web is not None:
                    web_data = cached_web
                    source_type = "enriched_web"
                    log.info("Reusing cached web data for %s (model: %s)", product_number, model_name)

            # Step 2: Try bosch-professional.com multi-country search (by model name)
            if web_data is None:
                product_url = await async_search_bosch_professional(
                    page, product_number, use_cache=use_cache, model_name=model_name,
                )
                if product_url:
                    web_data = await async_extract_from_product_page(page, product_url, product_number, use_cache=use_cache)
                    if web_data:
                        source_type = "enriched_web"

                # Store result in model cache (even if None, to avoid re-searching)
                if model_web_cache is not None and model_name:
                    model_web_cache[model_name] = web_data

            # Step 3: Resolve PIM techData (direct baseline or model-name sibling)
            if baseline:
                raw_td = baseline.get("source", {}).get("raw", {}).get("techData", {})
                if raw_td:
                    pim_tech_data = parse_pim_tech_data(raw_td)

            if not pim_tech_data and pim_techdata_by_model and model_name:
                pim_tech_data = pim_techdata_by_model.get(model_name)

            # Step 4: Build product
            product = build_product_dict(product_number, baseline, web_data, spare_data, pim_tech_data)

            _stats["total"] += 1
            if baseline:
                _stats["in_catalog"] += 1
            if source_type == "enriched_web":
                if model_web_cache is not None and model_name and model_name in model_web_cache and model_web_cache[model_name] is web_data:
                    _stats["enriched_web_cached"] += 1
                else:
                    _stats["enriched_web"] += 1
            elif source_type == "fallback_api":
                _stats["fallback_api"] += 1
            else:
                _stats["no_data"] += 1
            if pim_tech_data:
                if baseline and baseline.get("source", {}).get("raw", {}).get("techData"):
                    _stats["pim_techdata_direct"] += 1
                else:
                    _stats["pim_techdata_model"] += 1
            if product.get("ml", {}).get("features"):
                _stats["ml_filled"] += 1

            return product

        except Exception as e:
            log.error("Error processing %s: %s", product_number, e)
            _stats["errors"] += 1
            return build_product_dict(product_number, baseline, None, None)

        finally:
            await ctx.close()
            progress_callback()


# ---------------------------------------------------------------------------
# Batch processor — async with N parallel browser pages
# ---------------------------------------------------------------------------
def process_batch(
    product_numbers: list[str],
    pim_index: dict,
    use_cache: bool,
    workers: int,
    resume_set: set[str],
    pim_techdata_by_model: dict | None = None,
) -> list[dict]:
    """Process all product numbers with N parallel async browser pages."""
    return asyncio.run(_async_process_batch(
        product_numbers, pim_index, use_cache, workers, resume_set, OUTPUT_FILE,
        pim_techdata_by_model=pim_techdata_by_model,
    ))


async def _async_process_batch(
    product_numbers: list[str],
    pim_index: dict,
    use_cache: bool,
    workers: int,
    resume_set: set[str],
    output_file: Path,
    pim_techdata_by_model: dict | None = None,
) -> list[dict]:
    to_process = [pn for pn in product_numbers if pn not in resume_set]
    log.info("Processing %d products with %d parallel workers (%d skipped from checkpoint)",
             len(to_process), workers, len(resume_set))

    # Load existing results if resuming
    existing_results: dict[str, dict] = {}
    if resume_set and output_file.exists():
        try:
            prev = json.loads(output_file.read_text(encoding="utf-8"))
            existing_results = {p["ids"]["productNumber"]: p for p in prev}
            log.info("Loaded %d existing results from output file", len(existing_results))
        except Exception as e:
            log.warning("Could not load previous results: %s", e)

    results: list[dict] = []
    checkpoint_products: list[str] = list(resume_set)
    _interrupted = False

    # Shared cache: model_name → web_data (or None if already searched and not found)
    # This prevents re-crawling the same model for multiple product number variants.
    model_web_cache: dict[str, dict | None] = {}

    BATCH_SIZE = 50  # Process in small batches for crash safety

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        semaphore = asyncio.Semaphore(workers)

        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("Enriching...", total=len(to_process))

                def on_done():
                    progress.advance(task)

                # Process in batches for crash safety
                for batch_start in range(0, len(to_process), BATCH_SIZE):
                    if _interrupted:
                        break

                    batch = to_process[batch_start:batch_start + BATCH_SIZE]

                    tasks = [
                        async_process_product(
                            browser, semaphore, pn, pim_index, use_cache, on_done,
                            model_web_cache=model_web_cache,
                            pim_techdata_by_model=pim_techdata_by_model,
                        )
                        for pn in batch
                    ]

                    # Gather batch results
                    batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                    for r in batch_results:
                        if isinstance(r, Exception):
                            log.error("Task exception: %s", r)
                            _stats["errors"] += 1
                            continue
                        results.append(r)
                        pn = r["ids"]["productNumber"]
                        checkpoint_products.append(pn)
                        existing_results[pn] = r

                    # Save checkpoint + results after EVERY batch
                    _save_checkpoint(checkpoint_products)
                    _save_results(existing_results, output_file)
                    log.info("Batch saved — %d/%d total processed", len(results), len(to_process))

        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted! Saving progress...[/yellow]")
            _interrupted = True
        finally:
            # Always save on exit
            _save_checkpoint(checkpoint_products)
            _save_results(existing_results, output_file)
            console.print(f"[green]Progress saved: {len(checkpoint_products)} products in checkpoint[/green]")
            await browser.close()

    return list(existing_results.values())


def _save_checkpoint(processed: list[str]) -> None:
    CHECKPOINT_FILE.write_text(json.dumps({
        "processed": processed,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }), encoding="utf-8")


def _save_results(results_index: dict[str, dict], output_file: Path) -> None:
    """Incrementally save all results to the output file."""
    output_file.write_text(
        json.dumps(list(results_index.values()), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _load_checkpoint() -> set[str]:
    if CHECKPOINT_FILE.exists():
        data = json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
        return set(data.get("processed", []))
    return set()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Enrich BIQIC products with Bosch web data")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of products (0 = all)")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel browser instances")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument("--use-cache", action="store_true", help="Use cached HTML pages")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    console.print("\n[bold cyan]━━━ BIQIC Product Enrichment Crawler ━━━[/bold cyan]\n")

    # Step 1: Read unique product numbers from BIQIC CSV
    console.print("[bold]Step 1:[/bold] Reading product numbers from BIQIC CSV...")
    product_numbers = set()
    with open(BIQIC_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pn = row.get("mat_no_qm", "").strip()
            if pn:
                product_numbers.add(pn)

    # Sort with priority: 36xx first (product numbers = most critical for warranty),
    # then 06xx (SKUs), then everything else
    def _sort_key(pn: str) -> tuple:
        prefix = pn[:2]
        priority = {"36": 0, "06": 1, "F0": 2, "16": 3, "26": 4}.get(prefix, 5)
        return (priority, pn)

    product_numbers = sorted(product_numbers, key=_sort_key)
    console.print(f"  Unique product numbers: [green]{len(product_numbers)}[/green]")

    # Show breakdown by prefix
    from collections import Counter
    _pfx = Counter(pn[:2] for pn in product_numbers)
    for pfx, cnt in sorted(_pfx.items(), key=lambda x: _sort_key(x[0])):
        console.print(f"    {pfx}xx: {cnt}")

    if args.limit:
        product_numbers = product_numbers[:args.limit]
        console.print(f"  Limited to: [yellow]{args.limit}[/yellow]")

    # Step 2: Load existing PIM catalog as index
    console.print("\n[bold]Step 2:[/bold] Loading existing PIM catalog...")
    pim_index: dict[str, dict] = {}
    if PIM_CATALOG.exists():
        with open(PIM_CATALOG, "r", encoding="utf-8") as f:
            catalog = json.load(f)
        for product in catalog:
            pn = product.get("ids", {}).get("productNumber", "")
            if pn:
                pim_index[pn] = product
        console.print(f"  PIM catalog loaded: [green]{len(pim_index)}[/green] products")

    found_in_catalog = sum(1 for pn in product_numbers if pn in pim_index)
    console.print(f"  BIQIC products in PIM: [green]{found_in_catalog}[/green] / {len(product_numbers)}")

    # Step 2b: Build model-name → techData index from PIM
    # This lets us share mechanical specs across product number variants of the same model.
    console.print("\n[bold]Step 2b:[/bold] Building PIM techData model-name index...")
    pim_techdata_by_model: dict[str, dict] = {}
    for product in pim_index.values():
        raw_td = product.get("source", {}).get("raw", {}).get("techData", {})
        if raw_td:
            name = product.get("name", {}).get("display", "").strip()
            if name and name != product.get("ids", {}).get("productNumber", ""):
                parsed = parse_pim_tech_data(raw_td)
                if parsed:
                    existing = pim_techdata_by_model.get(name)
                    # Keep the richest techData per model name
                    if not existing or len(parsed) > len(existing):
                        pim_techdata_by_model[name] = parsed
    console.print(f"  Models with techData: [green]{len(pim_techdata_by_model)}[/green]")

    # Step 3: Resume support
    resume_set = set()
    if args.resume:
        resume_set = _load_checkpoint()
        console.print(f"  Resuming: [yellow]{len(resume_set)}[/yellow] already processed")

    # Step 4: Process all products
    console.print(f"\n[bold]Step 3:[/bold] Enriching products (workers={args.workers})...\n")
    results = process_batch(
        product_numbers, pim_index, args.use_cache, args.workers, resume_set,
        pim_techdata_by_model=pim_techdata_by_model,
    )

    # Step 5: Write output (already saved incrementally during processing)
    console.print(f"\n[bold]Step 4:[/bold] Output saved.")
    console.print(f"  Catalog: [green]{OUTPUT_FILE}[/green] ({len(results)} products)")

    # Meta
    meta = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "totalProducts": len(results),
        "stats": _stats,
        "args": {"limit": args.limit, "workers": args.workers, "resume": args.resume},
    }
    META_FILE.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    console.print(f"  Meta:    [green]{META_FILE}[/green]")

    # Summary
    console.print(f"\n[bold]Summary:[/bold]")
    for k, v in _stats.items():
        console.print(f"  {k:<20} {v:>6}")


if __name__ == "__main__":
    main()
