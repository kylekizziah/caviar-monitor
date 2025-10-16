#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Caviar Monitor Scraper
- Extracts brand, species, grade, grams, price, computed $/g
- Buckets items: for_2 (30-50g), for_4 (~100g), specials (125-250g), bulk (>=500g)
- Focused on US producers/retailers that ship overnight to Athens, GA
NOTE: This script makes HTTP requests; run it on your own machine/server with internet access.
"""
import re, json, math, time, sys
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
}

GRADE_PAT = re.compile(r"\b(Imperial|Royal|Gold(?:\s*Reserve)?|Reserve|Estate|Classic|Select|Supreme|Premier)\b", re.I)
SPECIES_PAT = re.compile(r"\b(Osetra|Sevruga|Beluga(?:\s*Hybrid)?|Kaluga\s*Hybrid|White\s*Sturgeon|Siberian|Paddlefish)\b", re.I)
GRAMS_PAT = re.compile(r"(\d+(?:[\.,]\d+)?)\s*(?:g|gram|grams)\b", re.I)
OZ_PAT = re.compile(r"(\d+(?:[\.,]\d+)?)\s*(?:oz|ounce|ounces)\b", re.I)
SCIENTIFIC_MAP = {
    "Osetra": "Acipenser gueldenstaedtii",
    "Sevruga": "Acipenser stellatus",
    "Beluga": "Huso huso",
    "Kaluga Hybrid": "Huso dauricus Ã Acipenser schrenckii",
    "White Sturgeon": "Acipenser transmontanus",
    "Siberian": "Acipenser baerii",
    "Paddlefish": "Polyodon spathula",
}

def oz_to_g(x):
    try:
        return float(x) * 28.3495
    except:
        return None

def bucket_size(g):
    if g is None: return None
    if 30 <= g <= 50: return "for_2"
    if 90 <= g <= 110: return "for_4"
    if 120 <= g <= 260: return "specials_10"
    if g >= 500: return "bulk"
    return "other"

@dataclass
class Item:
    brand: str
    url: str
    title: str
    price: Optional[float]
    grams: Optional[float]
    price_per_g: Optional[float]
    species_common: Optional[str]
    species_scientific: Optional[str]
    grade: Optional[str]
    size_bucket: Optional[str]
    region: Optional[str]

def first_match(pat, text):
    m = pat.search(text or "")
    return m.group(0) if m else None

def extract_grams(text):
    m = GRAMS_PAT.search(text or "")
    if m:
        return float(m.group(1).replace(",", "."))
    m2 = OZ_PAT.search(text or "")
    if m2:
        return round(float(m2.group(1).replace(",", ".")) * 28.3495, 2)
    return None

def parse_price(text):
    m = re.search(r"\$\s*([0-9][0-9,]*\.?[0-9]*)", text or "")
    if m:
        return float(m.group(1).replace(",", ""))
    return None

def normalize_species(token: Optional[str]) -> Optional[str]:
    if not token: return None
    t = token.strip().title()
    # Unify variants
    t = t.replace("Kaluga  Hybrid", "Kaluga Hybrid")
    t = t.replace("White  Sturgeon", "White Sturgeon")
    return t

def infer_scientific(common: Optional[str]) -> Optional[str]:
    if not common: return None
    return SCIENTIFIC_MAP.get(common, None)

def fetch(url):
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def crawl_site(site: Dict[str, Any]) -> List[Item]:
    out: List[Item] = []
    html = fetch(site["url"])
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    # Site-specific selector first
    if site.get("list_selector"):
        for a in soup.select(site["list_selector"]):
            href = a.get("href")
            if href and href.startswith("/"):
                href = site["base"].rstrip("/") + href
            if href and href.startswith("http"):
                links.add(href)
    # Fallback generic
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(tok in href for tok in ("/product", "/products/")):
            if href.startswith("/"):
                href = site["base"].rstrip("/") + href
            if href.startswith("http"):
                links.add(href)

    for href in list(links)[:60]:  # cap to be polite
        try:
            ph = fetch(href)
        except Exception:
            continue
        psoup = BeautifulSoup(ph, "html.parser")
        title = psoup.select_one(site.get("item", {}).get("title_selector") or "h1")
        title_text = title.get_text(" ", strip=True) if title else ""
        body_text = psoup.get_text(" ", strip=True)

        # Price: try selector then fallback regex
        price_sel = site.get("item", {}).get("price_selector")
        price_text = ""
        if price_sel:
            node = psoup.select_one(price_sel)
            if node:
                price_text = node.get_text(" ", strip=True)
        if not price_text:
            price_text = body_text
        price = parse_price(price_text)

        # Variant grams: look in selectors first
        grams = None
        var_sel = site.get("item", {}).get("variant_weight_selector")
        if var_sel:
            vs = psoup.select(var_sel)
            if vs:
                txt = " ".join(v.get_text(" ", strip=True) for v in vs)
                grams = extract_grams(txt)

        # If still none, infer from title/body
        if grams is None:
            grams = extract_grams(title_text) or extract_grams(body_text)

        # Grade/species from title first, then body
        grade = first_match(GRADE_PAT, title_text) or first_match(GRADE_PAT, body_text)
        species = first_match(SPECIES_PAT, title_text) or first_match(SPECIES_PAT, body_text)
        species = normalize_species(species) or site.get("species_hint")
        species_sci = infer_scientific(species)

        ppg = round(price/grams, 3) if price and grams else None
        bucket = bucket_size(grams)

        out.append(Item(
            brand=site["name"],
            url=href,
            title=title_text or "",
            price=price,
            grams=grams,
            price_per_g=ppg,
            species_common=species,
            species_scientific=species_sci,
            grade=(grade.title() if grade else None),
            size_bucket=bucket,
            region=site.get("region")
        ))
        time.sleep(0.3)
    return out

def main():
    import yaml, csv
    with open("price_sites.yaml", "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    all_items: List[Item] = []
    for site in cfg["sites"]:
        try:
            items = crawl_site(site)
            all_items.extend(items)
        except Exception as e:
            print(f"[WARN] {site['name']}: {e}", file=sys.stderr)
            continue

    rows = [asdict(x) for x in all_items if x.title]
    BUCKET_ORDER = {"for_2": 0, "for_4": 1, "specials_10": 2, "bulk": 3, "other": 9, None: 99}
    rows.sort(key=lambda r: (BUCKET_ORDER.get(r.get("size_bucket"), 99), r.get("price_per_g") or 1e9))

    with open("caviar_prices.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        if rows: w.writeheader()
        w.writerows(rows)

    with open("caviar_prices.json", "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    # Print simple picks for your digest
    def best_in_bucket(bucket):
        for r in rows:
            if r["size_bucket"] == bucket and r.get("price_per_g"):
                return r
        return None

    picks = {
        "for_2": best_in_bucket("for_2"),
        "for_4": best_in_bucket("for_4"),
        "specials_10": best_in_bucket("specials_10"),
        "bulk": best_in_bucket("bulk"),
    }
    print(json.dumps(picks, indent=2))

if __name__ == "__main__":
    main()
