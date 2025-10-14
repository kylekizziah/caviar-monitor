import os, re, json, time, sqlite3, yaml
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
from jinja2 import Environment, FileSystemLoader, select_autoescape
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv

# =========================
# Environment & constants
# =========================
load_dotenv()

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
FROM_EMAIL       = os.getenv("FROM_EMAIL")
TO_EMAIL         = os.getenv("TO_EMAIL")

DB_PATH = os.getenv("DB_PATH", "caviar_agent.db")

# keep runs short so an email always goes out
RUN_LIMIT_SECONDS  = int(os.getenv("RUN_LIMIT_SECONDS", "150"))
MAX_LINKS_PER_SITE = int(os.getenv("MAX_LINKS_PER_SITE", "50"))

# handy toggles
SEND_TEST        = os.getenv("SEND_TEST")           # "1" = send test email then exit
DEBUG_URL        = os.getenv("DEBUG_URL")           # scrape one URL then exit
REQUIRE_SPECIES  = int(os.getenv("REQUIRE_SPECIES", "1"))  # 1=require, 0=allow missing (for testing)

# templates
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), 'templates')
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR),
                  autoescape=select_autoescape(['html','xml']))

# =========================
# Filters, species & parsing
# =========================
CAVIAR_WORD = re.compile(r"\bcaviar\b", re.I)
SIZE_RE     = re.compile(r'(\d+(?:\.\d+)?)\s*(g|gram|grams|oz|ounce|ounces)\b', re.I)
MONEY_RE    = re.compile(r'([$\£\€])\s*([0-9]+(?:\.[0-9]{1,2})?)')

EXCLUDE_WORDS = [
    "gift set","giftset","set","bundle","sampler","flight","pairing","experience","kit",
    "accessory","accessories","spoon","mother of pearl","key","opener","tin opener",
    "blini","bellini","crepe","crème fraîche","creme fraiche","ice bowl","server",
    "tray","plate","dish","cooler","chiller","gift card","card","chips","tote","bag",
    "club","subscription","subscribe","duo","trio","quad","pack","2-pack","3-pack","4-pack",
    "class","tasting","pair","pairings","collection","assortment","starter"
]
NON_STURGEON_ROE = [
    "salmon roe","trout roe","whitefish roe","tobiko","masago","ikura","capelin roe",
    "lumpfish", "flying fish roe", "bowfin", "paddlefish", "hackleback roe"
]
ALLOW_HACKLEBACK_STURGEON = True

URL_BLOCKLIST = [
    "/cart", "/account", "/login", "/search", "/policy", "/policies",
    "/pages/contact", "/pages/faq", "/pages/shipping", "/pages/returns",
    "/privacy", "/terms", "/checkout"
]
LIKELY_TIN_SIZES_G = [28,30,50,56,57,85,100,114,125,180,200,250,500,1000]

# Species patterns: (pattern, normalized common, latin)
SPECIES_PATTERNS = [
    (r"\bbeluga\b|\bhuso\s*huso\b", "Beluga", "Huso huso"),
    (r"\bkaluga\b|\bhuso\s*dauricus\b", "Kaluga", "Huso dauricus"),
    (r"\bosc?ietr?a\b|\bossetra\b|\bgueldenstaedtii\b|\bacipenser\s*gueldenstaedtii\b", "Osetra", "Acipenser gueldenstaedtii"),
    (r"\bsevruga\b|\bacipenser\s*stellatus\b|\bstellatus\b", "Sevruga", "Acipenser stellatus"),
    (r"\bsiberian\b|\ba\.?\s*baerii\b|\bacipenser\s*baerii\b|\bbaerii\b", "Siberian", "Acipenser baerii"),
    (r"\bwhite\s*sturgeon\b|\ba\.?\s*transmontanus\b|\bacipenser\s*transmontanus\b|\btransmontanus\b", "White Sturgeon", "Acipenser transmontanus"),
    (r"\bsterlet\b|\bacipenser\s*ruthenus\b|\bruthenus\b", "Sterlet", "Acipenser ruthenus"),
    (r"\bhackleback\b|\bshovelnose\b|\bscaphirhynchus\s*platorynchus\b", "Hackleback", "Scaphirhynchus platorynchus"),
]
ALLOWED_SPECIES = {s[1] for s in SPECIES_PATTERNS}

# Origin patterns (coarse)
ORIGIN_PATTERNS = [
    (r"\busa\b|\bunited states\b|\bcalifornia\b|\bidaho\b|\bmontana\b", "USA"),
    (r"\buruguay\b|\brivera\b", "Uruguay"),
    (r"\bitaly\b|\bitalian\b", "Italy"),
    (r"\bfrance\b|\bfrench\b|\bgironde\b", "France"),
    (r"\bgermany\b|\bgerman\b", "Germany"),
    (r"\bpoland\b|\bpolish\b", "Poland"),
    (r"\bbulgaria\b|\bbulgarian\b", "Bulgaria"),
    (r"\bisrael\b|\bisraeli\b", "Israel"),
    (r"\bchina\b|\bchinese\b|\bheilongjiang\b", "China"),
    (r"\brussia\b|\brussian\b", "Russia"),
    (r"\bbelgium\b|\bbelgian\b", "Belgium"),
    (r"\bromania\b|\bromanian\b", "Romania"),
    (r"\bcanada\b|\bcanadian\b", "Canada"),
    (r"\biran\b|\biranian\b", "Iran"),
    (r"\bmadagascar\b", "Madagascar"),
]

def norm_netloc(host: str) -> str:
    host = host or ""
    return host[4:] if host.startswith("www.") else host

# =========================
# DB (tiny) + migrate
# =========================
def init_db(path):
    conn = sqlite3.connect(path, check_same_thread=False)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS prices(
        id INTEGER PRIMARY KEY,
        site TEXT, url TEXT, name TEXT, currency TEXT, price REAL,
        size_g REAL, size_label TEXT, per_g REAL, seen_at TEXT
    )""")
    # Add new columns if missing
    cols = {row[1] for row in c.execute("PRAGMA table_info(prices)").fetchall()}
    if "species" not in cols:
        c.execute("ALTER TABLE prices ADD COLUMN species TEXT")
    if "species_latin" not in cols:
        c.execute("ALTER TABLE prices ADD COLUMN species_latin TEXT")
    if "origin_country" not in cols:
        c.execute("ALTER TABLE prices ADD COLUMN origin_country TEXT")
    if "origin_notes" not in cols:
        c.execute("ALTER TABLE prices ADD COLUMN origin_notes TEXT")
    conn.commit()
    return conn

def store_prices(conn, items):
    if not items: return
    c = conn.cursor()
    for it in items:
        c.execute("""INSERT INTO prices(site,url,name,currency,price,size_g,size_label,per_g,seen_at,species,species_latin,origin_country,origin_notes)
                     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (it["site"], it["url"], it["name"], it["currency"], it["price"],
                   it["size_g"], it["size_label"], it["per_g"], datetime.utcnow().isoformat(),
                   it.get("species"), it.get("species_latin"), it.get("origin_country"), it.get("origin_notes")))
    conn.commit()

def get_cheapest(conn, top_n=12):
    """
    Cheapest CURRENT options, de-duplicated, species-required (unless REQUIRE_SPECIES=0):
      1) Keep latest per (site, url, size_g)
      2) Collapse to lowest per-g for (site, name, size_g, species, origin_country)
      3) Sort by $/g asc, limit N
    """
    c = conn.cursor()
    where_species = "" if REQUIRE_SPECIES == 0 else "WHERE species IS NOT NULL AND species <> ''"
    q = f"""
      WITH latest AS (
        SELECT site,name,url,currency,price,size_g,size_label,per_g,seen_at,species,species_latin,origin_country,origin_notes,
               ROW_NUMBER() OVER (
                 PARTITION BY site, url, size_g
                 ORDER BY datetime(seen_at) DESC
               ) rn
        FROM prices
        {where_species}
      ),
      dedup AS (
        SELECT site,name,url,currency,price,size_g,size_label,per_g,species,species_latin,origin_country,origin_notes
        FROM latest WHERE rn=1
      ),
      collapsed AS (
        SELECT
          site, name, currency, size_g, size_label, species, species_latin, origin_country, origin_notes,
          MIN(price) AS price,
          MIN(per_g) AS per_g,
          MIN(url)   AS url
        FROM dedup
        GROUP BY site, name, size_g, size_label, currency, species, species_latin, origin_country, origin_notes
      )
      SELECT site,name,price,currency,size_g,size_label,per_g,url,species,species_latin,origin_country,origin_notes
      FROM collapsed
      ORDER BY per_g ASC
      LIMIT ?
    """
    rows = c.execute(q, (top_n,)).fetchall()
    return [{
        "site": r[0], "name": r[1], "price": r[2], "currency": r[3],
        "size_g": r[4], "size_label": r[5], "per_g": r[6], "url": r[7],
        "species": r[8], "species_latin": r[9],
        "origin_country": r[10], "origin_notes": r[11]
    } for r in rows]

def get_movers(conn):
    c = conn.cursor()
    where_species = "" if REQUIRE_SPECIES == 0 else "WHERE species IS NOT NULL AND species <> ''"
    q = f"""
      WITH ranked AS (
        SELECT site,name,currency,price,size_label,seen_at,
               ROW_NUMBER() OVER (PARTITION BY site,name ORDER BY datetime(seen_at) DESC) rn
        FROM prices
        {where_species}
      )
      SELECT a.site,a.name,a.currency,a.price,a.size_label,b.price
      FROM ranked a
      LEFT JOIN ranked b
        ON a.site=b.site AND a.name=b.name AND b.rn=2
      WHERE a.rn=1 AND b.price IS NOT NULL
    """
    out=[]
    for site,name,cur,price,label,prev in c.execute(q).fetchall():
        if prev and price != prev:
            delta=price-prev
            pct=round((delta/prev)*100,2)
            out.append({"site":site,"name":name,"currency":cur,"price":price,
                        "delta_abs":abs(delta),"delta_pct":pct,
                        "delta_sign":"+" if delta>0 else "-","size_label":label})
    return sorted(out, key=lambda x: -abs(x["delta_pct"]))[:5]

# =========================
# HTTP session (no proxies)
# =========================
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    retries = Retry(total=3, backoff_factor=0.4, status_forcelist=[429,500,502,503,504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://",  HTTPAdapter(max_retries=retries))
    return s

SESSION = make_session()

def safe_get(url, timeout=20):
    try:
        return SESSION.get(url, timeout=timeout, allow_redirects=True)
    except Exception as e:
        print("GET exception:", e, url)
        return None

# =========================
# Species & origin helpers
# =========================
def parse_size(text):
    m = SIZE_RE.search(text or "")
    if not m: return None, None
    val, unit = float(m.group(1)), m.group(2).lower()
    grams = val * 28.3495 if unit.startswith('oz') else val
    label_raw = f"{int(val) if val.is_integer() else val}{unit}"
    return grams, label_raw

def grams_to_label_both(size_g, packaging=None):
    if not size_g: return None
    g = int(round(size_g))
    oz = size_g / 28.3495
    oz_round_int = round(oz)
    if abs(oz - oz_round_int) < 0.05:
        oz_str = f"{int(oz_round_int)} oz"
    else:
        oz_str = f"{oz:.1f}".rstrip('0').rstrip('.') + " oz"
    return f"{oz_str} / {g} g" + (f" {packaging}" if packaging else "")

def infer_packaging(text):
    t = (text or "").lower()
    if "tin" in t: return "tin"
    if "jar" in t: return "jar"
    return None

def looks_like_accessory(text):
    t = (text or "").lower()
    return any(w in t for w in EXCLUDE_WORDS)

def contains_non_sturgeon_or_roe(text):
    t = (text or "").lower()
    if any(w in t for w in NON_STURGEON_ROE):
        if ALLOW_HACKLEBACK_STURGEON and ("hackleback" in t and "sturgeon" in t):
            return False
        return True
    return False

def url_or_text_has_caviar(url, text):
    u = (url or "").lower()
    t = (text or "").lower()
    return ("caviar" in u) or CAVIAR_WORD.search(t)

def extract_species_any(text):
    t = (text or "").lower()
    for pat, common, latin in SPECIES_PATTERNS:
        if re.search(pat, t, re.I):
            return common, latin
    return None, None

def extract_origin_any(text):
    t = (text or "").lower()
    for pat, country in ORIGIN_PATTERNS:
        if re.search(pat, t, re.I):
            return country, None
    m = re.search(r"(raised|farmed|produced|harvested)\s+in\s+([A-Za-z ]{3,30})", t)
    if m:
        return m.group(2).strip().title(), m.group(0)
    return None, None

def extract_from_metas(soup):
    texts=[]
    for sel in [
        "meta[name='description']",
        "meta[property='og:description']",
        "meta[name='keywords']",
        "meta[property='product:retailer_item_id']",
    ]:
        tag = soup.select_one(sel)
        if tag:
            for attr in ("content","value"):
                if tag.has_attr(attr) and tag[attr]:
                    texts.append(tag[attr])
    return " ".join(texts)

def extract_from_feature_blocks(soup):
    # Tables, definition lists, bullet lists often contain "Species", "Origin", "Type"
    chunks=[]
    for table in soup.select("table"):
        chunks.append(table.get_text(" ", strip=True))
    for dl in soup.select("dl"):
        chunks.append(dl.get_text(" ", strip=True))
    for li in soup.select("ul li, ol li"):
        chunks.append(li.get_text(" ", strip=True))
    for div in soup.select("[class*='spec'],[class*='feature'],[class*='attribute'],[class*='details']"):
        chunks.append(div.get_text(" ", strip=True))
    return " ".join(chunks)

def extract_species_from_json_ld(soup):
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "{}")
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for it in items:
            if not isinstance(it, dict): continue
            desc = (it.get("description") or "")
            extras = " ".join([str(ap.get("value","")) for ap in (it.get("additionalProperty") or []) if isinstance(ap, dict)])
            sp = extract_species_any(desc + " " + extras)
            if sp[0]: return sp
    return (None, None)

def extract_origin_from_json_ld(soup):
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "{}")
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for it in items:
            if not isinstance(it, dict): continue
            addr = ""
            man  = it.get("manufacturer") or it.get("brand") or {}
            if isinstance(man, dict):
                addr = " ".join([
                    str((man.get("address") or {}).get(k,"")) for k in ("addressCountry","addressLocality","addressRegion")
                ])
            extras = " ".join([str(ap.get("value","")) for ap in (it.get("additionalProperty") or []) if isinstance(ap, dict)])
            country, notes = extract_origin_any((it.get("description") or "") + " " + addr + " " + extras)
            if country: return country, notes
    return None, None

def ensure_sturgeon_species(species_common):
    return species_common in ALLOWED_SPECIES

def is_individual_tinjar(name, page_text, url=None):
    nm = (name or "").lower(); pg = (page_text or "").lower()
    if not (url_or_text_has_caviar(url, nm) or url_or_text_has_caviar(url, pg)):
        return False
    if looks_like_accessory(nm) or looks_like_accessory(pg): return False
    if contains_non_sturgeon_or_roe(nm) or contains_non_sturgeon_or_roe(pg): return False
    pack = infer_packaging(nm) or infer_packaging(pg)
    size_g, _ = parse_size(nm + " " + pg)
    if pack: return True
    if size_g and any(abs(size_g - s) <= 2 for s in LIKELY_TIN_SIZES_G): return True
    return False

def size_tokens(size_g):
    if not size_g: return set()
    g = int(round(size_g)); oz = size_g/28.3495
    oz_round = round(oz, 1)
    tokens = {f"{g}g", f"{g} g"}
    if g in (28,29,30): tokens |= {"28g","29g","30g","28 g","29 g","30 g"}
    def ozs(x): return str(int(round(x))) if abs(x-round(x))<0.05 else f"{x:.1f}".rstrip('0').rstrip('.')
    for o in {oz_round, round(oz)}:
        s = ozs(o)
        tokens |= {f"{s}oz", f"{s} oz", f"{s} ounce", f"{s} ounces"}
    return {t.lower() for t in tokens}

def extract_ld_json_products_extended(soup):
    items=[]
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "{}")
        except Exception:
            continue
        for b in (data if isinstance(data, list) else [data]):
            if not isinstance(b, dict): continue
            if (b.get("@type") in ("Product","Offer")) or ("offers" in b):
                name = (b.get("name") or "").strip()
                desc = (b.get("description") or "")
                offers=[]
                off=b.get("offers")
                if isinstance(off, dict): offers=[off]
                elif isinstance(off, list): offers=[o for o in off if isinstance(o, dict)]
                norm=[]
                for o in offers:
                    norm.append({
                        "price": (float(o.get("price")) if (o.get("price") not in (None,"")) else None),
                        "currency": o.get("priceCurrency","USD"),
                        "name": (o.get("name") or o.get("description") or ""),
                        "sku": o.get("sku") or ""
                    })
                items.append({"name":name,"desc":desc,"offers":norm})
    return items

def choose_offer_for_size(offers, size_g):
    if not offers or not size_g: return None
    toks = size_tokens(size_g)
    for o in offers:
        s = (o.get("name","") + " " + o.get("sku","")).lower()
        if any(t in s for t in toks):
            return o
    priced = [o for o in offers if o.get("price")]
    if len(priced)==1: return priced[0]
    return sorted(priced, key=lambda x: x.get("price"))[0] if priced else None

# -------- WooCommerce variations --------
def extract_wc_variations(soup):
    results = []
    for form in soup.select("[data-product_variations]"):
        raw = form.get("data-product_variations")
        if not raw: 
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        if not isinstance(data, list):
            continue
        for var in data:
            price = None
            if isinstance(var.get("display_price"), (int,float)):
                price = float(var.get("display_price"))
            else:
                price_html = var.get("price_html") or ""
                m = MONEY_RE.search(BeautifulSoup(price_html, "lxml").get_text(" ", strip=True))
                if m: price = float(m.group(2))
            attrs = var.get("attributes") or {}
            label_bits = [str(v) for v in attrs.values() if v]
            label = " ".join(label_bits).strip() or (var.get("variation_description") or "").strip()
            size_g, _ = parse_size(label)
            if not size_g:
                label_ex = (var.get("sku") or "") + " " + (var.get("image",{}).get("alt","") or "")
                size_g, _ = parse_size(label_ex)
            if not size_g or price is None: 
                continue
            if not any(abs(size_g - s) <= 2 for s in LIKELY_TIN_SIZES_G): 
                continue
            results.append({"price": float(price), "currency":"USD", "label":label, "size_g": float(size_g)})
    return results

# -------- Shopify variants + product JSON (tags/options/vendor) --------
def extract_shopify_variants_and_tags(soup):
    results=[]
    extra_texts=[]
    for tag in soup.find_all("script", type="application/json"):
        txt = tag.string or ""
        if not txt:
            continue
        # product JSON blocks often include "variants" and "tags"
        if '"variants"' not in txt and '"tags"' not in txt and '"options"' not in txt:
            continue
        try:
            data = json.loads(txt)
        except Exception:
            continue
        # If this is the product object
        if isinstance(data, dict):
            # collect tags/options/vendor for species detection
            for key in ("tags","options","vendor","type"):
                val = data.get(key)
                if isinstance(val, list):
                    extra_texts.append(" ".join([str(x) for x in val]))
                elif isinstance(val, str):
                    extra_texts.append(val)
            # variants
            variants = data.get("variants")
            if isinstance(variants, list):
                for v in variants:
                    title = str(v.get("title") or "").strip()
                    size_g, _ = parse_size(title)
                    if not size_g:
                        continue
                    price_cents = v.get("price")
                    if price_cents is None:
                        continue
                    price = float(price_cents)/100.0
                    if not any(abs(size_g - s) <= 2 for s in LIKELY_TIN_SIZES_G):
                        continue
                    results.append({"price": price, "currency": "USD", "label": title, "size_g": float(size_g)})
    return results, " ".join(extra_texts)

# -------- Magento price/variant helpers (basic) --------
def extract_magento_price_and_sizes(soup):
    out=[]
    text = soup.get_text(" ", strip=True)
    size_g, _ = parse_size(text)
    price = None
    node = soup.select_one("[data-price-amount]")
    if node and node.has_attr("data-price-amount"):
        try: price = float(node["data-price-amount"])
        except: price = None
    if price is None:
        meta = soup.select_one("meta[property='og:price:amount']")
        if meta and meta.has_attr("content"):
            try: price = float(meta["content"])
            except: pass
    if size_g and price:
        out.append({"price":price,"currency":"USD","label":"","size_g":float(size_g)})
    return out

# =========================
# Scraper core
# =========================
def scrape_product_page(url, site_selectors=None):
    """
    Extract one or more products from an exact product URL.
    Requires species (sturgeon) and tin/jar with size+price (unless REQUIRE_SPECIES=0).
    Also extracts origin (country/region) where possible.
    """
    low = url.lower()
    if any(b in low for b in URL_BLOCKLIST): return []
    if "/product" not in low and "/products" not in low: return []

    r = safe_get(url)
    if not (r and r.ok):
        code = r.status_code if r else "ERR"
        print(f"GET failed ({code}):", url); return []

    soup = BeautifulSoup(r.text, "lxml")
    page_text = soup.get_text(" ", strip=True)

    out=[]
    page_title = (soup.title.string if soup.title else "") or ""
    h1 = soup.find("h1")
    h1_text = h1.get_text(strip=True) if h1 else ""
    canonical_name = (h1_text or page_title).strip()

    # Collect extra text sources for species/origin
    metas_text   = extract_from_metas(soup)
    features_txt = extract_from_feature_blocks(soup)

    # species (mandatory if REQUIRE_SPECIES=1)
    sp_name, sp_latin = extract_species_any(" ".join([canonical_name, page_text, metas_text, features_txt]))
    if not sp_name:
        sp_name, sp_latin = extract_species_from_json_ld(soup)

    # origin (best effort)
    origin_country, origin_notes = extract_origin_any(" ".join([canonical_name, page_text, metas_text, features_txt]))
    if not origin_country:
        origin_country, origin_notes = extract_origin_from_json_ld(soup)

    # JSON-LD products (try first)
    for item in extract_ld_json_products_extended(soup):
        base_name = (item.get("name") or "").strip() or canonical_name
        if not is_individual_tinjar(base_name, page_text, url=url):
            # print for diagnostics
            # print("Skip: not tin/jar", url)
            continue
        size_g, _ = parse_size(base_name + " " + (item.get("desc") or "") + " " + page_text)
        if not size_g:
            # print("Skip: no size", url)
            continue
        offer = choose_offer_for_size(item.get("offers"), size_g)
        price = offer.get("price") if offer else None
        currency = (offer.get("currency") if offer else "USD") if price else None
        if not price:
            m = MONEY_RE.search(page_text or "")
            if m:
                currency = {'$':'USD','£':'GBP','€':'EUR'}.get(m.group(1),'USD')
                price = float(m.group(2))
        if not (size_g and price and currency):
            # print("Skip: no price/currency", url)
            continue
        # require species?
        if REQUIRE_SPECIES == 1:
            if not (sp_name and ensure_sturgeon_species(sp_name)):
                # print("Skip: no/invalid species", url)
                continue
        pack = infer_packaging(base_name) or infer_packaging(page_text)
        size_label = grams_to_label_both(size_g, packaging=pack)
        out.append({
            "name": base_name, "price": float(price), "currency": currency,
            "size_g": float(size_g), "size_label": size_label or "n/a",
            "per_g": float(price)/float(size_g), "url": url,
            "species": sp_name, "species_latin": sp_latin,
            "origin_country": origin_country, "origin_notes": origin_notes
        })

    # Platform variants (Shopify/WooCommerce/Magento)
    if not out and url_or_text_has_caviar(url, page_text):
        if not is_individual_tinjar(canonical_name, page_text, url=url):
            # print("Skip: not tin/jar (platform branch)", url)
            return []

        # Shopify: variants + extra tags/options/vendor to improve species recall
        shopify, shopify_extra = extract_shopify_variants_and_tags(soup)
        combined_context = " ".join([page_text, metas_text, features_txt, shopify_extra])
        if not sp_name:
            sp_name, sp_latin = extract_species_any(combined_context)
        if not sp_name:
            sp_name, sp_latin = extract_species_from_json_ld(soup)
        if REQUIRE_SPECIES == 1 and not (sp_name and ensure_sturgeon_species(sp_name)):
            # print("Skip: no/invalid species (Shopify)", url)
            pass
        else:
            if shopify:
                for v in shopify:
                    pack = infer_packaging(canonical_name) or infer_packaging(page_text)
                    size_label = grams_to_label_both(v["size_g"], packaging=pack)
                    out.append({
                        "name": canonical_name, "price": v["price"], "currency": v["currency"],
                        "size_g": v["size_g"], "size_label": size_label or "n/a",
                        "per_g": v["price"]/v["size_g"], "url": url,
                        "species": sp_name, "species_latin": sp_latin,
                        "origin_country": origin_country, "origin_notes": origin_notes
                    })

        # WooCommerce
        if not out:
            wc = extract_wc_variations(soup)
            if REQUIRE_SPECIES == 1 and not (sp_name and ensure_sturgeon_species(sp_name)):
                # print("Skip: no/invalid species (WooCommerce)", url)
                pass
            else:
                for v in wc:
                    pack = infer_packaging(canonical_name) or infer_packaging(page_text)
                    size_label = grams_to_label_both(v["size_g"], packaging=pack)
                    out.append({
                        "name": canonical_name, "price": v["price"], "currency": v["currency"],
                        "size_g": v["size_g"], "size_label": size_label or "n/a",
                        "per_g": v["price"]/v["size_g"], "url": url,
                        "species": sp_name, "species_latin": sp_latin,
                        "origin_country": origin_country, "origin_notes": origin_notes
                    })

        # Magento
        if not out:
            mag = extract_magento_price_and_sizes(soup)
            if REQUIRE_SPECIES == 1 and not (sp_name and ensure_sturgeon_species(sp_name)):
                # print("Skip: no/invalid species (Magento)", url)
                pass
            else:
                for v in mag:
                    pack = infer_packaging(canonical_name) or infer_packaging(page_text)
                    size_label = grams_to_label_both(v["size_g"], packaging=pack)
                    out.append({
                        "name": canonical_name, "price": v["price"], "currency": v["currency"],
                        "size_g": v["size_g"], "size_label": size_label or "n/a",
                        "per_g": v["price"]/v["size_g"], "url": url,
                        "species": sp_name, "species_latin": sp_latin,
                        "origin_country": origin_country, "origin_notes": origin_notes
                    })

    # final guard: if REQUIRE_SPECIES=1 and we somehow added with missing species, purge
    if REQUIRE_SPECIES == 1:
        out = [p for p in out if p.get("species") and ensure_sturgeon_species(p["species"])]

    return out

def crawl_site(site, deadline=None):
    results=[]
    start_urls = site.get("start_urls",[])
    sel = site.get("selectors",{}) or {}
    whitelist = set(norm_netloc(h) for h in (site.get("allow_domains") or []))
    site_name = site.get("name") or (whitelist and next(iter(whitelist))) or "site"

    def timed_out(): return bool(deadline and datetime.utcnow() >= deadline)
    def domain_ok(u):
        if not whitelist: return True
        net = norm_netloc(urlparse(u).netloc); return net in whitelist
    def allowed(u):
        low=(u or "").lower()
        if any(b in low for b in URL_BLOCKLIST): return False
        # favor product detail pages; collections often point to these
        return ("/products/" in low) or ("/product/" in low)

    for start in start_urls:
        if timed_out(): print(f"[{site_name}] timebox before start."); break
        r = safe_get(start)
        if not (r and r.ok):
            code = r.status_code if r else "ERR"
            print(f"START failed ({code}):", start); continue
        soup = BeautifulSoup(r.text, "lxml")
        links=set()

        if sel.get("product_link"):
            for a in soup.select(sel["product_link"]):
                href=a.get("href")
                if not href: continue
                full=urljoin(start, href)
                if domain_ok(full) and allowed(full): links.add(full)

        for a in soup.find_all("a", href=True):
            full=urljoin(start, a["href"])
            if domain_ok(full) and allowed(full): links.add(full)

        links_list = list(links)[:MAX_LINKS_PER_SITE]
        print(f"[{site_name}] found {len(links_list)} product links on {start}")

        kept=0
        for url in links_list:
            if timed_out(): print(f"[{site_name}] timebox mid-site."); break
            prods = scrape_product_page(url, site_selectors=sel)
            if not prods:
                # Diagnostic logging for why nothing kept from this URL
                # print(f"[{site_name}] no keep: {url}")
                pass
            for p in prods:
                p["site"]=site_name; results.append(p); kept+=1
        print(f"[{site_name}] kept {kept} products from {start}")
        time.sleep(0.3)
        if timed_out(): break
    return results

# =========================
# Email
# =========================
def render_html(cheapest, movers):
    tpl = env.get_template("digest_template.html")
    return tpl.render(
        date=datetime.utcnow().strftime("%B %d, %Y"),
        cheapest=cheapest, movers=movers, news_items=[]
    )

def send_email_html(subject, html_body):
    if not (SENDGRID_API_KEY and FROM_EMAIL and TO_EMAIL):
        print("ERROR: SendGrid vars not set."); return
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        resp = sg.send(Mail(from_email=FROM_EMAIL, to_emails=TO_EMAIL,
                            subject=subject, html_content=html_body))
        print("Email send status:", resp.status_code)
    except Exception as e:
        import traceback; print("SENDGRID ERROR:", e); print(traceback.format_exc())

# =========================
# Main
# =========================
def main():
    try:
        if SEND_TEST == "1":
            print("SEND_TEST=1 — sending test email.")
            send_email_html(f"Test — {datetime.utcnow():%b %d, %Y %H:%M UTC}",
                            "<h1>Test email from Caviar Agent</h1><p>If you see this, SendGrid works.</p>")
            return

        if DEBUG_URL:
            print("DEBUG_URL →", DEBUG_URL)
            prods = scrape_product_page(DEBUG_URL)
            print("Extracted products:", prods)
            return

        start = datetime.utcnow()
        deadline = start + timedelta(seconds=RUN_LIMIT_SECONDS)
        def time_left(): return max(0, (deadline - datetime.utcnow()).total_seconds())

        print(f"Starting run at {start.isoformat()} (timebox {RUN_LIMIT_SECONDS}s, REQUIRE_SPECIES={REQUIRE_SPECIES})")
        conn = init_db(DB_PATH)

        # Load sites
        seed_path = os.path.join(os.path.dirname(__file__), "price_sites.yaml")
        try:
            with open(seed_path,"r") as f:
                cfg = yaml.safe_load(f) or {}
            sites = cfg.get("sites", [])
            print("Loaded price_sites.yaml with", len(sites), "sites.")
        except Exception as e:
            print("YAML load error:", e); sites=[]

        all_prices=[]
        for site in sites:
            if time_left() <= 3:
                print("Timebox reached before next site."); break
            print(f"Crawling: {site.get('name')} (time left ≈ {int(time_left())}s)")
            items = crawl_site(site, deadline=deadline)
            print(f" -> {len(items)} products from {site.get('name')}")
            all_prices.extend(items); time.sleep(0.2)

        if all_prices:
            store_prices(conn, all_prices)
            print(f"Stored {len(all_prices)} price rows.")
        else:
            print("No prices found this run.")

        cheapest = get_cheapest(conn, top_n=12)
        movers   = get_movers(conn)
        print(f"Digest sections -> cheapest:{len(cheapest)} movers:{len(movers)}")

        html = render_html(cheapest, movers)
        subject = f"Daily Caviar Digest — {datetime.utcnow():%b %d, %Y}"
        print("Sending email to:", TO_EMAIL)
        send_email_html(subject, html)
        print("Done. Total runtime ~", int((datetime.utcnow() - start).total_seconds()), "seconds")
    except Exception as e:
        import traceback
        print("FATAL ERROR:", e); print(traceback.format_exc())

if __name__ == "__main__":
    main()
