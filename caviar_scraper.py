import os, re, json, time, yaml, sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

# -------------------------
# Config & paths
# -------------------------
BASE_DIR = Path(__file__).resolve().parent
RUN_LIMIT_SECONDS  = int(os.getenv("RUN_LIMIT_SECONDS", "150"))
MAX_LINKS_PER_SITE = int(os.getenv("MAX_LINKS_PER_SITE", "60"))
DB_PATH            = os.getenv("DB_PATH", str(BASE_DIR / "caviar_agent.db"))

# -------------------------
# Patterns & dictionaries
# -------------------------
CAVIAR_WORD = re.compile(r"\bcaviar\b", re.I)
SIZE_RE     = re.compile(r'(\d+(?:\.\d+)?)\s*(g|gram|grams|oz|ounce|ounces)\b', re.I)
MONEY_RE    = re.compile(r'([$\£\€])\s*([0-9]+(?:\.[0-9]{1,2})?)')
LIKELY_TIN_SIZES_G = [28,30,50,56,57,85,100,114,125,180,200,250,500,1000]

# Grade sort order (lower number = better)
GRADE_RANK = {
    "imperial": 1,
    "royal": 2,
    "reserve": 3,
    "gold": 3,
    "classic": 4,
    "select": 5,
    "traditional": 6,
}
def grade_rank(text):
    t=(text or "").lower()
    for g,rank in GRADE_RANK.items():
        if g in t:
            return rank, g.title()
    return 99, None

# Recognized sturgeon species (strict filter)
SPECIES_PATTERNS = [
    (r"\bbeluga\b|\bhuso\s*huso\b", "Beluga", "Huso huso"),
    (r"\bkaluga\b|\bhuso\s*dauricus\b", "Kaluga", "Huso dauricus"),
    (r"\bkaluga\s*hybrid\b|\b(amur|schrenckii).*(kaluga)|kaluga.*(amur|schrenckii)", "Kaluga Hybrid", "Huso dauricus × Acipenser schrenckii"),
    (r"\bamur\b|\bacipenser\s*schrenckii\b|\bschrenckii\b", "Amur", "Acipenser schrenckii"),
    (r"\bosc?ietr?a\b|\bossetra\b|\bgueldenstaedtii\b", "Osetra", "Acipenser gueldenstaedtii"),
    (r"\bsevruga\b|\bacipenser\s*stellatus\b|\bstellatus\b", "Sevruga", "Acipenser stellatus"),
    (r"\bsiberian\b|\bacipenser\s*baerii\b|\bbaerii\b", "Siberian", "Acipenser baerii"),
    (r"\bwhite\s*sturgeon\b|\bacipenser\s*transmontanus\b|\btransmontanus\b", "White Sturgeon", "Acipenser transmontanus"),
    (r"\bsterlet\b|\bacipenser\s*ruthenus\b|\bruthenus\b", "Sterlet", "Acipenser ruthenus"),
    (r"\bhackleback\b|\bshovelnose\b|\bscaphirhynchus\s*platorynchus\b", "Hackleback", "Scaphirhynchus platorynchus"),
]
NON_STURGEON_ROE = ["salmon roe","trout roe","whitefish roe","tobiko","masago","ikura","capelin","lumpfish","paddlefish","bowfin"]

EXCLUDE_WORDS = [
    "gift set","giftset","set","bundle","sampler","flight","pairing","experience","kit",
    "accessory","accessories","spoon","mother of pearl","key","opener","tin opener",
    "blini","bellini","creme","crème","server","bowl","tray","plate","dish","cooler","chiller",
    "gift card","chips","tote","bag","club","subscription","subscribe","duo","trio","quad","pack",
    "class","tasting","pair","collection","assortment","starter"
]

# Vendor → state (for proximity label)
VENDOR_HOME_STATE = {
    "Marshallberg Farm": "NC",
    "Island Creek": "MA",
    "Pearl Street Caviar": "NY",
    "Marky's": "FL",
    "Bemka / CaviarLover": "FL",
    "Sterling": "CA",
    "Tsar Nicoulai": "CA",
}

# -------------------------
# HTTP session
# -------------------------
def session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    })
    retries = Retry(total=3, backoff_factor=0.4, status_forcelist=[429,500,502,503,504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://",  HTTPAdapter(max_retries=retries))
    return s
SESSION = session()

def safe_get(url, timeout=20):
    try:
        return SESSION.get(url, timeout=timeout, allow_redirects=True)
    except Exception as e:
        print("GET exception:", e, url)
        return None

# -------------------------
# Helpers
# -------------------------
def parse_size(text):
    m = SIZE_RE.search(text or "")
    if not m: return None
    val, unit = float(m.group(1)), m.group(2).lower()
    grams = val * 28.3495 if unit.startswith("oz") else val
    return grams

def size_label_both(size_g):
    if not size_g: return None
    g = int(round(size_g))
    oz = size_g/28.3495
    oz_str = str(int(round(oz))) if abs(oz-round(oz))<0.05 else f"{oz:.1f}".rstrip("0").rstrip(".")
    return f"{oz_str} oz / {g} g"

def is_accessory(text):
    t=(text or "").lower()
    return any(w in t for w in EXCLUDE_WORDS)

def is_non_sturgeon(text):
    t=(text or "").lower()
    return any(w in t for w in NON_STURGEON_ROE)

def extract_species(text):
    t=(text or "").lower()
    for pat, common, latin in SPECIES_PATTERNS:
        if re.search(pat, t, re.I):
            return common, latin
    return None, None

def looks_like_product_url(u_low):
    return ("/product" in u_low) or ("/products/" in u_low) or ("/collections/" in u_low and "/products/" in u_low) or ("/shop/" in u_low and "/category/" not in u_low)

def vendor_state(vendor_name):
    return VENDOR_HOME_STATE.get(vendor_name, "US")

# -------------------------
# SQLite (simple cache/history)
# -------------------------
def init_db(path):
    conn = sqlite3.connect(path, check_same_thread=False)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS prices(
        id INTEGER PRIMARY KEY,
        vendor TEXT, url TEXT, name TEXT,
        species TEXT, species_latin TEXT,
        grade TEXT,
        currency TEXT, price REAL,
        size_g REAL, size_label TEXT,
        per_g REAL, origin_state TEXT,
        seen_at TEXT
    )""")
    conn.commit()
    return conn

def store(conn, rows):
    if not rows: return
    c = conn.cursor()
    for r in rows:
        c.execute("""INSERT INTO prices(vendor,url,name,species,species_latin,grade,currency,price,size_g,size_label,per_g,origin_state,seen_at)
                     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (r["vendor"], r["url"], r["name"], r["species"], r["species_latin"], r["grade"],
                   r["currency"], r["price"], r["size_g"], r["size_label"], r["per_g"],
                   r["origin_state"], datetime.utcnow().isoformat()))
    conn.commit()

def latest_best_by_vendor(conn):
    q = """
      WITH latest AS (
        SELECT vendor,url,name,species,species_latin,grade,currency,price,size_g,size_label,per_g,origin_state,seen_at,
               ROW_NUMBER() OVER (PARTITION BY vendor,url,size_g ORDER BY datetime(seen_at) DESC) rn
        FROM prices
        WHERE species IS NOT NULL AND species <> ''
      )
      SELECT vendor,name,species,species_latin,grade,currency,price,size_g,size_label,per_g,url,origin_state
      FROM latest WHERE rn=1
    """
    rows = conn.execute(q).fetchall()
    out=[]
    for v,n,sp,lat,gr,cur,p,sg,sl,pg,u,st in rows:
        out.append({"vendor":v,"name":n,"species":sp,"species_latin":lat,"grade":gr,"currency":cur,"price":p,
                    "size_g":sg,"size_label":sl,"per_g":pg,"url":u,"origin_state":st})
    return out

# -------------------------
# Parsing structured data
# -------------------------
def extract_ld_offers(soup):
    items=[]
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data=json.loads(tag.string or "{}")
        except Exception:
            continue
        data = data if isinstance(data, list) else [data]
        for it in data:
            if not isinstance(it, dict): continue
            if it.get("@type") in ("Product","Offer") or it.get("offers"):
                name = (it.get("name") or "").strip()
                desc = (it.get("description") or "")
                offers = it.get("offers")
                norm=[]
                if isinstance(offers, dict): offers=[offers]
                if isinstance(offers, list):
                    for o in offers:
                        if not isinstance(o, dict): continue
                        price = o.get("price")
                        if price not in (None,""):
                            try: price = float(price)
                            except: price = None
                        norm.append({
                            "price": price,
                            "currency": o.get("priceCurrency") or "USD",
                            "name": o.get("name") or o.get("description") or "",
                            "sku": o.get("sku") or ""
                        })
                items.append({"name":name,"desc":desc,"offers":norm})
    return items

def extract_shopify_variants(soup):
    results=[]
    for tag in soup.find_all("script", type="application/json"):
        txt=tag.string or ""
        if not txt or '"variants"' not in txt:
            continue
        try:
            data=json.loads(txt)
        except Exception:
            continue
        if isinstance(data, dict):
            vars=data.get("variants") or []
            for v in vars:
                title = (v.get("title") or "").strip()
                size_g = parse_size(title)
                price_cents = v.get("price")
                if size_g and price_cents is not None:
                    price = float(price_cents)/100.0
                    if any(abs(size_g - s) <= 2 for s in LIKELY_TIN_SIZES_G):
                        results.append({"title":title,"size_g":float(size_g),"price":price,"currency":"USD"})
    return results

# -------------------------
# Scrape one product page
# -------------------------
def scrape_product(url, vendor):
    r = safe_get(url)
    if not (r and r.ok):
        return []
    soup = BeautifulSoup(r.text, "lxml")
    text = soup.get_text(" ", strip=True)
    title = (soup.title.string if soup.title else "") or ""
    h1 = soup.find("h1")
    name = (h1.get_text(" ", strip=True) if h1 else title).strip()

    # must be actual caviar tin/jar
    if not (CAVIAR_WORD.search(name.lower()+ " "+ text.lower())): return []
    if is_accessory(name) or is_accessory(text): return []
    if is_non_sturgeon(name) or is_non_sturgeon(text): return []

    # species (required)
    species, latin = extract_species(name)
    if not species:
        species, latin = extract_species(text)
    if not species:
        # try LD blobs
        for tag in soup.find_all("script", type="application/ld+json"):
            try:
                blob = json.dumps(json.loads(tag.string or "{}"))
            except Exception:
                blob = ""
            s2, l2 = extract_species(blob)
            if s2:
                species, latin = s2, l2
                break
    if not species:
        return []  # filter out if species not stated

    gr_rank, grade_label = grade_rank(name + " " + text)

    out=[]
    # JSON-LD offers first
    for item in extract_ld_offers(soup):
        nm = item["name"] or name
        size_g = parse_size(nm + " " + item.get("desc","") + " " + text)
        offer = None
        if size_g and item["offers"]:
            # try to match size tokens in offer label/SKU
            tokens = size_tokens(size_g)
            cand = [o for o in item["offers"] if any(t in (o.get("name","")+" "+o.get("sku","")).lower() for t in tokens)]
            offer = cand[0] if cand else None
        if not offer and item["offers"]:
            priced=[o for o in item["offers"] if o.get("price")]
            offer = sorted(priced, key=lambda x:x["price"])[0] if priced else None
        if size_g and offer and offer.get("price"):
            sl = size_label_both(size_g)
            out.append({
                "vendor": vendor,
                "url": url,
                "name": nm,
                "species": species,
                "species_latin": latin,
                "grade": grade_label,
                "currency": offer.get("currency","USD"),
                "price": float(offer["price"]),
                "size_g": float(size_g),
                "size_label": sl,
                "per_g": float(offer["price"]) / float(size_g),
                "origin_state": vendor_state(vendor)
            })
            break

    # Shopify JSON fallback
    if not out:
        vars_ = extract_shopify_variants(soup)
        if vars_:
            for v in vars_:
                sl = size_label_both(v["size_g"])
                out.append({
                    "vendor": vendor, "url": url, "name": name,
                    "species": species, "species_latin": latin,
                    "grade": grade_label, "currency": v["currency"],
                    "price": v["price"], "size_g": v["size_g"], "size_label": sl,
                    "per_g": v["price"]/v["size_g"], "origin_state": vendor_state(vendor)
                })
    return out

def size_tokens(size_g):
    if not size_g: return set()
    g = int(round(size_g))
    oz = size_g/28.3495
    oz_round = round(oz, 1)
    tokens = {f"{g}g", f"{g} g"}
    if g in (28,29,30): tokens |= {"28g","29g","30g","28 g","29 g","30 g"}
    def ozs(x): return str(int(round(x))) if abs(x-round(x))<0.05 else f"{x:.1f}".rstrip('0').rstrip('.')
    for o in {oz_round, round(oz)}:
        s = ozs(o)
        tokens |= {f"{s}oz", f"{s} oz", f"{s} ounce", f"{s} ounces"}
    return {t.lower() for t in tokens}

# -------------------------
# Crawl a site config
# -------------------------
def crawl_site(site_cfg, deadline):
    results=[]
    vendor = site_cfg.get("name","Vendor")
    allow = set(site_cfg.get("allow_domains") or [])
    def domain_ok(u):
        host = urlparse(u).netloc.lower().replace("www.","")
        return (not allow) or (host in {d.lower().replace("www.","") for d in allow})

    # Seed product URLs (recommended for reliability)
    for seed in site_cfg.get("seed_product_urls", [])[:MAX_LINKS_PER_SITE]:
        if datetime.utcnow() > deadline: return results
        low = (seed or "").lower()
        if looks_like_product_url(low) and domain_ok(seed):
            results += scrape_product(seed, vendor)
            time.sleep(0.1)

    # Category pages → discover product links
    for start in site_cfg.get("start_urls", []):
        if datetime.utcnow() > deadline: return results
        r = safe_get(start)
        if not (r and r.ok): continue
        soup = BeautifulSoup(r.text, "lxml")
        links=set()

        sel = (site_cfg.get("selectors") or {}).get("product_link")
        if sel:
            for a in soup.select(sel):
                href = a.get("href")
                if not href: continue
                full = urljoin(start, href)
                if domain_ok(full) and looks_like_product_url(full.lower()):
                    links.add(full)

        for a in soup.find_all("a", href=True):
            full = urljoin(start, a["href"])
            if domain_ok(full) and looks_like_product_url(full.lower()):
                links.add(full)

        for u in list(links)[:MAX_LINKS_PER_SITE]:
            if datetime.utcnow() > deadline: break
            results += scrape_product(u, vendor)
            time.sleep(0.1)

    return results

# -------------------------
# Public API
# -------------------------
def run_scrape(price_sites_yaml="price_sites.yaml"):
    start = datetime.utcnow()
    deadline = start + timedelta(seconds=RUN_LIMIT_SECONDS)
    yaml_path = Path(price_sites_yaml)
    if not yaml_path.is_file():
        yaml_path = BASE_DIR / "price_sites.yaml"
    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f) or {}
    sites = config.get("sites", [])
    conn = init_db(DB_PATH)
    all_rows=[]
    for site in sites:
        if datetime.utcnow() > deadline:
            break
        rows = crawl_site(site, deadline)
        all_rows.extend(rows)
    if all_rows:
        store(conn, all_rows)
    return latest_best_by_vendor(conn)

# -------------------------
# Grouping for email
# -------------------------
def bucket_for_size(g):
    if g is None: return "Other"
    if g <= 50:   return "For 2 (30–50 g)"
    if g <= 110:  return "For 4 (~100 g)"
    if g <= 260:  return "Specials (125–250 g)"
    return "Bulk (500 g+)"

def best_sort_key(item):
    # grade → $/g → vendor alpha
    rank = GRADE_RANK.get((item.get("grade") or "").lower(), 99)
    per_g = item.get("per_g") or 1e9
    vendor = item.get("vendor","")
    return (rank, per_g, vendor)

def group_and_pick(rows):
    goods = [r for r in rows if r.get("size_g") and r.get("species")]
    buckets = {}
    for r in goods:
        b = bucket_for_size(r["size_g"])
        buckets.setdefault(b, []).append(r)
    top_picks = {}
    for b, items in buckets.items():
        items_sorted = sorted(items, key=best_sort_key)
        top_picks[b] = items_sorted[:6]
    return buckets, top_picks
