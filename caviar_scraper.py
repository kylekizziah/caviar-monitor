import os, re, json, time, yaml, sqlite3, xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

# =========================
# Config & paths
# =========================
BASE_DIR = Path(__file__).resolve().parent
RUN_LIMIT_SECONDS   = int(os.getenv("RUN_LIMIT_SECONDS", "180"))   # total timebox per run
MAX_LINKS_PER_SITE  = int(os.getenv("MAX_LINKS_PER_SITE", "400"))  # hard cap on discovered links per site
MAX_PAGES_PER_SITE  = int(os.getenv("MAX_PAGES_PER_SITE", "120"))  # cap on crawled category/listing pages
MAX_PRODUCTS_PER_SITE = int(os.getenv("MAX_PRODUCTS_PER_SITE", "180"))  # cap on scraped product pages per site
DB_PATH             = os.getenv("DB_PATH", str(BASE_DIR / "caviar_agent.db"))
VERBOSE_LOG         = os.getenv("VERBOSE_LOG", "1") == "1"         # print skip reasons

# =========================
# Patterns & dictionaries
# =========================
CAVIAR_WORD = re.compile(r"\bcaviar\b", re.I)
SIZE_RE     = re.compile(r'(\d+(?:\.\d+)?)\s*(g|gram|grams|oz|ounce|ounces)\b', re.I)
MONEY_RE    = re.compile(r'([$\£\€])\s*([0-9]+(?:\.[0-9]{1,2})?)')
LIKELY_TIN_SIZES_G = [28,30,50,56,57,85,100,114,125,180,200,250,500,1000]

# Grade priority (lower is “better”)
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

# Recognize product URL shapes (Shopify/Woo/etc.)
PRODUCT_URL_HINTS = (
    "/products/",          # Shopify canonical
    "/product/",           # Woo / generic
    "/shop/",              # many Woo sites use /shop/.../{product}
)
DISALLOWED_URL_PARTS = (
    "/cart", "/account", "/login", "/checkout", "/policy", "/policies",
    "/privacy", "/terms", "/search", "/pages/", "/page-not-found",
    "/contact", "/faq", "/returns", "/shipping", "/blog"
)

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

# =========================
# HTTP session
# =========================
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
        if VERBOSE_LOG: print("GET exception:", e, url)
        return None

# =========================
# Helpers
# =========================
def norm_host(h):
    return (h or "").lower().replace("www.","")

def same_domain(u, allow_set):
    host = norm_host(urlparse(u).netloc)
    return (not allow_set) or (host in allow_set)

def tidy_url(u):
    """Normalize URL (strip fragments, keep query that matters)."""
    try:
        pr = urlparse(u)
        q = parse_qs(pr.query)
        # keep page parameter; drop tracking
        keep = {}
        for k,v in q.items():
            kk = k.lower()
            if kk in ("page","p","filter","variant","size"):
                keep[k] = v
        new_q = urlencode({k:v[-1] for k,v in keep.items()})
        return urlunparse((pr.scheme, pr.netloc, pr.path, "", new_q, "")) or u
    except Exception:
        return u

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

def vendor_state(vendor_name):
    return VENDOR_HOME_STATE.get(vendor_name, "US")

def grade_from_text(text):
    _, g = grade_rank(text or "")
    return g

# =========================
# SQLite (cache/history)
# =========================
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

# =========================
# Structured data helpers
# =========================
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

# =========================
# Product page scraper
# =========================
def scrape_product(url, vendor, default_species=None):
    r = safe_get(url)
    if not (r and r.ok):
        if VERBOSE_LOG: print(f"[skip:{vendor}] GET failed: {url}")
        return []
    soup = BeautifulSoup(r.text, "lxml")

    # Full text surface
    texts = []
    title = (soup.title.string if soup.title else "") or ""
    texts.append(title)
    h1 = soup.find("h1")
    if h1:
        texts.append(h1.get_text(" ", strip=True))
    for meta in soup.select("meta[property='og:title'],meta[name='og:title'],meta[name='twitter:title'],meta[name='description'],meta[property='og:description']"):
        c = meta.get("content") or ""
        if c: texts.append(c)
    texts.append(soup.get_text(" ", strip=True))
    page_text = " ".join(t for t in texts if t).strip()

    name = (h1.get_text(" ", strip=True) if h1 else title).strip()

    # must be actual caviar tin/jar (exclude accessories, non-sturgeon)
    if not (CAVIAR_WORD.search((name + " " + page_text).lower())):
        if VERBOSE_LOG: print(f"[skip:{vendor}] not caviar: {url}")
        return []
    if is_accessory(name) or is_accessory(page_text):
        if VERBOSE_LOG: print(f"[skip:{vendor}] accessory/gift: {url}")
        return []
    if is_non_sturgeon(name) or is_non_sturgeon(page_text):
        if VERBOSE_LOG: print(f"[skip:{vendor}] non-sturgeon roe: {url}")
        return []

    # species (required; allow YAML default_species fallback)
    species, latin = extract_species(name)
    if not species:
        species, latin = extract_species(page_text)
    if not species and default_species:
        s2, l2 = extract_species(default_species)
        if s2:
            species, latin = s2, l2
    if not species:
        if VERBOSE_LOG: print(f"[skip:{vendor}] species not found: {url}")
        return []

    grade_label = grade_from_text(name + " " + page_text)
    out=[]

    # JSON-LD offers
    for item in extract_ld_offers(soup):
        nm = item["name"] or name
        size_g = parse_size(nm + " " + (item.get("desc","") or "") + " " + page_text)
        offer = None
        if size_g and item["offers"]:
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

    # Shopify variants fallback
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
        else:
            # meta price + detected size
            m_price = MONEY_RE.search(page_text)
            size_g = parse_size(name + " " + page_text)
            if m_price and size_g:
                cur = {'$':'USD','£':'GBP','€':'EUR'}.get(m_price.group(1),'USD')
                price = float(m_price.group(2))
                sl = size_label_both(size_g)
                out.append({
                    "vendor": vendor, "url": url, "name": name,
                    "species": species, "species_latin": latin,
                    "grade": grade_label, "currency": cur,
                    "price": price, "size_g": size_g, "size_label": sl,
                    "per_g": price/size_g, "origin_state": vendor_state(vendor)
                })

    if not out and VERBOSE_LOG:
        print(f"[skip:{vendor}] no price/size match: {url}")
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

# =========================
# Discovery (deep crawl)
# =========================
def is_product_url(u):
    low = u.lower()
    if any(x in low for x in DISALLOWED_URL_PARTS):
        return False
    return any(h in low for h in PRODUCT_URL_HINTS)

def is_category_like(u):
    low = u.lower()
    if any(x in low for x in DISALLOWED_URL_PARTS):
        return False
    # catch “/collections/…”, “/category/…”, “/caviar”, “/shop” listing pages
    return any(x in low for x in ("/collections/", "/collection/", "/category/", "/categories/", "/caviar", "/shop", "/products"))

def discover_bfs(start_urls, allow_set, deadline):
    """Breadth-first crawl category/listing pages, follow pagination."""
    seen = set()
    queue = []
    out_product_links = set()

    # seed queue
    for s in (start_urls or []):
        if not s: continue
        u = tidy_url(s)
        if same_domain(u, allow_set):
            queue.append(u); seen.add(u)

    pages_crawled = 0
    while queue and pages_crawled < MAX_PAGES_PER_SITE and datetime.utcnow() < deadline:
        cur = queue.pop(0)
        r = safe_get(cur)
        if not (r and r.ok):
            continue
        soup = BeautifulSoup(r.text, "lxml")
        pages_crawled += 1

        # collect links
        links = set()
        for a in soup.find_all("a", href=True):
            u = urljoin(cur, a["href"])
            u = tidy_url(u)
            if not same_domain(u, allow_set):
                continue
            if u in seen:
                continue
            if any(x in u.lower() for x in DISALLOWED_URL_PARTS):
                continue
            if is_product_url(u):
                out_product_links.add(u)
                seen.add(u)
                continue
            if is_category_like(u):
                links.add(u)

        # pagination hints
        next_rel = soup.find("link", rel=lambda x: x and "next" in x.lower())
        if next_rel and next_rel.get("href"):
            u = urljoin(cur, next_rel["href"])
            u = tidy_url(u)
            if same_domain(u, allow_set) and u not in seen:
                links.add(u)

        # also find “Next” buttons
        for a in soup.find_all("a", string=re.compile(r'^\s*next\s*$', re.I)):
            u = urljoin(cur, a.get("href") or "")
            if u:
                u = tidy_url(u)
                if same_domain(u, allow_set) and u not in seen:
                    links.add(u)

        # enqueue
        for u in links:
            if len(seen) >= MAX_LINKS_PER_SITE:
                break
            queue.append(u); seen.add(u)

    return list(out_product_links)[:MAX_PRODUCTS_PER_SITE]

def discover_from_sitemap(base_url, allow_set, deadline):
    """Try to read /sitemap.xml and pick product-like URLs quickly."""
    try:
        pr = urlparse(base_url)
        root = f"{pr.scheme}://{pr.netloc}"
        candidates = [f"{root}/sitemap.xml",
                      f"{root}/sitemap_index.xml",
                      f"{root}/product-sitemap.xml",
                      f"{root}/products-sitemap.xml"]
        found = set()
        for sm in candidates:
            if datetime.utcnow() >= deadline:
                break
            r = safe_get(sm, timeout=10)
            if not (r and r.ok and r.headers.get("content-type","").startswith(("application/xml","text/xml"))):
                continue
            try:
                tree = ET.fromstring(r.text)
            except Exception:
                continue
            # sitemap index or urlset
            for loc in tree.iter():
                if loc.tag.endswith("loc"):
                    u = tidy_url(loc.text or "")
                    if same_domain(u, allow_set) and is_product_url(u):
                        found.add(u)
            if found:
                break
        return list(found)[:MAX_PRODUCTS_PER_SITE]
    except Exception:
        return []

# =========================
# Crawl a site config
# =========================
def crawl_site(site_cfg, deadline):
    results=[]
    vendor = site_cfg.get("name", "Vendor")

    # Normalize YAML fields
    allow = {norm_host(d) for d in (site_cfg.get("allow_domains") or [])}
    seed_urls = list(site_cfg.get("seed_product_urls") or [])
    start_urls = list(site_cfg.get("start_urls") or [])
    selectors = (site_cfg.get("selectors") or {})
    product_link_sel = selectors.get("product_link")
    default_species = site_cfg.get("default_species")

    # 1) Seeds (exact products you trust)
    product_links = set()
    for seed in seed_urls:
        if datetime.utcnow() >= deadline: break
        if not seed: continue
        u = tidy_url(seed)
        if same_domain(u, allow) and is_product_url(u):
            product_links.add(u)

    # 2) Category/collection BFS (deep crawl)
    if datetime.utcnow() < deadline and start_urls:
        discovered = discover_bfs(start_urls, allow, deadline)
        product_links.update(discovered)

    # 3) Site map discovery (fast catch-all)
    if datetime.utcnow() < deadline and start_urls:
        # use first start as base
        discovered_sm = discover_from_sitemap(start_urls[0], allow, deadline)
        product_links.update(discovered_sm)

    # 4) If site provided a product_link selector, scan those pages directly too
    for start in start_urls:
        if datetime.utcnow() >= deadline: break
        r = safe_get(start)
        if not (r and r.ok): continue
        if product_link_sel:
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.select(product_link_sel):
                href = a.get("href"); 
                if not href: continue
                u = tidy_url(urljoin(start, href))
                if same_domain(u, allow) and is_product_url(u):
                    product_links.add(u)

    # Cap total products per site
    product_list = list(product_links)[:MAX_PRODUCTS_PER_SITE]
    if VERBOSE_LOG:
        print(f"[{vendor}] discovered {len(product_list)} product URLs (seeds + crawl + sitemap)")

    kept=0
    for u in product_list:
        if datetime.utcnow() >= deadline:
            if VERBOSE_LOG: print(f"[{vendor}] timebox hit during products")
            break
        rows = scrape_product(u, vendor, default_species=default_species)
        results.extend(rows)
        if rows: kept += len(rows)
        time.sleep(0.06)

    if VERBOSE_LOG:
        print(f"[{vendor}] kept {kept} product rows")
    return results

# =========================
# Public API
# =========================
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
            if VERBOSE_LOG: print("Run timebox reached before next site.")
            break
        rows = crawl_site(site, deadline)
        all_rows.extend(rows)
    if all_rows:
        store(conn, all_rows)
    return latest_best_by_vendor(conn)

# =========================
# Grouping for email
# =========================
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
