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
RUN_LIMIT_SECONDS     = int(os.getenv("RUN_LIMIT_SECONDS", "240"))
MAX_LINKS_PER_SITE    = int(os.getenv("MAX_LINKS_PER_SITE", "500"))
MAX_PAGES_PER_SITE    = int(os.getenv("MAX_PAGES_PER_SITE", "160"))
MAX_PRODUCTS_PER_SITE = int(os.getenv("MAX_PRODUCTS_PER_SITE", "220"))
DB_PATH               = os.getenv("DB_PATH", str(BASE_DIR / "caviar_agent.db"))
VERBOSE_LOG           = os.getenv("VERBOSE_LOG", "1") == "1"

HTTP_PROXY  = os.getenv("HTTP_PROXY")
HTTPS_PROXY = os.getenv("HTTPS_PROXY")

# =========================
# Patterns & dictionaries
# =========================
CAVIAR_WORD = re.compile(r"\bcaviar\b", re.I)
SIZE_RE     = re.compile(r'(\d+(?:\.\d+)?)\s*(g|gram|grams|oz|ounce|ounces)\b', re.I)
MONEY_RE    = re.compile(r'([$\£\€])\s*([0-9]+(?:\.[0-9]{1,2})?)')

LIKELY_TIN_SIZES_G = [28,30,50,56,57,85,100,113,114,125,180,200,250,500,1000]
PREFERRED_SIZES_G  = [28,30,50,100,113,114,125,250]  # choose these first if multiple

GRADE_RANK = {
    "imperial": 1, "royal": 2, "reserve": 3, "gold": 3,
    "classic": 4, "select": 5, "traditional": 6,
}
def grade_rank(text):
    t = (text or "").lower()
    for g, rank in GRADE_RANK.items():
        if re.search(rf"\b{re.escape(g)}\b", t):
            return rank, g.title()
    return 99, None

SPECIES_PATTERNS = [
    (r"\bbeluga\b|\bhuso\s*huso\b", "Beluga", "Huso huso"),
    (r"\bkaluga\b|\bhuso\s*dauricus\b|\bkeluga\b", "Kaluga", "Huso dauricus"),
    (r"\bkaluga\s*hybrid\b|\b(amur|schrenckii).*(kaluga|keluga)|(kaluga|keluga).*(amur|schrenckii)", "Kaluga Hybrid", "Huso dauricus × Acipenser schrenckii"),
    (r"\bamur\b|\bacipenser\s*schrenckii\b|\bschrenckii\b", "Amur", "Acipenser schrenckii"),
    (r"\bosc?ietr?a\b|\bossetra\b|\bgueldenstaedtii\b", "Osetra", "Acipenser gueldenstaedtii"),
    (r"\bsevruga\b|\bacipenser\s*stellatus\b|\bstellatus\b", "Sevruga", "Acipenser stellatus"),
    (r"\bsiberian\b|\bacipenser\s*baerii\b|\bbaerii\b", "Siberian", "Acipenser baerii"),
    (r"\bwhite\s*sturgeon\b|\bacipenser\s*transmontanus\b|\btransmontanus\b", "White Sturgeon", "Acipenser transmontanus"),
    (r"\bsterlet\b|\bacipenser\s*ruthenus\b|\bruthenus\b", "Sterlet", "Acipenser ruthenus"),
    (r"\bhackleback\b|\bshovelnose\b|\bscaphirhynchus\s*platorynchus\b", "Hackleback", "Scaphirhynchus platorynchus"),
]
NON_STURGEON_TOKENS = [
    "salmon", "trout", "whitefish", "capelin", "lumpfish", "bowfin", "paddlefish",
    "tobiko", "masago", "ikura", "smelt", "cod roe"
]

ACCESSORY_TOKENS = [
    "gift", "set", "bundle", "sampler", "flight", "pairing", "experience", "kit",
    "accessory", "accessories", "spoon", "opener",
    "blini", "crème fraîche", "creme fraiche", "server", "bowl", "tray", "plate", "dish",
    "cooler", "chiller", "gift card", "chips", "tote", "bag", "club", "subscription",
    "duo", "trio", "quad", "pack", "collection", "assortment", "starter", "flight"
]
ACCESSORY_RE = re.compile(r"|".join(rf"\b{re.escape(tok)}\b" for tok in ACCESSORY_TOKENS), re.I)

PRODUCT_URL_HINTS = ("/products/", "/product/", "/shop/")
DISALLOWED_URL_PARTS = (
    "/cart", "/account", "/login", "/checkout", "/policy", "/policies",
    "/privacy", "/terms", "/search", "/page-not-found", "/contact", "/faq", "/returns", "/shipping", "/blog"
)

VENDOR_HOME_STATE = {
    "Marshallberg Farm": "NC",
    "Island Creek": "MA",
    "Pearl Street Caviar": "NY",
    "Marky's": "FL",
    "Bemka / CaviarLover": "FL",
    "Sterling": "CA",
    "Tsar Nicoulai": "CA",
}

# Vendor-specific rules
ALLOW_BELUGA_DOMAINS = set()  # empty = never accept true Beluga by token alone
VENDOR_SPECIES_CANON = {
    # If vendor markets “Beluga” branding but it’s actually a hybrid,
    # normalize it so species is accurate for comparison.
    "Pearl Street Caviar": {"Beluga": ("Kaluga Hybrid", "Huso dauricus × Acipenser schrenckii")},
}

# =========================
# HTTP sessions
# =========================
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
    })
    retries = Retry(
        total=4, backoff_factor=0.6,
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://",  HTTPAdapter(max_retries=retries))
    if HTTP_PROXY or HTTPS_PROXY:
        s.proxies.update({
            "http": HTTP_PROXY or HTTPS_PROXY,
            "https": HTTPS_PROXY or HTTP_PROXY
        })
    return s

def site_session():
    return make_session()

def safe_get(sess, url, referer=None, timeout=20):
    try:
        headers = {}
        if referer: headers["Referer"] = referer
        return sess.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    except Exception as e:
        if VERBOSE_LOG: print("GET exception:", e, url)
        return None

# =========================
# Helpers
# =========================
def norm_host(h): return (h or "").lower().replace("www.","")
def same_domain(u, allow_set):
    host = norm_host(urlparse(u).netloc)
    return (not allow_set) or (host in allow_set)

def tidy_url(u):
    try:
        pr = urlparse(u)
        q = parse_qs(pr.query)
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

def is_accessory_name_only(product_name):
    t=(product_name or "").lower()
    return bool(ACCESSORY_RE.search(t))

def mentions_non_sturgeon(text):
    t=(text or "").lower()
    return any(re.search(rf"\b{re.escape(tok)}\b", t) for tok in NON_STURGEON_TOKENS)

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

def looks_sold_out(soup, page_text, availability=None, variant_available=None):
    if availability:
        if isinstance(availability, str) and "instock" not in availability.lower():
            return True
        if isinstance(availability, bool) and availability is False:
            return True
    if variant_available is False:
        return True
    t = (page_text or "").lower()
    if "sold out" in t or "out of stock" in t or "currently unavailable" in t:
        return True
    for b in soup.find_all(["button","a"]):
        txt = (b.get_text(" ", strip=True) or "").lower()
        if "sold out" in txt:
            return True
    return False

def sizes_from_offers_and_variants(soup):
    sizes=set()
    # JSON-LD offers
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data=json.loads(tag.string or "{}")
        except Exception:
            continue
        for it in (data if isinstance(data,list) else [data]):
            if not isinstance(it, dict): continue
            offers = it.get("offers")
            cand=[]
            if isinstance(offers, dict): cand=[offers]
            elif isinstance(offers, list): cand=[o for o in offers if isinstance(o,dict)]
            for o in cand:
                txt = " ".join([o.get("name",""), o.get("description",""), o.get("sku","")])
                sg = parse_size(txt)
                if sg and any(abs(sg - s)<=2 for s in LIKELY_TIN_SIZES_G):
                    sizes.add(int(round(sg)))
    # Shopify variants
    for tag in soup.find_all("script", type="application/json"):
        txt=tag.string or ""
        if '"variants"' not in txt: continue
        try:
            data=json.loads(txt)
        except Exception:
            continue
        if isinstance(data, dict):
            for v in (data.get("variants") or []):
                sg = parse_size(v.get("title",""))
                if sg and any(abs(sg - s)<=2 for s in LIKELY_TIN_SIZES_G):
                    sizes.add(int(round(sg)))
    ordered = sorted(sizes, key=lambda x: (x not in PREFERRED_SIZES_G, PREFERRED_SIZES_G.index(x) if x in PREFERRED_SIZES_G else 999, x))
    return ordered

# =========================
# SQLite
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
    cur = conn.cursor()
    for r in rows:
        cur.execute("""INSERT INTO prices(
            vendor,url,name,species,species_latin,grade,currency,price,size_g,size_label,per_g,origin_state,seen_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (r["vendor"], r["url"], r["name"], r["species"], r["species_latin"], r["grade"],
         r["currency"], r["price"], r["size_g"], r["size_label"], r["per_g"],
         r["origin_state"], datetime.utcnow().isoformat()))
    conn.commit()

def latest_best_by_vendor(conn):
    # pick the latest row per (vendor,url,size_g) then keep the CHEAPEST price among the latest seen per product/size
    q = """
      WITH latest AS (
        SELECT vendor,url,name,species,species_latin,grade,currency,price,size_g,size_label,per_g,origin_state,seen_at,
               ROW_NUMBER() OVER (PARTITION BY vendor,url,size_g ORDER BY datetime(seen_at) DESC) rn
        FROM prices
        WHERE species IS NOT NULL AND species <> ''
      ),
      dedup AS (
        SELECT vendor,url,name,species,species_latin,grade,currency,
               MIN(price) AS price, size_g, size_label,
               MIN(per_g) AS per_g, origin_state
        FROM latest
        WHERE rn=1
        GROUP BY vendor,url,name,species,species_latin,grade,size_g,size_label,origin_state,currency
      )
      SELECT vendor,name,species,species_latin,grade,currency,price,size_g,size_label,per_g,url,origin_state
      FROM dedup
    """
    rows = conn.execute(q).fetchall()
    out=[]
    seen=set()
    for v,n,sp,lat,gr,cur,p,sg,sl,pg,u,st in rows:
        key=(v,u,int(round(sg)),round(p,2))
        if key in seen: continue
        seen.add(key)
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
                        # Try to sniff availability booleans as well
                        avail = o.get("availability") or o.get("itemAvailability") or o.get("availabilityStarts")
                        norm.append({
                            "price": price,
                            "currency": o.get("priceCurrency") or "USD",
                            "name": o.get("name") or o.get("description") or "",
                            "sku": o.get("sku") or "",
                            "availability": avail
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
                available = v.get("available", True)
                if size_g and price_cents is not None:
                    price = float(price_cents)/100.0
                    if any(abs(size_g - s) <= 2 for s in LIKELY_TIN_SIZES_G):
                        results.append({
                            "title":title,"size_g":float(size_g),
                            "price":price,"currency":"USD",
                            "available": bool(available)
                        })
    return results

# =========================
# Product page scraper
# =========================
def _canonicalize_species(vendor, species, latin, url):
    # Optional: block “Beluga” unless explicitly allowed
    host = norm_host(urlparse(url).netloc)
    if species == "Beluga" and host not in ALLOW_BELUGA_DOMAINS:
        # If vendor has a mapping, apply it. Else drop species (which will skip row).
        mapped = VENDOR_SPECIES_CANON.get(vendor, {}).get("Beluga")
        if mapped:
            return mapped[0], mapped[1]
        return None, None
    # Vendor-specific canonicalization
    canon = VENDOR_SPECIES_CANON.get(vendor, {}).get(species)
    if canon:
        return canon[0], canon[1]
    return species, latin

def scrape_product(sess, url, vendor, referer=None, default_species=None):
    r = safe_get(sess, url, referer=referer)
    if not (r and r.ok):
        if VERBOSE_LOG: print(f"[skip:{vendor}] GET failed: {url}")
        return []
    soup = BeautifulSoup(r.text, "lxml")

    title = (soup.title.string if soup.title else "") or ""
    h1 = soup.find("h1")
    name = (h1.get_text(" ", strip=True) if h1 else title).strip()

    # page text (meta + body)
    metas = []
    for meta in soup.select("meta[property='og:title'],meta[name='og:title'],meta[name='twitter:title'],meta[name='description'],meta[property='og:description']"):
        c = meta.get("content") or ""
        if c: metas.append(c)
    page_text = " ".join([name] + metas + [soup.get_text(" ", strip=True)])

    # accessories rejected by NAME
    if is_accessory_name_only(name):
        if VERBOSE_LOG: print(f"[skip:{vendor}] accessory/gift by name: {url}")
        return []

    # reject if mentions non-sturgeon anywhere
    if mentions_non_sturgeon(page_text):
        if VERBOSE_LOG: print(f"[skip:{vendor}] mentions non-sturgeon: {url}")
        return []

    # require “caviar” OR recognized species; then canonicalize
    species, latin = extract_species(name)
    if not species:
        species, latin = extract_species(page_text)
    if not species and default_species:
        s2, l2 = extract_species(default_species)
        if s2: species, latin = s2, l2
    if not (species or CAVIAR_WORD.search(page_text.lower())):
        if VERBOSE_LOG: print(f"[skip:{vendor}] not caviar / no species: {url}")
        return []
    if not species:
        if VERBOSE_LOG: print(f"[skip:{vendor}] species not found: {url}")
        return []
    species, latin = _canonicalize_species(vendor, species, latin, url)
    if not species:
        if VERBOSE_LOG: print(f"[skip:{vendor}] species blocked/could not canonicalize: {url}")
        return []

    grade_label = grade_from_text(name + " " + page_text)

    # size from text, else from offers/variants
    size_g = parse_size(name) or parse_size(page_text)
    derived_sizes = []
    if not size_g:
        derived_sizes = sizes_from_offers_and_variants(soup)
        size_g = (derived_sizes[0] if derived_sizes else None)

    if not size_g or not any(abs(size_g - s) <= 2 for s in LIKELY_TIN_SIZES_G):
        if VERBOSE_LOG: print(f"[skip:{vendor}] no plausible tin/jar size: {url}")
        return []

    out=[]

    # JSON-LD offers — match variant by size tokens; require InStock and price>0
    for item in extract_ld_offers(soup):
        nm = item["name"] or name
        offer = None
        if item["offers"]:
            tokens = size_tokens(size_g)
            cand = [o for o in item["offers"] if any(t in (o.get("name","")+" "+o.get("sku","")).lower() for t in tokens)]
            offer = cand[0] if cand else None
        if not offer and item["offers"]:
            priced=[o for o in item["offers"] if o.get("price")]
            offer = sorted(priced, key=lambda x:x["price"])[0] if priced else None

        if offer:
            avail = offer.get("availability","")
            price = offer.get("price")
            if price and price > 0 and not looks_sold_out(soup, page_text, availability=avail):
                sl = size_label_both(size_g)
                per_g = round(float(price)/float(size_g), 2)
                out.append({
                    "vendor": vendor, "url": url, "name": nm,
                    "species": species, "species_latin": latin,
                    "grade": grade_label, "currency": offer.get("currency","USD"),
                    "price": float(price), "size_g": float(size_g), "size_label": sl,
                    "per_g": per_g, "origin_state": vendor_state(vendor)
                })
                break

    # Shopify variants fallback; require available=true and price>0
    if not out:
        vars_ = extract_shopify_variants(soup)
        if vars_:
            preferred_sizes = [int(round(size_g))] + derived_sizes if derived_sizes else [int(round(size_g))]
            match = None
            tokens = size_tokens(size_g)
            for v in vars_:
                t = (v["title"] or "").lower()
                if any(tok in t for tok in tokens):
                    match = v; break
            if not match:
                cand = sorted([v for v in vars_ if v.get("available")],
                              key=lambda v: (abs(v["size_g"]-size_g), v["price"]))
                match = cand[0] if cand else None
            if match and match.get("available") and (match.get("price",0) > 0):
                sg = match["size_g"]
                sl = size_label_both(sg)
                per_g = round(match["price"]/sg, 2)
                out.append({
                    "vendor": vendor, "url": url, "name": name,
                    "species": species, "species_latin": latin,
                    "grade": grade_label, "currency": match["currency"],
                    "price": float(match["price"]), "size_g": float(sg), "size_label": sl,
                    "per_g": per_g, "origin_state": vendor_state(vendor)
                })

    # Last resort: inline price, only if not sold out and >0
    if not out:
        m_price = MONEY_RE.search(page_text)
        if m_price:
            cur = {'$':'USD','£':'GBP','€':'EUR'}.get(m_price.group(1),'USD')
            price = float(m_price.group(2))
            if price > 0 and not looks_sold_out(soup, page_text):
                sl = size_label_both(size_g)
                per_g = round(price/size_g, 2)
                out.append({
                    "vendor": vendor, "url": url, "name": name,
                    "species": species, "species_latin": latin,
                    "grade": grade_label, "currency": cur,
                    "price": price, "size_g": size_g, "size_label": sl,
                    "per_g": per_g, "origin_state": vendor_state(vendor)
                })

    # sanity filters
    cleaned=[]
    for r in out:
        if r["per_g"] <= 0 or r["per_g"] > 100:
            continue
        cleaned.append(r)

    if not cleaned and VERBOSE_LOG:
        print(f"[skip:{vendor}] no valid in-stock price/size match: {url}")
    return cleaned

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
    return any(x in low for x in ("/collections/", "/collection/", "/category/", "/categories/", "/caviar", "/shop", "/products"))

def discover_bfs(sess, start_urls, allow_set, deadline):
    seen = set()
    queue = []
    out_product_links = set()

    for s in (start_urls or []):
        if not s: continue
        u = tidy_url(s)
        if same_domain(u, allow_set):
            queue.append(u); seen.add(u)

    pages_crawled = 0
    while queue and pages_crawled < MAX_PAGES_PER_SITE and datetime.utcnow() < deadline:
        cur = queue.pop(0)
        r = safe_get(sess, cur)
        if not (r and r.ok):
            continue
        soup = BeautifulSoup(r.text, "lxml")
        pages_crawled += 1

        links = set()
        for a in soup.find_all("a", href=True):
            u = urljoin(cur, a["href"])
            u = tidy_url(u)
            if not same_domain(u, allow_set): continue
            if u in seen: continue
            if any(x in u.lower() for x in DISALLOWED_URL_PARTS): continue
            if is_product_url(u):
                out_product_links.add(u)
                seen.add(u)
                continue
            if is_category_like(u):
                links.add(u)

        # rel="next"
        next_rel = soup.find("link", rel=lambda x: x and "next" in x.lower())
        if next_rel and next_rel.get("href"):
            u = tidy_url(urljoin(cur, next_rel["href"]))
            if same_domain(u, allow_set) and u not in seen:
                links.add(u)

        # "Next" text links
        for a in soup.find_all("a", string=re.compile(r'^\s*next\s*$', re.I)):
            u = tidy_url(urljoin(cur, a.get("href") or ""))
            if u and same_domain(u, allow_set) and u not in seen:
                links.add(u)

        for u in links:
            if len(seen) >= MAX_LINKS_PER_SITE: break
            queue.append(u); seen.add(u)

    return list(out_product_links)[:MAX_PRODUCTS_PER_SITE]

def discover_from_sitemap(sess, base_url, allow_set, deadline):
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
            r = safe_get(sess, sm, timeout=10)
            if not (r and r.ok and r.headers.get("content-type","").startswith(("application/xml","text/xml"))):
                continue
            try:
                tree = ET.fromstring(r.text)
            except Exception:
                continue
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

    allow = {norm_host(d) for d in (site_cfg.get("allow_domains") or [])}
    seed_urls = list(site_cfg.get("seed_product_urls") or [])
    start_urls = list(site_cfg.get("start_urls") or [])
    selectors = (site_cfg.get("selectors") or {})
    product_link_sel = selectors.get("product_link")
    default_species = site_cfg.get("default_species")

    sess = site_session()

    # Seeds (trusted product URLs)
    product_links = set()
    for seed in seed_urls:
        if datetime.utcnow() >= deadline: break
        if not seed: continue
        u = tidy_url(seed)
        if same_domain(u, allow) and is_product_url(u):
            product_links.add(u)

    # BFS crawl categories
    if datetime.utcnow() < deadline and start_urls:
        product_links.update(discover_bfs(sess, start_urls, allow, deadline))

    # Sitemaps
    if datetime.utcnow() < deadline and start_urls:
        product_links.update(discover_from_sitemap(sess, start_urls[0], allow, deadline))

    # product_link selector sweep (first page only)
    for start in start_urls:
        if datetime.utcnow() >= deadline: break
        r = safe_get(sess, start)
        if not (r and r.ok): continue
        if product_link_sel:
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.select(product_link_sel):
                href = a.get("href")
                if not href: continue
                u = tidy_url(urljoin(start, href))
                if same_domain(u, allow) and is_product_url(u):
                    product_links.add(u)

    product_list = list(product_links)[:MAX_PRODUCTS_PER_SITE]
    if VERBOSE_LOG:
        print(f"[{vendor}] discovered {len(product_list)} product URLs (seeds + crawl + sitemap)")

    kept=0
    referer_for_site = start_urls[0] if start_urls else None
    for u in product_list:
        if datetime.utcnow() >= deadline:
            if VERBOSE_LOG: print(f"[{vendor}] timebox hit during products")
            break
        rows = scrape_product(sess, u, vendor, referer=referer_for_site, default_species=default_species)
        results.extend(rows)
        if rows: kept += len(rows)
        time.sleep(0.06)

    if VERBOSE_LOG:
        print(f"[{vendor}] kept {kept} product rows")
    return results

# =========================
# Public API
# =========================
def init_db_and_scrape(price_sites_yaml="price_sites.yaml"):
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
        # de-dupe before storing
        unique = []
        seen=set()
        for r in all_rows:
            key=(r["vendor"], r["url"], int(round(r["size_g"])), round(r["price"],2))
            if key in seen: continue
            seen.add(key); unique.append(r)
        store(conn, unique)
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

# Back-compat alias
def run_scrape(price_sites_yaml="price_sites.yaml"):
    return init_db_and_scrape(price_sites_yaml)
