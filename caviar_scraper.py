import os, re, json, time, yaml, sqlite3, xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode
import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

# =====================================================
# CONFIG
# =====================================================
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = os.getenv("DB_PATH", str(BASE_DIR / "caviar_agent.db"))

RUN_LIMIT_SECONDS   = int(os.getenv("RUN_LIMIT_SECONDS", "240"))
MAX_LINKS_PER_SITE  = int(os.getenv("MAX_LINKS_PER_SITE", "500"))
MAX_PAGES_PER_SITE  = int(os.getenv("MAX_PAGES_PER_SITE", "160"))
MAX_PRODUCTS_PER_SITE = int(os.getenv("MAX_PRODUCTS_PER_SITE", "220"))
VERBOSE_LOG         = os.getenv("VERBOSE_LOG", "1") == "1"

# Size rules
MIN_TIN_G          = 28   # hard minimum to accept any product
FOR2_MIN_G         = 28
FOR2_MAX_G         = 50
FOR4_MAX_G         = 110
SPECIALS_MAX_G     = 260

# =====================================================
# CONSTANTS & REGEX
# =====================================================
CAVIAR_WORD = re.compile(r"\bcaviar\b", re.I)
SIZE_RE     = re.compile(r"(\d+(?:\.\d+)?)\s*(g|gram|grams|oz|ounce|ounces)\b", re.I)
MONEY_RE    = re.compile(r"([$\£\€])\s*([0-9]+(?:\.[0-9]{1,2})?)")
SOLD_OUT_RE = re.compile(r"\bsold\s*out\b|\bout\s*of\s*stock\b", re.I)

LIKELY_TIN_SIZES_G = [28,30,50,57,85,100,113,125,180,200,250,500,1000]

GRADE_RANK = {"imperial":1,"royal":2,"reserve":3,"gold":3,"classic":4,"select":5,"traditional":6}

NON_STURGEON_TOKENS = [
    "salmon","trout","whitefish","capelin","lumpfish",
    "bowfin","paddlefish","tobiko","masago","ikura","smelt","cod roe"
]

# accessory filters — CAREFUL: no “class”
EXCLUDE_WORDS = [
    "gift set","giftset","set","bundle","sampler","flight","pairing","experience","kit",
    "accessory","accessories","spoon","mother of pearl","key","opener","tin opener",
    "blini","bellini","creme","crème","server","bowl","tray","plate","dish","cooler","chiller",
    "gift card","chips","tote","bag","club","subscription","subscribe","duo","trio","quad","pack",
    "tasting","pair","collection","assortment","starter"
]

PRODUCT_URL_HINTS = ("/products/", "/product/", "/shop/")
DISALLOWED_URL_PARTS = (
    "/cart","/account","/login","/checkout","/policy","/policies",
    "/privacy","/terms","/search","/faq","/returns","/shipping","/blog"
)

VENDOR_HOME_STATE = {
    "Marshallberg Farm":"NC","Sterling":"CA","Tsar Nicoulai":"CA",
    "Pearl Street Caviar":"NY","Marky's":"FL","Bemka / CaviarLover":"FL",
    "Island Creek":"MA","Browne Trading":"ME","Seattle Caviar":"WA",
    "Black River Caviar":"FL","The Caviar Co.":"CA","Petrossian USA":"NY",
    "Paramount Caviar":"NY","Imperia Caviar":"CA","OLMA Caviar":"NY",
    "Russ & Daughters":"NY","Zabars":"NY","Regiis Ova":"CA","Roe Caviar":"CA"
}

# =====================================================
# ACCESSORY CHECK
# =====================================================
def is_accessory_name_only(product_name):
    t = (product_name or "").lower()
    words = set(re.findall(r"[a-z]+", t))
    for w in EXCLUDE_WORDS:
        if " " in w:
            if w in t:
                return True
        else:
            if w in words:
                return True
    return False

# =====================================================
# HTTP SESSION
# =====================================================
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    })
    retries = Retry(total=4, backoff_factor=0.5,
                    status_forcelist=[403,429,500,502,503,504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://",  HTTPAdapter(max_retries=retries))
    return s

def safe_get(sess, url, timeout=20):
    try:
        return sess.get(url, timeout=timeout)
    except Exception as e:
        if VERBOSE_LOG: print("GET exception:", e, url)
        return None

# =====================================================
# HELPERS
# =====================================================
def parse_size(text):
    m = SIZE_RE.search(text or "")
    if not m: return None
    val, unit = float(m.group(1)), m.group(2).lower()
    return val * 28.3495 if unit.startswith("oz") else val

def size_label_both(g):
    if not g: return None
    oz = g/28.3495
    oz_str = f"{oz:.1f}".rstrip("0").rstrip(".")
    return f"{oz_str} oz / {int(round(g))} g"

def mentions_non_sturgeon(text):
    t=(text or "").lower()
    return any(re.search(rf"\b{re.escape(tok)}\b", t) for tok in NON_STURGEON_TOKENS)

def vendor_state(vendor): 
    return VENDOR_HOME_STATE.get(vendor,"US")

def extract_species(t):
    t = (t or "").lower()
    if "osetra" in t or "oscietra" in t: return "Osetra","Acipenser gueldenstaedtii"
    if "siberian" in t or "baerii" in t: return "Siberian","Acipenser baerii"
    if "kaluga" in t: return "Kaluga Hybrid","Huso dauricus × Acipenser schrenckii"
    if "white" in t and "sturgeon" in t: return "White Sturgeon","Acipenser transmontanus"
    if "sevruga" in t: return "Sevruga","Acipenser stellatus"
    if "beluga" in t or "huso huso" in t: return "Beluga","Huso huso"
    return None,None

def grade_from_text(text):
    t=(text or "").lower()
    for g,rank in GRADE_RANK.items():
        if re.search(rf"\b{re.escape(g)}\b", t):
            return g.title()
    return None

# =====================================================
# DB
# =====================================================
def init_db(path):
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("""CREATE TABLE IF NOT EXISTS prices(
        id INTEGER PRIMARY KEY,
        vendor TEXT,url TEXT,name TEXT,
        species TEXT,species_latin TEXT,grade TEXT,
        currency TEXT,price REAL,size_g REAL,size_label TEXT,
        per_g REAL,origin_state TEXT,seen_at TEXT)""")
    conn.commit()
    return conn

def store(conn, rows):
    if not rows: return
    cur = conn.cursor()
    for r in rows:
        cur.execute("""INSERT INTO prices(
            vendor,url,name,species,species_latin,grade,currency,price,
            size_g,size_label,per_g,origin_state,seen_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (r["vendor"],r["url"],r["name"],r["species"],r["species_latin"],
             r["grade"],r["currency"],r["price"],r["size_g"],r["size_label"],
             r["per_g"],r["origin_state"],datetime.utcnow().isoformat()))
    conn.commit()

def latest_best_by_vendor(conn):
    q = """
    WITH ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY vendor,url,size_g ORDER BY datetime(seen_at) DESC) rn
        FROM prices WHERE species IS NOT NULL AND species<>''
    )
    SELECT vendor,name,species,species_latin,grade,currency,MIN(price),size_g,size_label,MIN(per_g),url,origin_state
    FROM ranked WHERE rn=1
    GROUP BY vendor,url,name,species,species_latin,grade,size_g,size_label,origin_state,currency
    """
    rows = conn.execute(q).fetchall()
    out=[]
    seen=set()
    for v,n,s,lat,g,cur,p,sg,sl,pg,u,st in rows:
        key=(v,u,int(round(sg)),round(p or 0,2))
        if key in seen: continue
        seen.add(key)
        out.append({
            "vendor":v,"name":n,"species":s,"species_latin":lat,"grade":g,
            "currency":cur,"price":p,"size_g":sg,"size_label":sl,
            "per_g":pg,"url":u,"origin_state":st})
    return out

# =====================================================
# STRUCTURED DATA (for stock + variants)
# =====================================================
def extract_ld_offers_with_availability(soup):
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
                        avail = (o.get("availability") or "").lower()
                        norm.append({
                            "price": price,
                            "currency": o.get("priceCurrency") or "USD",
                            "name": o.get("name") or o.get("description") or "",
                            "sku": o.get("sku") or "",
                            "availability": avail
                        })
                items.append({"name":name,"desc":desc,"offers":norm})
    return items

# =====================================================
# PRODUCT SCRAPER
# =====================================================
def scrape_product(sess, url, vendor):
    r = safe_get(sess,url)
    if not r or not r.ok:
        if VERBOSE_LOG: print(f"[skip:{vendor}] bad url {url}")
        return []
    soup = BeautifulSoup(r.text,"lxml")
    text = soup.get_text(" ",strip=True)

    # Skip sold-out/out-of-stock
    if SOLD_OUT_RE.search(text):
        if VERBOSE_LOG: print(f"[skip:{vendor}] sold out: {url}")
        return []

    # Name/title
    title = (soup.title.string if soup.title else "") or ""
    h1 = soup.find("h1")
    name = (h1.get_text(" ",strip=True) if h1 else title).strip()

    # Must be caviar, not an accessory
    if is_accessory_name_only(name):
        if VERBOSE_LOG: print(f"[skip:{vendor}] accessory/gift: {url}")
        return []
    if not CAVIAR_WORD.search((name + " " + text).lower()):
        if VERBOSE_LOG: print(f"[skip:{vendor}] not caviar: {url}")
        return []

    # Reject non-sturgeon roe
    if mentions_non_sturgeon(text):
        if VERBOSE_LOG: print(f"[skip:{vendor}] non-sturgeon roe: {url}")
        return []

    # Species required
    species, latin = extract_species(name) or (None, None)
    if not species:
        species, latin = extract_species(text)
    if not species:
        if VERBOSE_LOG: print(f"[skip:{vendor}] species not found: {url}")
        return []

    # Size detection with minimum tin rule
    size_g = parse_size(name) or parse_size(text)
    if not size_g or size_g < MIN_TIN_G or not any(abs(size_g - s) <= 2 for s in LIKELY_TIN_SIZES_G+[MIN_TIN_G]):
        if VERBOSE_LOG: print(f"[skip:{vendor}] size < {MIN_TIN_G} g or not tin-like: {url}")
        return []

    # Price (prefer JSON-LD offers that are InStock)
    price = None
    currency = "USD"

    for item in extract_ld_offers_with_availability(soup):
        for offer in (item.get("offers") or []):
            avail = offer.get("availability","")
            if "instock" in avail or avail == "":
                if offer.get("price") not in (None,""):
                    price = float(offer["price"])
                    currency = offer.get("currency","USD")
                    break
        if price is not None:
            break

    # Fallback to first money on page
    if price is None:
        m = MONEY_RE.search(text)
        if m:
            price = float(m.group(2))
            currency = {'$':'USD','£':'GBP','€':'EUR'}.get(m.group(1),'USD')

    if price is None:
        if VERBOSE_LOG: print(f"[skip:{vendor}] no price found: {url}")
        return []

    grade = grade_from_text(name + " " + text)
    size_label = size_label_both(size_g)
    per_g = round(price/size_g, 2)

    return [{
        "vendor":vendor,"url":url,"name":name,
        "species":species,"species_latin":latin,"grade":grade,
        "currency":currency,"price":price,"size_g":size_g,
        "size_label":size_label,"per_g":per_g,"origin_state":vendor_state(vendor)
    }]

# =====================================================
# DISCOVERY (kept simple here – seeds or prior logic)
# =====================================================
def is_product_url(u):
    low = u.lower()
    if any(x in low for x in DISALLOWED_URL_PARTS): return False
    return any(h in low for h in PRODUCT_URL_HINTS)

def crawl_site(cfg, deadline):
    results=[]
    vendor=cfg.get("name")
    sess=make_session()

    # Seed product URLs (trusted)
    for u in list(cfg.get("seed_product_urls") or []):
        if datetime.utcnow()>deadline: break
        if not u: continue
        if not is_product_url(u): continue
        rows = scrape_product(sess, u, vendor)
        results.extend(rows)
        time.sleep(0.08)

    # Optionally, you can add deeper crawling here (category pages, sitemaps)
    return results

# =====================================================
# MAIN SCRAPE ENTRY
# =====================================================
def init_db_and_scrape(yaml_path="price_sites.yaml"):
    start=datetime.utcnow()
    deadline=start+timedelta(seconds=RUN_LIMIT_SECONDS)
    with open(yaml_path) as f:
        conf=yaml.safe_load(f) or {}
    sites=conf.get("sites",[])
    conn=init_db(DB_PATH)
    all_rows=[]
    for s in sites:
        if datetime.utcnow()>deadline: break
        all_rows+=crawl_site(s,deadline)
    if all_rows: store(conn,all_rows)
    return latest_best_by_vendor(conn)

# =====================================================
# GROUPING
# =====================================================
def bucket_for_size(g):
    if g is None or g < MIN_TIN_G:
        return None
    if FOR2_MIN_G <= g <= FOR2_MAX_G:
        return "For 2 (30–50 g)"
    if g <= FOR4_MAX_G:
        return "For 4 (~100 g)"
    if g <= SPECIALS_MAX_G:
        return "Specials (125–250 g)"
    return "Bulk (500 g+)"

def best_sort_key(item):
    rank = GRADE_RANK.get((item.get("grade") or "").lower(),99)
    return (rank,item.get("per_g",9999))

def group_and_pick(rows):
    goods = [r for r in rows if r.get("size_g") and r["size_g"] >= MIN_TIN_G]
    buckets={}
    for r in goods:
        b=bucket_for_size(r["size_g"])
        if not b: 
            continue
        buckets.setdefault(b,[]).append(r)
    top={}
    for b,it in buckets.items():
        top[b]=sorted(it,key=best_sort_key)[:6]
    return buckets,top

# =====================================================
# ALIAS (for main.py)
# =====================================================
def run_scrape(price_sites_yaml="price_sites.yaml"):
    return init_db_and_scrape(price_sites_yaml)
