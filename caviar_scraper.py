import os, re, json, time, yaml, sqlite3
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

# ---------- config ----------
RUN_LIMIT_SECONDS  = int(os.getenv("RUN_LIMIT_SECONDS", "150"))
MAX_LINKS_PER_SITE = int(os.getenv("MAX_LINKS_PER_SITE", "60"))
DB_PATH            = os.getenv("DB_PATH", "caviar_agent.db")

# Home base for proximity scoring (Athens, GA)
HOME_STATE = "GA"

# ---------- regex / dictionaries ----------
CAVIAR_WORD = re.compile(r"\bcaviar\b", re.I)
SIZE_RE     = re.compile(r'(\d+(?:\.\d+)?)\s*(g|gram|grams|oz|ounce|ounces)\b', re.I)
MONEY_RE    = re.compile(r'([$\£\€])\s*([0-9]+(?:\.[0-9]{1,2})?)')
LIKELY_TIN_SIZES_G = [28,30,50,56,57,85,100,114,125,180,200,250,500,1000]

# Grades ordered best→lowest (tune as you like)
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
ALLOWED_SPECIES = {s[1] for s in SPECIES_PATTERNS}

EXCLUDE_WORDS = [
    "gift set","giftset","set","bundle","sampler","flight","pairing","experience","kit",
    "accessory","accessories","spoon","mother of pearl","key","opener","tin opener",
    "blini","bellini","creme","crème","server","bowl","tray","plate","dish","cooler","chiller",
    "gift card","chips","tote","bag","club","subscription","subscribe","duo","trio","quad","pack",
    "class","tasting","pair","collection","assortment","starter"
]
NON_STURGEON_ROE = ["salmon roe","trout roe","whitefish roe","tobiko","masago","ikura","capelin","lumpfish","paddlefish","bowfin"]

# Approx shipping “proximity” scoring for US states (closer to GA better)
STATE_DISTANCE_BUCKET = {
    # GA neighbors / Southeast
    "GA":0, "AL":1, "FL":1, "TN":1, "SC":1, "NC":1, "MS":2, "LA":2, "VA":2,
    # East/Mid-Atlantic
    "MD":2, "DC":2, "DE":2, "PA":3, "NJ":3, "NY":3, "MA":4, "CT":4, "RI":4, "VT":5, "NH":5, "ME":6,
    # Midwest
    "KY":2, "WV":2, "OH":3, "MI":4, "IN":3, "IL":3, "WI":4, "MN":5, "IA":4, "MO":3,
    # Plains/Texas
    "TX":3, "OK":3, "AR":2, "KS":4, "NE":4, "SD":5, "ND":6,
    # Mountain/West
    "CO":4, "NM":4, "AZ":5, "UT":5, "ID":6, "MT":6, "WY":6, "NV":6,
    # West Coast
    "CA":6, "OR":6, "WA":6,
}

VENDOR_HOME_STATE = {
    "Marshallberg Farm": "NC",
    "Island Creek": "MA",
    "Pearl Street Caviar": "NY",
    "Marky's": "FL",
    "Bemka / CaviarLover": "FL",
    "Sterling": "CA",
    "Tsar Nicoulai": "CA",
}

# ---------- HTTP ----------
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

# ---------- utils ----------
def parse_size(text):
    m = SIZE_RE.search(text or "")
    if not m: return None
    val, unit = float(m.group(1)), m.group(2).lower()
    grams = val * 28.3495 if unit.startswith("oz") else val
    return grams

def size_label_both(g):
    if not g: return None
    g_int = int(round(g))
    oz = g/28.3495
    oz_str = str(int(round(oz))) if abs(oz-round(oz))<0.05 else f"{oz:.1f}".rstrip("0").rstrip(".")
    return f"{oz_str} oz / {g_int} g"

def is_accessory(text):
    t=(text or "").lower()
    return any(w in t for w in EXCLUDE_WORDS)

def non_sturgeon(text):
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
    st = VENDOR_HOME_STATE.get(vendor_name)
    return st or "FL"  # default

def proximity_score(state):
    return STATE_DISTANCE_BUCKET.get(state, 6)

# ---------- SQLite ----------
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
    # latest per (vendor,url,size)
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

# ---------- scraping ----------
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
    extras=[]
    for tag in soup.find_all("script", type="application/json"):
        txt=tag.string or ""
        if not txt or '"variants"' not in txt:
            continue
        try:
            data=json.loads(txt)
        except Exception:
            continue
        if isinstance(data, dict):
            # collect tags/options for extra species/grade hints
            for key in ("tags","options","vendor","type","product_type"):
                v=data.get(key)
                if isinstance(v, list): extras.append(" ".join(map(str,v)))
                elif isinstance(v,str): extras.append(v)
            vars=data.get("variants") or []
            for v in vars:
                title = (v.get("title") or "").strip()
                size_g = parse_size(title)
                price_cents = v.get("price")
                if size_g and price_cents is not None:
                    price = float(price_cents)/100.0
                    if any(abs(size_g - s) <= 2 for s in LIKELY_TIN_SIZES_G):
                        results.append({"title":title,"size_g":float(size_g),"price":price,"currency":"USD"})
    return results, " ".join(extras)

def scrape_product(url, vendor):
    r = safe_get(url)
    if not (r and r.ok):
        return []
    soup = BeautifulSoup(r.text, "lxml")
    text = soup.get_text(" ", strip=True)
    title = (soup.title.string if soup.title else "") or ""
    h1 = soup.find("h1")
    h1_text = h1.get_text(" ", strip=True) if h1 else ""
    name = (h1_text or title).strip()

    # must be actual caviar tin/jar
    if not (CAVIAR_WORD.search(name.lower()+ " "+ text.lower())): return []
    if is_accessory(name) or is_accessory(text): return []
    if non_sturgeon(name) or non_sturgeon(text): return []

    # species & grade
    species, latin = extract_species(name) or (None,None)
    if not species:
        species, latin = extract_species(text)
    if not species:
        # try JSON-LD additionalProperty
        for tag in soup.find_all("script", type="application/ld+json"):
            try:
                data=json.loads(tag.string or "{}")
            except Exception:
                continue
            blob=json.dumps(data)
            species, latin = extract_species(blob)
            if species: break
    if not species:
        return []  # species required

    gr_rank, grade_label = grade_rank(name + " " + text)

    # price/size from JSON-LD
    out=[]
    ld = extract_ld_offers(soup)
    got_variant=False
    for item in ld:
        nm = item["name"] or name
        size_g = parse_size(nm + " " + item.get("desc","") + " " + text)
        offer = None
        if size_g:
            # find best offer that mentions size tokens
            toks = size_tokens(size_g)
            candidates = [o for o in item["offers"] if any(t in (o.get("name","")+ " "+o.get("sku","")).lower() for t in toks)]
            offer = candidates[0] if candidates else None
        if not offer and item["offers"]:
            # fallback cheapest
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
            got_variant=True
            break

    if not got_variant:
        # platform variants (Shopify)
        vars, extras = extract_shopify_variants(soup)
        if vars:
            for v in vars:
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
    if g in (28,29,30): tokens|={"28g","29g","30g","28 g","29 g","30 g"}
    def ozs(x): return str(int(round(x))) if abs(x-round(x))<0.05 else f"{x:.1f}".rstrip('0').rstrip('.')
    for o in {oz_round, round(oz)}:
        s = ozs(o)
        tokens |= {f"{s}oz", f"{s} oz", f"{s} ounce", f"{s} ounces"}
    return {t.lower() for t in tokens}

def crawl_site(site_cfg, deadline):
    results=[]
    vendor = site_cfg.get("name","Vendor")
    allow = set(site_cfg.get("allow_domains") or [])
    def domain_ok(u):
        return (not allow) or (urlparse(u).netloc.replace("www.","") in {d.replace("www.","") for d in allow})

    # seeds first (guaranteed PDPs)
    for seed in site_cfg.get("seed_product_urls", [])[:MAX_LINKS_PER_SITE]:
        if datetime.utcnow() > deadline: return results
        if not (looks_like_product_url(seed.lower()) and domain_ok(seed)):
            continue
        results += scrape_product(seed, vendor)
        time.sleep(0.1)

    # then category pages
    for start in site_cfg.get("start_urls", []):
        if datetime.utcnow() > deadline: return results
        r = safe_get(start)
        if not (r and r.ok): continue
        soup = BeautifulSoup(r.text, "lxml")
        links=set()

        sel = (site_cfg.get("selectors") or {}).get("product_link")
        if sel:
            for a in soup.select(sel):
                href=a.get("href"); 
                if not href: continue
                full=urljoin(start, href)
                if domain_ok(full) and looks_like_product_url(full.lower()): links.add(full)
        # heuristic
        for a in soup.find_all("a", href=True):
            full=urljoin(start, a["href"])
            if domain_ok(full) and looks_like_product_url(full.lower()): links.add(full)

        for u in list(links)[:MAX_LINKS_PER_SITE]:
            if datetime.utcnow() > deadline: break
            results += scrape_product(u, vendor)
            time.sleep(0.1)
    return results

# ---------- public API ----------
def run_scrape(price_sites_yaml="price_sites.yaml"):
    start = datetime.utcnow()
    deadline = start + timedelta(seconds=RUN_LIMIT_SECONDS)
    with open(price_sites_yaml, "r") as f:
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
