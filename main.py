import os, re, json, time, sqlite3, yaml
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import urllib.robotparser as robotparser

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
from jinja2 import Environment, FileSystemLoader, select_autoescape
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv

# =========================
# Environment & Constants
# =========================
load_dotenv()

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
FROM_EMAIL       = os.getenv("FROM_EMAIL")
TO_EMAIL         = os.getenv("TO_EMAIL")

DB_PATH = os.getenv("DB_PATH", "caviar_agent.db")

# Keep these small & simple so a run completes and emails reliably
RUN_LIMIT_SECONDS  = int(os.getenv("RUN_LIMIT_SECONDS", "150"))
MAX_LINKS_PER_SITE = int(os.getenv("MAX_LINKS_PER_SITE", "30"))

# Handy toggles you already used
SEND_TEST      = os.getenv("SEND_TEST")       # "1" = send a simple test email then exit
DEBUG_URL      = os.getenv("DEBUG_URL")       # scrape one URL then exit (for quick checks)
RESPECT_ROBOTS = os.getenv("RESPECT_ROBOTS", "1") not in ("0", "false", "False")

# Templates
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), 'templates')
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR),
                  autoescape=select_autoescape(['html','xml']))

# =========================
# Filters & Parsing
# =========================
CAVIAR_WORD = re.compile(r"\bcaviar\b", re.I)
SIZE_RE     = re.compile(r'(\d+(?:\.\d+)?)\s*(g|gram|grams|oz|ounce|ounces)\b', re.I)
MONEY_RE    = re.compile(r'([$\£\€])\s*([0-9]+(?:\.[0-9]{1,2})?)')

# Exclude accessories/sets/etc.
EXCLUDE_WORDS = [
    "gift set","giftset","set","bundle","sampler","flight","pairing","experience","kit",
    "accessory","accessories","spoon","mother of pearl","key","opener","tin opener",
    "blini","bellini","crepe","crème fraîche","creme fraiche","ice bowl","server",
    "tray","plate","dish","cooler","chiller","gift card","card","chips","tote","bag",
    "club","subscription","subscribe","duo","trio","quad","pack","2-pack","3-pack","4-pack",
    "class","tasting","pair","pairings","collection","assortment","starter"
]
NON_CAVIAR_ROE = ["salmon roe","trout roe","whitefish roe","tobiko","masago","ikura"]

URL_BLOCKLIST = [
    "/cart", "/account", "/login", "/search", "/policy", "/policies",
    "/pages/contact", "/pages/faq", "/pages/shipping", "/pages/returns",
    "/privacy", "/terms", "/checkout"
]
LIKELY_TIN_SIZES_G = [28,30,50,56,57,85,100,114,125,180,200,250,500,1000]

def norm_netloc(host: str) -> str:
    host = host or ""
    return host[4:] if host.startswith("www.") else host

# =========================
# DB (tiny)
# =========================
def init_db(path):
    conn = sqlite3.connect(path, check_same_thread=False)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS prices(
        id INTEGER PRIMARY KEY,
        site TEXT, url TEXT, name TEXT, currency TEXT, price REAL,
        size_g REAL, size_label TEXT, per_g REAL, seen_at TEXT
    )""")
    conn.commit()
    return conn

def store_prices(conn, items):
    if not items: return
    c = conn.cursor()
    for it in items:
        c.execute("""INSERT INTO prices(site,url,name,currency,price,size_g,size_label,per_g,seen_at)
                     VALUES(?,?,?,?,?,?,?,?,?)""",
                  (it["site"], it["url"], it["name"], it["currency"], it["price"],
                   it["size_g"], it["size_label"], it["per_g"], datetime.utcnow().isoformat()))
    conn.commit()

def get_cheapest(conn, top_n=10):
    c = conn.cursor()
    c.execute("""SELECT site,name,price,currency,size_g,size_label,per_g,url
                 FROM prices ORDER BY per_g ASC LIMIT ?""",(top_n,))
    rows = c.fetchall()
    return [{"site":r[0],"name":r[1],"price":r[2],"currency":r[3],
             "size_g":r[4],"size_label":r[5],"per_g":r[6],"url":r[7]} for r in rows]

def get_movers(conn):
    # compare last vs previous price for same site+name
    c = conn.cursor()
    c.execute("""
      WITH ranked AS (
        SELECT site,name,currency,price,size_label,seen_at,
               ROW_NUMBER() OVER (PARTITION BY site,name ORDER BY seen_at DESC) rn
        FROM prices)
      SELECT a.site,a.name,a.currency,a.price,a.size_label,b.price
      FROM ranked a LEFT JOIN ranked b
        ON a.site=b.site AND a.name=b.name AND b.rn=2
      WHERE a.rn=1 AND b.price IS NOT NULL
    """)
    out=[]
    for site,name,cur,price,label,prev in c.fetchall():
        if prev and price != prev:
            delta=price-prev
            pct=round((delta/prev)*100,2)
            out.append({"site":site,"name":name,"currency":cur,"price":price,
                        "delta_abs":abs(delta),"delta_pct":pct,
                        "delta_sign":"+" if delta>0 else "-","size_label":label})
    return sorted(out, key=lambda x: -abs(x["delta_pct"]))[:5]

# =========================
# HTTP session (no proxy)
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

def robots_allowed(url):
    if not RESPECT_ROBOTS:
        return True
    try:
        base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        rp = robotparser.RobotFileParser()
        rp.set_url(urljoin(base, "/robots.txt")); rp.read()
        return rp.can_fetch(SESSION.headers.get("User-Agent","*"), url)
    except Exception:
        return True

# =========================
# Parsing helpers
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

def contains_non_caviar_roe(text):
    t = (text or "").lower()
    return any(w in t for w in NON_CAVIAR_ROE)

def is_individual_tinjar(name, page_text):
    nm = (name or "").lower(); pg = (page_text or "").lower()
    if not CAVIAR_WORD.search(nm + " " + pg): return False
    if looks_like_accessory(nm) or looks_like_accessory(pg): return False
    if contains_non_caviar_roe(nm) or contains_non_caviar_roe(pg): return False
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
    """Return list of {name, desc, offers:[{price, currency, name?, sku?}]}"""
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
    """Pick the offer whose name/SKU best matches the size tokens."""
    if not offers or not size_g: return None
    toks = size_tokens(size_g)
    for o in offers:
        s = (o.get("name","") + " " + o.get("sku","")).lower()
        if any(t in s for t in toks):
            return o
    priced = [o for o in offers if o.get("price")]
    if len(priced)==1: return priced[0]
    return sorted(priced, key=lambda x: x.get("price"))[0] if priced else None

# =========================
# Scraper Core
# =========================
def scrape_product_page(url, site_selectors=None):
    """
    Extract one product from an exact product URL.
    - Only accepts true product pages (/product/ or /products/).
    - Only keeps individual caviar tins/jars.
    - Matches price to detected size via JSON-LD offers when available.
    """
    low = url.lower()
    if any(b in low for b in URL_BLOCKLIST): return []
    if "/product" not in low and "/products" not in low: return []

    if not robots_allowed(url):
        print("ROBOTS disallow:", url)
        if RESPECT_ROBOTS: return []
        else: print("ROBOTS override: proceeding.")

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

    # JSON-LD with variant match
    for item in extract_ld_json_products_extended(soup):
        name = (item.get("name") or "").strip() or h1_text or page_title
        if not name or not is_individual_tinjar(name, page_text): 
            continue
        size_g, _ = parse_size(name + " " + (item.get("desc") or "") + " " + page_text)
        if not size_g: 
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
            continue
        pack = infer_packaging(name) or infer_packaging(page_text)
        size_label = grams_to_label_both(size_g, packaging=pack)
        out.append({"name":name,"price":float(price),"currency":currency,
                    "size_g":float(size_g),"size_label":size_label or "n/a",
                    "per_g":float(price)/float(size_g),"url":url})
        # one product per page is expected; break once we have a good one
        break

    # Fallback: selectors/meta
    if not out:
        name = h1_text or page_title
        if not is_individual_tinjar(name, page_text): return []
        size_g, _ = parse_size(name + " " + page_text)
        if not size_g: return []
        price=None; currency="USD"

        try_selectors=[]
        if site_selectors:
            if site_selectors.get("price"): try_selectors.append(site_selectors["price"])
            if site_selectors.get("name") and not name:
                n = soup.select_one(site_selectors["name"])
                if n: name = n.get_text(strip=True)

        try_selectors += [
            "[itemprop='price']",
            ".price-item--regular", ".price", ".product-price",
            "meta[property='og:price:amount']::attr(content)",
            "meta[name='twitter:data1']::attr(content)",
            "[data-price]"
        ]

        for sel in try_selectors:
            node=None; attr=None
            if "::attr(" in sel:
                css, attr = sel.split("::attr("); attr = attr.replace(")","").strip()
                node = soup.select_one(css.strip())
                val = node[attr] if (node and node.has_attr(attr)) else None
            else:
                node = soup.select_one(sel)
                val = node.get_text(" ", strip=True) if node else None
            if val:
                m = MONEY_RE.search(val)
                if m:
                    currency = {'$':'USD','£':'GBP','€':'EUR'}.get(m.group(1),'USD')
                    price = float(m.group(2)); break

        if price:
            pack = infer_packaging(name) or infer_packaging(page_text)
            size_label = grams_to_label_both(size_g, packaging=pack)
            out.append({"name":name,"price":float(price),"currency":currency,
                        "size_g":float(size_g),"size_label":size_label or "n/a",
                        "per_g":float(price)/float(size_g),"url":url})
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
        return ("/products/" in low) or ("/product/" in low)

    for start in start_urls:
        if timed_out(): print(f"[{site_name}] timebox before start."); break
        r = safe_get(start)
        if not (r and r.ok):
            code = r.status_code if r else "ERR"
            print(f"START failed ({code}):", start); continue
        soup = BeautifulSoup(r.text, "lxml")
        links=set()

        # site selector
        if sel.get("product_link"):
            for a in soup.select(sel["product_link"]):
                href=a.get("href"); 
                if not href: continue
                full=urljoin(start, href)
                if domain_ok(full) and allowed(full): links.add(full)

        # heuristic
        for a in soup.find_all("a", href=True):
            full=urljoin(start, a["href"])
            if domain_ok(full) and allowed(full): links.add(full)

        links_list = list(links)[:MAX_LINKS_PER_SITE]
        print(f"[{site_name}] found {len(links_list)} product links on {start}")

        kept=0
        for url in links_list:
            if timed_out(): print(f"[{site_name}] timebox mid-site."); break
            prods = scrape_product_page(url, site_selectors=sel)
            for p in prods:
                p["site"]=site_name; results.append(p); kept+=1
        print(f"[{site_name}] kept {kept} products from {start}")
        time.sleep(0.4)
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

        print(f"Starting run at {start.isoformat()} (timebox {RUN_LIMIT_SECONDS}s, RESPECT_ROBOTS={'1' if RESPECT_ROBOTS else '0'})")
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

        cheapest = get_cheapest(conn, top_n=10)
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
