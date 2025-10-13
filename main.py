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

NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")  # optional (news currently disabled in digest)
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
FROM_EMAIL = os.getenv("FROM_EMAIL")
TO_EMAIL = os.getenv("TO_EMAIL")

DB_PATH = os.getenv("DB_PATH", "caviar_agent.db")
DIGEST_ITEMS = int(os.getenv("DIGEST_ITEMS", "6"))

# Controls crawl/runtime so email always sends
RUN_LIMIT_SECONDS = int(os.getenv("RUN_LIMIT_SECONDS", "180"))   # total timebox
MAX_LINKS_PER_SITE = int(os.getenv("MAX_LINKS_PER_SITE", "40"))  # per site cap

# Debug / test modes
DEBUG_URL = os.getenv("DEBUG_URL")    # scrape a single product URL and print result (no email)
SEND_TEST = os.getenv("SEND_TEST")    # set to "1" to send a simple test email immediately
RESPECT_ROBOTS = os.getenv("RESPECT_ROBOTS", "1") not in ("0", "false", "False")

# Template loader (absolute path so it works on Render)
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), 'templates')
env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(['html', 'xml'])
)

# =========================
# Matching & Parsing
# =========================
CAVIAR_WORD = re.compile(r"\bcaviar\b", re.IGNORECASE)
SIZE_RE = re.compile(r'(\d+(?:\.\d+)?)\s*(g|gram|grams|oz|ounce|ounces)\b', re.I)
MONEY_RE = re.compile(r'([$\£\€])\s*([0-9]+(?:\.[0-9]{1,2})?)')

# Exclude accessories/gifts/sets/subscriptions/etc.
EXCLUDE_WORDS = [
    "gift set","giftset","set","bundle","sampler","flight","pairing","experience","kit",
    "accessory","accessories","spoon","mother of pearl","key","opener","tin opener",
    "blini","bellini","crepe","crème fraîche","creme fraiche","ice bowl","server",
    "tray","plate","dish","cooler","chiller","gift card","card","chips","tote","bag",
    "club","subscription","subscribe","duo","trio","quad","pack","2-pack","3-pack","4-pack",
    "caviar class","class","tasting","pair","pairings","collection","assortment","starter"
]
# Exclude non-caviar roe unless explicitly marked caviar
NON_CAVIAR_ROE = ["salmon roe","trout roe","whitefish roe","tobiko","masago","ikura"]

# Hard blocklist of URL path fragments to skip entirely
URL_BLOCKLIST = [
    "/cart", "/account", "/login", "/search", "/policy", "/policies",
    "/pages/contact", "/pages/faq", "/pages/shipping", "/pages/returns",
    "/privacy", "/terms", "/checkout"
]

# Recognized single-unit sizes (grams) that look like tins/jars
LIKELY_TIN_SIZES_G = [28, 30, 50, 56, 57, 85, 100, 114, 125, 180, 200, 250, 500, 1000]

def norm_netloc(host: str) -> str:
    host = host or ""
    return host[4:] if host.startswith("www.") else host

# =========================
# DB
# =========================
def init_db(path):
    conn = sqlite3.connect(path, check_same_thread=False)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS prices(
        id INTEGER PRIMARY KEY,
        site TEXT, url TEXT, name TEXT, currency TEXT, price REAL,
        size_g REAL, size_label TEXT, per_g REAL, seen_at TEXT
    )""")
    # keep news table for future use
    c.execute("""CREATE TABLE IF NOT EXISTS news(
        id TEXT PRIMARY KEY, title TEXT, url TEXT, published_at TEXT,
        source TEXT, summary TEXT, category TEXT, seen_at TEXT
    )""")
    conn.commit()
    return conn

def store_prices(conn, items):
    if not items: return
    c=conn.cursor()
    for it in items:
        c.execute("""INSERT INTO prices(site,url,name,currency,price,size_g,size_label,per_g,seen_at)
                     VALUES(?,?,?,?,?,?,?,?,?)""",
                  (it["site"], it["url"], it["name"], it["currency"], it["price"],
                   it["size_g"], it["size_label"], it["per_g"], datetime.utcnow().isoformat()))
    conn.commit()

def get_cheapest(conn, top_n=10):
    c=conn.cursor()
    c.execute("""SELECT site,name,price,currency,size_g,size_label,per_g,url
                 FROM prices ORDER BY per_g ASC LIMIT ?""",(top_n,))
    rows=c.fetchall()
    return [{"site":r[0],"name":r[1],"price":r[2],"currency":r[3],
             "size_g":r[4],"size_label":r[5],"per_g":r[6],"url":r[7]} for r in rows]

def get_movers(conn):
    c=conn.cursor()
    c.execute("""
      WITH ranked AS (
        SELECT site,name,currency,price,size_label,seen_at,
               ROW_NUMBER() OVER (PARTITION BY site,name ORDER BY seen_at DESC) AS rn
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
                        "delta_abs":abs(delta),"delta_pct":pct,"delta_sign":"+" if delta>0 else "-",
                        "size_label":label})
    return sorted(out, key=lambda x: -abs(x["delta_pct"]))[:5]

# =========================
# HTTP session with retries
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
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s

SESSION = make_session()

def safe_get(url, timeout=20):
    try:
        r = SESSION.get(url, timeout=timeout)
        return r
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
        allowed = rp.can_fetch(SESSION.headers.get("User-Agent","*"), url)
        return allowed
    except Exception:
        return True  # if robots can't be read, be permissive

# =========================
# Product parsing utilities
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
    label = f"{oz_str} / {g} g"
    if packaging:
        label += f" {packaging}"
    return label

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
    nm = (name or "").lower()
    pg = (page_text or "").lower()
    if not CAVIAR_WORD.search(nm + " " + pg):
        return False
    if looks_like_accessory(nm) or looks_like_accessory(pg):
        return False
    if contains_non_caviar_roe(nm) or contains_non_caviar_roe(pg):
        return False
    pack = infer_packaging(nm) or infer_packaging(pg)
    size_g, _ = parse_size(nm + " " + pg)
    # Require tin/jar OR a common single size (to avoid sets/bundles)
    if pack: 
        return True
    if size_g:
        # treat as individual tin if size is very close to a typical tin size
        if any(abs(size_g - s) <= 2 for s in LIKELY_TIN_SIZES_G):
            return True
    return False

def size_tokens(size_g):
    """Tokens like '30g', '30 g', '1 oz', '1oz', '1 ounce' to match variant names/SKU."""
    if not size_g: return []
    g = int(round(size_g))
    oz = size_g / 28.3495
    oz_round = round(oz, 1)
    tokens = {f"{g}g", f"{g} g"}
    # also add nearby gram spellings like 28/30g around 1oz
    if g in (28,29,30):
        tokens.update({"28g","29g","30g","28 g","29 g","30 g"})
    # ounce tokens
    def oz_str(x):
        if abs(x - round(x)) < 0.05:
            return str(int(round(x)))
        return f"{x:.1f}".rstrip('0').rstrip('.')
    for o in {oz_round, round(oz)}:
        tokens.update({f"{oz_str(o)}oz", f"{oz_str(o)} oz", f"{oz_str(o)} ounce", f"{oz_str(o)} ounces"})
    return {t.lower() for t in tokens}

def extract_ld_json_products_extended(soup):
    """
    Return list of {name, desc, offers:[{price, currency, name?, sku?}]}
    """
    items=[]
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "{}")
        except Exception:
            continue
        blocks = data if isinstance(data, list) else [data]
        for b in blocks:
            if not isinstance(b, dict): continue
            if (b.get("@type") in ("Product","Offer")) or ("offers" in b):
                name = (b.get("name") or "").strip()
                desc = (b.get("description") or "")
                offers=[]
                off = b.get("offers")
                if isinstance(off, dict):
                    offers = [off]
                elif isinstance(off, list):
                    offers = [o for o in off if isinstance(o, dict)]
                norm_offers=[]
                for o in offers:
                    norm_offers.append({
                        "price": (float(o.get("price")) if (o.get("price") not in (None,"")) else None),
                        "currency": o.get("priceCurrency","USD"),
                        "name": (o.get("name") or o.get("description") or ""),
                        "sku": o.get("sku") or ""
                    })
                items.append({"name":name, "desc":desc, "offers":norm_offers})
    return items

def choose_offer_for_size(offers, size_g):
    """Pick the offer whose name/SKU best matches the size tokens."""
    if not offers or not size_g: 
        return None
    toks = size_tokens(size_g)
    # 1) perfect token match in offer name/sku
    for o in offers:
        s = (o.get("name","") + " " + o.get("sku","")).lower()
        if any(t in s for t in toks):
            return o
    # 2) otherwise: if there is only one offer with a price, take it
    priced = [o for o in offers if o.get("price")]
    if len(priced) == 1:
        return priced[0]
    # 3) last resort: the lowest priced offer
    if priced:
        return sorted(priced, key=lambda x: x.get("price"))[0]
    return None

# =========================
# Scraper Core
# =========================
def scrape_product_page(url, site_selectors=None):
    """
    Extract products for an exact product URL.
    - Only accepts true product pages (we pass only /product(s)/ links).
    - Filters to individual caviar tins/jars.
    - Price matched to the size via JSON-LD offers when available.
    """
    low_url = url.lower()
    if any(b in low_url for b in URL_BLOCKLIST):
        print("BLOCKLIST skip:", url)
        return []
    if "/product" not in low_url and "/products" not in low_url:
        # hard guard: must be a product page
        return []

    if not robots_allowed(url):
        print("ROBOTS disallow:", url)
        if RESPECT_ROBOTS:
            return []
        else:
            print("ROBOTS override (RESPECT_ROBOTS=0): proceeding.")

    r = safe_get(url)
    if not (r and r.ok):
        code = r.status_code if r else "ERR"
        print(f"GET failed ({code}):", url)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    page_text = soup.get_text(" ", strip=True)
    out = []

    page_title = (soup.title.string if soup.title else "") or ""
    h1 = soup.find("h1")
    h1_text = h1.get_text(strip=True) if h1 else ""

    # 1) JSON-LD with variant matching
    for item in extract_ld_json_products_extended(soup):
        name = (item.get("name") or "").strip() or h1_text or page_title
        if not name:
            continue
        if not is_individual_tinjar(name, page_text):
            continue
        size_g, _ = parse_size(name + " " + (item.get("desc") or "") + " " + page_text)
        if not size_g:
            continue
        offer = choose_offer_for_size(item.get("offers"), size_g)
        price = offer.get("price") if offer else None
        currency = (offer.get("currency") if offer else "USD") if price else None
        if not price:
            # fallback price anywhere on page
            cur, maybe = norm_price_currency(page_text)
            price = maybe; currency = cur if maybe else None
        if not (size_g and price and currency):
            continue
        pack = infer_packaging(name) or infer_packaging(page_text)
        size_label = grams_to_label_both(size_g, packaging=pack)
        out.append({
            "name": name, "price": float(price), "currency": currency,
            "size_g": float(size_g), "size_label": size_label or "n/a",
            "per_g": float(price)/float(size_g), "url": url
        })

    # 2) Fallback: selectors/meta (when no usable JSON-LD)
    if not out:
        name = h1_text or page_title
        if not is_individual_tinjar(name, page_text):
            return []
        size_g, _ = parse_size(name + " " + page_text)
        if not size_g:
            return []
        price = None; currency = "USD"

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
                css, attr = sel.split("::attr(")
                attr = attr.replace(")","").strip()
                node = soup.select_one(css.strip())
                val = node[attr] if (node and node.has_attr(attr)) else None
            else:
                node = soup.select_one(sel)
                val = node.get_text(" ", strip=True) if node else None
            if val:
                cur, maybe_price = norm_price_currency(val)
                if maybe_price:
                    price = maybe_price; currency = cur; break

        if price:
            pack = infer_packaging(name) or infer_packaging(page_text)
            size_label = grams_to_label_both(size_g, packaging=pack)
            out.append({
                "name": name, "price": float(price), "currency": currency,
                "size_g": float(size_g), "size_label": size_label or "n/a",
                "per_g": float(price)/float(size_g), "url": url
            })

    return out

def crawl_site(site, deadline=None):
    """
    Crawl a site's category/start URLs; discover product links; visit them; return parsed products.
    - Only follows **real product pages** (/product/ or /products/).
    - Skips blocked/utility pages.
    - Stops early if 'deadline' is reached.
    """
    results=[]
    start_urls = site.get("start_urls",[])
    sel = site.get("selectors",{}) or {}
    whitelist = set(norm_netloc(h) for h in (site.get("allow_domains") or []))
    site_name = site.get("name") or (whitelist and next(iter(whitelist))) or "site"

    def timed_out():
        return bool(deadline and datetime.utcnow() >= deadline)

    def domain_ok(u):
        if not whitelist:  # no whitelist, allow all
            return True
        net = norm_netloc(urlparse(u).netloc)
        return net in whitelist

    def allowed_candidate(u):
        low = (u or "").lower()
        if any(b in low for b in URL_BLOCKLIST):
            return False
        return ("/products/" in low) or ("/product/" in low)

    for start in start_urls:
        if timed_out():
            print(f"[{site_name}] timebox reached before fetching start url.")
            break

        r = safe_get(start)
        if not (r and r.ok):
            code = r.status_code if r else "ERR"
            print(f"START failed ({code}):", start)
            continue
        soup = BeautifulSoup(r.text, "lxml")

        # Gather candidate product links
        links=set()

        # 1) Site-provided selector
        if sel.get("product_link"):
            for a in soup.select(sel["product_link"]):
                href=a.get("href")
                if not href: continue
                full=urljoin(start, href)
                if not domain_ok(full): continue
                if not allowed_candidate(full): continue
                links.add(full)

        # 2) Heuristic fallback
        for a in soup.find_all("a", href=True):
            href=a["href"]
            full=urljoin(start, href)
            if not domain_ok(full): continue
            if not allowed_candidate(full): continue
            links.add(full)

        links_list = list(links)[:MAX_LINKS_PER_SITE]  # product pages only
        print(f"[{site_name}] found {len(links_list)} product links on {start}")

        # Visit each product link
        kept=0
        for url in links_list:
            if timed_out():
                print(f"[{site_name}] timebox reached mid-site. Stopping.")
                break
            prods = scrape_product_page(url, site_selectors=sel)
            for p in prods:
                p["site"]=site_name
                results.append(p)
                kept+=1

        print(f"[{site_name}] kept {kept} products from {start}")
        time.sleep(0.5)  # politeness delay
        if timed_out():
            break

    return results

# =========================
# Email render/send
# =========================
def render_html(cheapest, movers, news_items):
    # Your template should display items' size_label, which looks like: "1 oz / 30 g tin"
    tpl = env.get_template("digest_template.html")
    return tpl.render(
        date=datetime.utcnow().strftime("%B %d, %Y"),
        cheapest=cheapest, movers=movers, news_items=news_items
    )

def send_email_html(subject, html_body):
    if not (SENDGRID_API_KEY and FROM_EMAIL and TO_EMAIL):
        print("ERROR: SendGrid vars not set.")
        return
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        message = Mail(from_email=FROM_EMAIL, to_emails=TO_EMAIL,
                       subject=subject, html_content=html_body)
        resp = sg.send(message)
        print("Email send status:", resp.status_code)
        try:
            print("Email send body:", getattr(resp, "body", b"")[:500])
        except Exception:
            pass
    except Exception as e:
        import traceback
        print("SENDGRID ERROR:", e)
        print(traceback.format_exc())

# =========================
# Main
# =========================
def main():
    try:
        # Test mode: send email immediately (no scraping)
        if SEND_TEST == "1":
            print("SEND_TEST=1 — sending a simple test email now.")
            html = "<h1>Test email from Caviar Agent</h1><p>If you see this, SendGrid works.</p>"
            subject = f"Test — {datetime.utcnow().strftime('%b %d, %Y %H:%M UTC')}"
            send_email_html(subject, html)
            print("Done with test mode.")
            return

        # Single-page debug scrape
        if DEBUG_URL:
            print("DEBUG_URL set — scraping just this page:", DEBUG_URL)
            prods = scrape_product_page(DEBUG_URL)
            print("Extracted products:", prods)
            return

        if not (SENDGRID_API_KEY and FROM_EMAIL and TO_EMAIL):
            print("ERROR: SendGrid vars not set.")
            return

        # ---- Timebox setup ----
        start = datetime.utcnow()
        deadline = start + timedelta(seconds=RUN_LIMIT_SECONDS)

        def time_left():
            return max(0, (deadline - datetime.utcnow()).total_seconds())

        print(f"Starting run at {start.isoformat()} (timebox {RUN_LIMIT_SECONDS}s, RESPECT_ROBOTS={'1' if RESPECT_ROBOTS else '0'})")
        conn = init_db(DB_PATH)

        # Load sellers
        seed_path = os.path.join(os.path.dirname(__file__), "price_sites.yaml")
        try:
            with open(seed_path, "r") as f:
                cfg = yaml.safe_load(f) or {}
            print("Loaded price_sites.yaml with", len(cfg.get("sites", [])), "sites.")
        except Exception as e:
            print("No price_sites.yaml found or unreadable:", e)
            cfg = {"sites": []}

        # Crawl within timebox
        all_prices=[]
        for site in cfg.get("sites", []):
            if time_left() <= 3:
                print("Global timebox reached before starting next site.")
                break
            print(f"Crawling: {site.get('name')} (time left ≈ {int(time_left())}s)")
            items = crawl_site(site, deadline=deadline)
            print(f" -> {len(items)} products from {site.get('name')}")
            all_prices.extend(items)
            time.sleep(0.2)

        if all_prices:
            store_prices(conn, all_prices)
            print(f"Stored {len(all_prices)} price rows.")
        else:
            print("No prices found this run.")

        # Build digest (price focus)
        cheapest = get_cheapest(conn, top_n=10)
        movers = get_movers(conn)
        news_items = []  # keep empty for speed/focus

        print(f"Digest sections -> cheapest:{len(cheapest)} movers:{len(movers)} news:{len(news_items)}")

        # Email (always send whatever we have so far)
        html_body = render_html(cheapest, movers, news_items)
        subject = f"Daily Caviar Digest — {datetime.utcnow().strftime('%b %d, %Y')}"
        print("Sending email to:", TO_EMAIL)
        send_email_html(subject, html_body)
        print("Done. Total runtime ~", int((datetime.utcnow() - start).total_seconds()), "seconds")
    except Exception as e:
        import traceback
        print("FATAL ERROR:", e)
        print(traceback.format_exc())

if __name__ == "__main__":
    main()
