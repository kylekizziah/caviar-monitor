import os, re, json, time, sqlite3, yaml
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import urllib.robotparser as robotparser

import requests
from bs4 import BeautifulSoup
from jinja2 import Environment, FileSystemLoader, select_autoescape
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv

# ----- Load env (Render already injects; .env for local) -----
load_dotenv()

NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
FROM_EMAIL = os.getenv("FROM_EMAIL")
TO_EMAIL = os.getenv("TO_EMAIL")

# Optional: adjust how many items to show
DIGEST_ITEMS = int(os.getenv("DIGEST_ITEMS", "6"))
DB_PATH = os.getenv("DB_PATH", "caviar_agent.db")
# Queries for producer/farm news (kept minimal because we hard-filter later)
QUERY_TERMS = [q.strip() for q in os.getenv("QUERY_TERMS", '"caviar"').split(",")]

# ----- Jinja template loader (absolute path so it works on Render) -----
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), 'templates')
env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(['html', 'xml'])
)

# =========================
# Database
# =========================
def init_db(path):
    conn = sqlite3.connect(path, check_same_thread=False)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS prices(
        id INTEGER PRIMARY KEY,
        site TEXT, url TEXT, name TEXT, currency TEXT, price REAL,
        size_g REAL, size_label TEXT, per_g REAL,
        seen_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS news(
        id TEXT PRIMARY KEY, title TEXT, url TEXT, published_at TEXT,
        source TEXT, summary TEXT, category TEXT, seen_at TEXT
    )""")
    conn.commit()
    return conn

# =========================
# Helpers
# =========================
CAVIAR_WORD = re.compile(r"\bcaviar\b", re.IGNORECASE)
SIZE_RE = re.compile(r'(\d+(?:\.\d+)?)\s*(g|gram|grams|oz|ounce|ounces)\b', re.I)
MONEY_RE = re.compile(r'([$\£\€])\s*([0-9]+(?:\.[0-9]{1,2})?)')
BAD_NEWS_CONTEXT = [
    "champagne and caviar", "private jet", "status symbol",
    "beauty", "skincare", "makeup", "art deco", "fashion show"
]

def parse_size(text):
    m = SIZE_RE.search(text or "")
    if not m: return None, None
    val, unit = float(m.group(1)), m.group(2).lower()
    grams = val * 28.3495 if unit.startswith('oz') else val
    label = f"{int(val) if val.is_integer() else val}{unit}"
    return grams, label

def norm_price_currency(text):
    m = MONEY_RE.search(text or "")
    if m:
        currency = {'$':'USD','£':'GBP','€':'EUR'}.get(m.group(1),'USD')
        return currency, float(m.group(2))
    return 'USD', None

def safe_get(url, timeout=20):
    try:
        return requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0"})
    except Exception:
        return None

def robots_allowed(url):
    try:
        base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        rp = robotparser.RobotFileParser()
        rp.set_url(urljoin(base, "/robots.txt")); rp.read()
        return rp.can_fetch("*", url)
    except Exception:
        return True  # if robots can't be read, be permissive

# =========================
# Price scraping
# =========================
def extract_ld_json_products(soup):
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
                price=None; currency='USD'
                offers = b.get("offers")
                if isinstance(offers, dict):
                    price = float(offers.get("price")) if offers.get("price") else None
                    currency = offers.get("priceCurrency","USD")
                elif isinstance(offers, list):
                    for off in offers:
                        if isinstance(off, dict) and off.get("price"):
                            price=float(off.get("price")); currency=off.get("priceCurrency","USD"); break
                desc = (b.get("description") or "")
                items.append({"name":name, "price":price, "currency":currency, "desc":desc})
    return items

def scrape_product_page(url):
    if not robots_allowed(url): return []
    r = safe_get(url)
    if not (r and r.ok): return []
    soup = BeautifulSoup(r.text, "lxml")
    out = []
    # Prefer JSON-LD
    for p in extract_ld_json_products(soup):
        size_g, size_label = parse_size(p["name"] + " " + p.get("desc",""))
        price = p["price"]
        if price is None:
            cur, price = norm_price_currency(soup.get_text(" "))
        else:
            cur = p["currency"] or "USD"
        if not (price and size_g):  # need both price & size to compute per-gram
            continue
        per_g = price / size_g
        out.append({
            "name": p["name"], "price": price, "currency": cur,
            "size_g": size_g, "size_label": size_label or "n/a",
            "per_g": per_g, "url": url
        })
    return out

def crawl_site(site):
    results=[]
    start_urls = site.get("start_urls",[])
    sel = site.get("selectors",{})
    domain_whitelist = site.get("allow_domains") or []
    for start in start_urls:
        r = safe_get(start)
        if not (r and r.ok): continue
        soup = BeautifulSoup(r.text, "lxml")
        # find candidate product links
        links=set()
        # site-provided selector
        if sel.get("product_link"):
            for a in soup.select(sel["product_link"]):
                href=a.get("href"); 
                if not href: continue
                full=urljoin(start, href)
                if domain_whitelist and urlparse(full).netloc not in domain_whitelist: continue
                links.add(full)
        # heuristic: product-ish urls
        for a in soup.find_all("a", href=True):
            href=a["href"].lower()
            if any(x in href for x in ["product","shop","store","/p/","/prod/","/item/","collection","caviar"]):
                full=urljoin(start, a["href"])
                if domain_whitelist and urlparse(full).netloc not in domain_whitelist: continue
                links.add(full)
        # visit each product (cap per site)
        for url in list(links)[:60]:
            for p in scrape_product_page(url):
                # must explicitly contain the word "caviar" in product name
                text = (p["name"] or "").lower()
                if not CAVIAR_WORD.search(text):
                    continue
                p["site"]=site.get("name") or urlparse(url).netloc
                results.append(p)
    return results

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
                 FROM prices
                 ORDER BY per_g ASC LIMIT ?""",(top_n,))
    rows=c.fetchall()
    return [{"site":r[0],"name":r[1],"price":r[2],"currency":r[3],
             "size_g":r[4],"size_label":r[5],"per_g":r[6],"url":r[7]} for r in rows]

def get_movers(conn):
    # last price vs previous for same (site,name)
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
# News (strict "caviar" topic)
# =========================
def newsapi_search(q, since):
    if not NEWSAPI_KEY: return []
    r = requests.get("https://newsapi.org/v2/everything", params={
        "q": q, "apiKey": NEWSAPI_KEY, "sortBy":"publishedAt", "language":"en", "from": since.isoformat()
    }, timeout=20)
    if not r.ok: return []
    return r.json().get("articles",[])

def is_relevant_exact_caviar(title: str, desc: str, url: str = "") -> bool:
    text = f"{title or ''} {desc or ''} {url or ''}".lower()
    if not CAVIAR_WORD.search(text): return False
    if any(b in text for b in BAD_NEWS_CONTEXT): return False
    return True

def fetch_news(conn, hours=72):
    since = datetime.utcnow() - timedelta(hours=hours)
    for q in QUERY_TERMS:
        arts=newsapi_search(q, since)
        for a in arts:
            title=a.get("title",""); desc=a.get("description",""); url=a.get("url")
            if not url or not is_relevant_exact_caviar(title,desc,url): continue
            src=(a.get("source") or {}).get("name","")
            item_id=url
            try:
                conn.execute("""INSERT INTO news(id,title,url,published_at,source,summary,category,seen_at)
                                VALUES(?,?,?,?,?,?,?,?)""",
                             (item_id,title,url,a.get("publishedAt"),src,desc,"General",datetime.utcnow().isoformat()))
            except sqlite3.IntegrityError:
                pass
    conn.commit()
    # return top recent items
    cur=conn.cursor()
    cur.execute("""SELECT title,url,published_at,source,summary,category FROM news
                   ORDER BY published_at DESC LIMIT ?""",(DIGEST_ITEMS,))
    rows=cur.fetchall()
    return [{"title":r[0],"url":r[1],"published_at":r[2],"source":r[3],"summary":r[4],"category":r[5]} for r in rows]

# =========================
# Email render & send
# =========================
def render_html(cheapest, movers, news_items):
    tpl = env.get_template("digest_template.html")
    return tpl.render(
        date=datetime.utcnow().strftime("%B %d, %Y"),
        cheapest=cheapest, movers=movers, news_items=news_items
    )

def send_email_html(subject, html_body):
    sg = SendGridAPIClient(SENDGRID_API_KEY)
    resp = sg.send(Mail(from_email=FROM_EMAIL, to_emails=TO_EMAIL,
                        subject=subject, html_content=html_body))
    print("Email send status:", resp.status_code)

# =========================
# Main
# =========================
def main():
    if not (SENDGRID_API_KEY and FROM_EMAIL and TO_EMAIL):
        print("ERROR: SendGrid vars not set."); return

    conn=init_db(DB_PATH)

    # ---- Load seller list (required) ----
    seed_path = os.path.join(os.path.dirname(__file__),"price_sites.yaml")
    try:
        with open(seed_path, "r") as f:
            cfg=yaml.safe_load(f) or {}
    except Exception as e:
        print("No price_sites.yaml found or unreadable:", e)
        cfg={"sites":[]}

    # ---- Crawl each site; store prices ----
    all_prices=[]
    for site in cfg.get("sites", []):
        print("Crawling:", site.get("name"))
        items=crawl_site(site)
        all_prices.extend(items)
        time.sleep(0.5)  # be polite
    if all_prices:
        store_prices(conn, all_prices)
        print(f"Stored {len(all_prices)} price rows.")

    # ---- Compute outputs ----
    cheapest=get_cheapest(conn, top_n=10)
    movers=get_movers(conn)
    news_items=fetch_news(conn, hours=72)

    # ---- Email ----
    html_body=render_html(cheapest, movers, news_items)
    subject=f"Daily Caviar Digest — {datetime.utcnow().strftime('%b %d, %Y')}"
    send_email_html(subject, html_body)
    print("Digest compiled with", len(cheapest), "cheapest rows and", len(news_items), "news items.")

if __name__=="__main__":
    main()
