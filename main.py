import os
import requests
import sqlite3
from datetime import datetime, timedelta
from jinja2 import Environment, FileSystemLoader, select_autoescape
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv

# Load .env when running locally; on Render, vars are already present
load_dotenv()

# ---- Environment variables (set these in Render -> Environment) ----
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
FROM_EMAIL = os.getenv("FROM_EMAIL")
TO_EMAIL = os.getenv("TO_EMAIL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # optional
QUERY_TERMS = [q.strip() for q in os.getenv(
    "QUERY_TERMS",
    'caviar,sturgeon,roe,"sturgeon farming","sturgeon farm","caviar industry","Beluga caviar","Osetra caviar","Kaluga Queen","Tsar Nicoulai","seafood market","luxury food"'
).split(",")]
DIGEST_ITEMS = int(os.getenv("DIGEST_ITEMS", "6"))
DB_PATH = os.getenv("DB_PATH", "caviar_agent.db")

# ---- Jinja template environment ----
env = Environment(
    loader=FileSystemLoader('templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

# ---- DB helpers (very simple cache so we don’t resend dupes) ----
def init_db(path):
    conn = sqlite3.connect(path, check_same_thread=False)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS items (
            id TEXT PRIMARY KEY,
            title TEXT,
            url TEXT,
            published_at TEXT,
            source TEXT,
            summary TEXT,
            impact INTEGER,
            category TEXT,
            raw TEXT,
            seen_at TEXT
        )
    """)
    conn.commit()
    return conn

# ---- Fetch from NewsAPI ----
def newsapi_search(q, from_dt=None, page_size=50):
    if not NEWSAPI_KEY:
        raise RuntimeError("Missing NEWSAPI_KEY")
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": q,
        "apiKey": NEWSAPI_KEY,
        "sortBy": "publishedAt",
        "language": "en",
        "pageSize": page_size
    }
    if from_dt:
        params["from"] = from_dt.isoformat()
    r = requests.get(url, params=params, timeout=25)
    r.raise_for_status()
    return r.json().get("articles", [])

# ---- Lightweight classify/score heuristics ----
CATEGORIES = {
    "Fraud": ["fraud", "illegal", "seize", "seized", "bust", "smuggled", "smuggling"],
    "Producer": ["expansion", "acquire", "acquires", "launched", "opened", "production", "capacity"],
    "Research": ["study", "research", "journal", "paper", "scientists"],
    "Sustainability": ["sustainab", "environment", "pollution", "conservation"],
    "Market": ["price", "market", "trade", "export", "import"]
}

def classify_text(text):
    t = (text or "").lower()
    for cat, kws in CATEGORIES.items():
        for k in kws:
            if k in t:
                return cat
    return "General"

def impact_score(text):
    t = (text or "").lower()
    if any(w in t for w in ["illegal","seized","fraud","smuggled","lawsuit","bust"]): return 9
    if any(w in t for w in ["expansion","acquire","launch","increase","price"]): return 7
    return 5

# ---- Summarization (optional: OpenAI). Fallback = description/title ----
def summarize_text(title, description):
    if OPENAI_API_KEY:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
            msg = f"Summarize in 2 sentences for a business digest.\nTitle: {title}\nDescription: {description}"
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role":"user","content":msg}],
                temperature=0.2,
                max_tokens=180
            )
            return resp.choices[0].message.content.strip()
        except Exception:
            pass
    return description or title or "(no summary provided)"

# ---- Store a new item; return True if inserted (i.e., not a duplicate) ----
def upsert_item(conn, item):
    c = conn.cursor()
    try:
        c.execute("""INSERT INTO items (id,title,url,published_at,source,summary,impact,category,raw,seen_at)
                     VALUES (?,?,?,?,?,?,?,?,?,?)""",
                  (item['id'], item['title'], item['url'], item['published_at'], item['source'],
                   item['summary'], item['impact'], item['category'], item['raw'], datetime.utcnow().isoformat()))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

# ---- Fetch recent items and store ----
def fetch_and_store(conn, hours=72):
    now = datetime.utcnow()
    since = now - timedelta(hours=hours)
    collected = []
    print(f"Fetching articles from the last {hours} hours...")
    for q in QUERY_TERMS:
        try:
            arts = newsapi_search(q, from_dt=since)
        except Exception as e:
            print(f"NewsAPI error for '{q}': {e}")
            continue
        for a in arts:
            title = a.get("title") or ""
            desc = a.get("description") or ""
            url = a.get("url")
            published = a.get("publishedAt")
            source = (a.get("source") or {}).get("name","")
            if not url:
                continue
            rawtext = (title + " " + desc).strip()
            item = {
                "id": url,
                "title": title.strip(),
                "url": url,
                "published_at": published,
                "source": source,
                "summary": summarize_text(title, desc),
                "impact": impact_score(rawtext),
                "category": classify_text(rawtext),
                "raw": rawtext
            }
            if upsert_item(conn, item):
                collected.append(item)
    print(f"Stored {len(collected)} new items.")
    return collected

# ---- Build digest list (top N by impact, then recency) ----
def build_digest(conn, top_n=6):
    c = conn.cursor()
    c.execute("""
        SELECT id,title,url,published_at,source,summary,impact,category
        FROM items
        ORDER BY impact DESC, published_at DESC
        LIMIT ?
    """, (top_n,))
    rows = c.fetchall()
    items = [{
        "id": r[0], "title": r[1], "url": r[2], "published_at": r[3],
        "source": r[4], "summary": r[5], "impact": r[6], "category": r[7]
    } for r in rows]
    highlight = ", ".join([it['title'] for it in items[:3]]) if items else "No major caviar headlines in the last 72 hours"
    return items, highlight

# ---- Render HTML digest from template ----
def render_html(items, highlight):
    tpl = env.get_template("digest_template.html")
    return tpl.render(
        date=datetime.utcnow().strftime("%B %d, %Y"),
        items=items,
        highlight_summary=highlight
    )

# ---- Send with SendGrid ----
def send_email_html(subject, html_body):
    if not (SENDGRID_API_KEY and FROM_EMAIL and TO_EMAIL):
        raise RuntimeError("Missing SENDGRID_API_KEY / FROM_EMAIL / TO_EMAIL")
    sg = SendGridAPIClient(SENDGRID_API_KEY)
    message = Mail(
        from_email=FROM_EMAIL,
        to_emails=TO_EMAIL,
        subject=subject,
        html_content=html_body
    )
    resp = sg.send(message)
    print("Email send status:", resp.status_code)
    return resp.status_code

def main():
    # Safety checks
    if not NEWSAPI_KEY:
        print("ERROR: NEWSAPI_KEY not set.")
        return
    if not (SENDGRID_API_KEY and FROM_EMAIL and TO_EMAIL):
        print("ERROR: SendGrid vars not set.")
        return

    conn = init_db(DB_PATH)
    fetch_and_store(conn, hours=72)  # 3-day window for better coverage
    items, highlight = build_digest(conn, top_n=DIGEST_ITEMS)

    # Always send an email, even if there are 0 items
    html_body = render_html(items, highlight)
    subject = f"Daily Caviar Digest — {datetime.utcnow().strftime('%b %d, %Y')}"

    try:
        status = send_email_html(subject, html_body)
        if status in (200, 202):
            print(f"Digest sent to {TO_EMAIL}")
        else:
            print("SendGrid returned non-success status:", status)
    except Exception as e:
        print("Email error:", str(e))

if __name__ == "__main__":
    main()
