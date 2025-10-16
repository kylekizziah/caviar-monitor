import os
import smtplib
import traceback
from pathlib import Path
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader, select_autoescape, TemplateNotFound

# Local imports
from caviar_scraper import run_scrape, proximity_score, vendor_state, GRADE_RANK

load_dotenv()

# ----- Absolute paths (works on Render, Docker, anywhere) -----
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
YAML_PATH = BASE_DIR / "price_sites.yaml"

DEST_EMAIL = os.getenv("DEST_EMAIL", "kylekizziah@gmail.com")
GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
TZ = os.getenv("TZ", "America/New_York")

# ---------- Bucketing / ranking ----------
def bucket_for_size(g):
    if g is None: return "Other"
    if g <= 50:   return "For 2 (30–50 g)"
    if g <= 110:  return "For 4 (~100 g)"
    if g <= 260:  return "Specials (125–250 g)"
    return "Bulk (500 g+)"

def best_sort_key(item):
    # grade → $/g → proximity
    grade_rank = GRADE_RANK.get((item.get("grade") or "").lower(), 99)
    per_g = item.get("per_g") or 1e9
    prox = proximity_score(item.get("origin_state") or vendor_state(item.get("vendor") or ""))
    return (grade_rank, per_g, prox)

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

# ---------- Templates ----------
env = Environment(
    loader=FileSystemLoader(str(TEMPLATES_DIR)),
    autoescape=select_autoescape(["html", "xml"])
)

def render_html(date_str, top_picks, buckets):
    # You said you don't want to rename: use your file name
    try:
        tpl = env.get_template("digest_template.html")
        return tpl.render(date=date_str, top_picks=top_picks, buckets=buckets)
    except TemplateNotFound as e:
        # Fallback minimal HTML so the job never crashes
        rows = []
        for bname, items in top_picks.items():
            for it in items:
                rows.append(
                    f"<tr><td>{it.get('vendor','')}</td>"
                    f"<td>{it.get('species','')}"
                    f"{' ('+it['grade']+')' if it.get('grade') else ''}</td>"
                    f"<td>{it.get('size_label','')}</td>"
                    f"<td>{it.get('currency','USD')} {it.get('price',0):.2f}</td>"
                    f"<td>{it.get('per_g',0):.2f}</td>"
                    f"<td><a href='{it.get('url','#')}'>View</a></td></tr>"
                )
        body = (
            f"<h1>🐟 Daily Caviar Digest — {date_str}</h1>"
            "<p>(Template not found; using fallback HTML.)</p>"
            "<table border='1' cellpadding='6' cellspacing='0'>"
            "<tr><th>Vendor</th><th>Species/Grade</th><th>Size</th><th>Price</th><th>$ / g</th><th>Link</th></tr>"
            + "".join(rows) + "</table>"
        )
        return body

def render_text(date_str, top_picks, buckets):
    lines = [
        f"🐟 Daily Caviar Digest — {date_str}",
        "Cheapest verified sturgeon caviar options across trusted producers.",
        ""
    ]
    any_items = False
    for bname, items in top_picks.items():
        if not items:
            continue
        any_items = True
        lines.append(f"{bname}:")
        for it in items:
            grade_str = f" ({it['grade']})" if it.get("grade") else ""
            vendor = it.get("vendor","")
            species = it.get("species","")
            size_label = it.get("size_label","")
            currency = it.get("currency","USD")
            price = it.get("price", 0.0)
            per_g = it.get("per_g", 0.0)
            origin = it.get("origin_state") or "US"
            lines.append(
                f"• {vendor} — {species}{grade_str} {size_label} — "
                f"{currency} {price:.2f} (${per_g:.2f}/g) — {origin}"
            )
        lines.append("")
    if not any_items:
        lines.append("No caviar listings with verified species found in this run.")
    lines.append("Data gathered automatically from verified producers & farms. Prices may change. Edit price_sites.yaml to add/remove sellers.")
    return "\n".join(lines)

# ---------- Email ----------
def send_email(subject, html_body, text_body):
    missing = [k for k,v in {
        "GMAIL_USER": GMAIL_USER,
        "GMAIL_APP_PASSWORD": GMAIL_APP_PASSWORD,
        "DEST_EMAIL": DEST_EMAIL
    }.items() if not v]
    if missing:
        print("Missing env vars:", ", ".join(missing))
        return
    msg = MIMEMultipart("alternative")
    msg["From"] = GMAIL_USER
    msg["To"] = DEST_EMAIL
    msg["Subject"] = subject
    msg.attach(MIMEText(text_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))
    with smtplib.SMTP("smtp.gmail.com", 587) as s:
        s.starttls()
        s.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        s.sendmail(GMAIL_USER, [DEST_EMAIL], msg.as_string())

def main():
    try:
        # Absolute YAML path so Render finds it even if CWD differs
        results = run_scrape(str(YAML_PATH))
        buckets, top_picks = group_and_pick(results)
        date_str = datetime.now().strftime("%B %d, %Y")
        html = render_html(date_str, top_picks, buckets)
        text = render_text(date_str, top_picks, buckets)
        subject = f"🐟 Daily Caviar Digest — {date_str}"
        send_email(subject, html, text)
        print("Digest sent to", DEST_EMAIL)
    except Exception as e:
        print("FATAL ERROR in email_digest.py:", e)
        print(traceback.format_exc())
        # Re-raise so Render shows 'Exit 1' with traceback
        raise

if __name__ == "__main__":
    main()
