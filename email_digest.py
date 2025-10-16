import os
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader, select_autoescape

from caviar_scraper import run_scrape, proximity_score, vendor_state, GRADE_RANK

load_dotenv()

DEST_EMAIL = os.getenv("DEST_EMAIL", "kylekizziah@gmail.com")
GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
TZ = os.getenv("TZ","America/New_York")

# Buckets
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
    # Keep only tins/jars with size and species
    goods = [r for r in rows if r.get("size_g") and r.get("species")]
    # Bucket
    buckets = {}
    for r in goods:
        b = bucket_for_size(r["size_g"])
        buckets.setdefault(b, []).append(r)
    # Sort each bucket and pick top N
    top_picks = {}
    for b, items in buckets.items():
        items_sorted = sorted(items, key=best_sort_key)
        top_picks[b] = items_sorted[:6]
    return buckets, top_picks

# ---------- Templates ----------
env = Environment(
    loader=FileSystemLoader("templates"),
    autoescape=select_autoescape(["html","xml"])
)

def render_html(date_str, top_picks, buckets):
    tpl = env.get_template("digest.html")
    return tpl.render(date=date_str, top_picks=top_picks, buckets=buckets)

def render_text(date_str, top_picks, buckets):
    lines = [f"🐟 Daily Caviar Digest — {date_str}", "Cheapest verified sturgeon caviar options across trusted producers.", ""]
    any_items = False
    for bname, items in top_picks.items():
        if not items: continue
        any_items = True
        lines.append(f"{bname}:")
        for it in items:
            lines.append(f"• {it['vendor']} — {it['species']}{f' ({it['grade']})' if it.get('grade') else ''} {it['size_label']} — {it['currency']} {it['price']:.2f} (${it['per_g']:.2f}/g) — {it.get('origin_state') or 'US'}")
        lines.append("")
    if not any_items:
        lines.append("No caviar listings with verified species found in this run.")
    lines.append("Data gathered automatically from verified producers & farms. Prices may change. Edit price_sites.yaml to add/remove sellers.")
    return "\n".join(lines)

# ---------- Email ----------
def send_email(subject, html_body, text_body):
    if not (GMAIL_USER and GMAIL_APP_PASSWORD and DEST_EMAIL):
        print("Missing Gmail SMTP env vars"); return
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
    results = run_scrape("price_sites.yaml")  # list of rows
    buckets, top_picks = group_and_pick(results)
    date_str = datetime.now().strftime("%B %d, %Y")
    html = render_html(date_str, top_picks, buckets)
    text = render_text(date_str, top_picks, buckets)
    subject = f"🐟 Daily Caviar Digest — {date_str}"
    send_email(subject, html, text)
    print("Digest sent to", DEST_EMAIL)

if __name__ == "__main__":
    main()
