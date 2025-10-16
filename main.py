import os
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader, select_autoescape, TemplateNotFound
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from caviar_scraper import run_scrape, group_and_pick

# --- Paths & env ---
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
YAML_PATH = BASE_DIR / "price_sites.yaml"

load_dotenv()

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
FROM_EMAIL       = os.getenv("FROM_EMAIL")
TO_EMAIL         = os.getenv("TO_EMAIL", "kylekizziah@gmail.com")

# --- Templates ---
env = Environment(
    loader=FileSystemLoader(str(TEMPLATES_DIR)),
    autoescape=select_autoescape(["html","xml"])
)

def render_html(date_str, top_picks, buckets):
    try:
        tpl = env.get_template("digest_template.html")
        return tpl.render(date=date_str, top_picks=top_picks, buckets=buckets)
    except TemplateNotFound:
        # Fallback minimal HTML so a missing template never breaks the cron
        body = [f"<h1>🐟 Daily Caviar Digest — {date_str}</h1>",
                "<p>(Template missing; using fallback.)</p>",
                "<table border='1' cellpadding='6' cellspacing='0'>"
                "<tr><th>Vendor</th><th>Species/Grade</th><th>Size</th><th>Price</th><th>$ / g</th><th>Link</th></tr>"]
        for bname, items in top_picks.items():
            for it in items:
                grade = f" ({it['grade']})" if it.get("grade") else ""
                body.append(
                    f"<tr><td>{it.get('vendor','')}</td>"
                    f"<td>{it.get('species','')}{grade}</td>"
                    f"<td>{it.get('size_label','')}</td>"
                    f"<td>{it.get('currency','USD')} {it.get('price',0):.2f}</td>"
                    f"<td>{it.get('per_g',0):.2f}</td>"
                    f"<td><a href='{it.get('url','#')}'>View</a></td></tr>"
                )
        body.append("</table>")
        return "".join(body)

def render_text(date_str, top_picks):
    lines = [f"🐟 Daily Caviar Digest — {date_str}",
             "Cheapest verified sturgeon caviar options across trusted producers.", ""]
    any_items = False
    for bucket, items in top_picks.items():
        if not items:
            continue
        any_items = True
        lines.append(bucket + ":")
        for it in items:
            grade = f" ({it['grade']})" if it.get("grade") else ""
            lines.append(
                f"• {it['vendor']} — {it['species']}{grade} {it['size_label']} — "
                f"{it['currency']} {it['price']:.2f} (${it['per_g']:.2f}/g) — {it.get('origin_state','US')}"
            )
        lines.append("")
    if not any_items:
        lines.append("No caviar listings with verified species found in this run.")
    lines.append("Data auto-gathered; prices may change. Edit price_sites.yaml to add/remove sellers.")
    return "\n".join(lines)

def send_via_sendgrid(subject, html_body, text_body):
    missing = [k for k,v in {
        "SENDGRID_API_KEY": SENDGRID_API_KEY,
        "FROM_EMAIL": FROM_EMAIL,
        "TO_EMAIL": TO_EMAIL
    }.items() if not v]
    if missing:
        print("Missing env vars:", ", ".join(missing))
        return  # Do not crash cron; just log

    sg = SendGridAPIClient(SENDGRID_API_KEY)
    msg = Mail(from_email=FROM_EMAIL, to_emails=TO_EMAIL,
               subject=subject, html_content=html_body, plain_text_content=text_body)
    resp = sg.send(msg)
    print("Email send status:", resp.status_code)

def main():
    # scrape -> pick -> render -> send
    results = run_scrape(str(YAML_PATH))
    buckets, top_picks = group_and_pick(results)
    date_str = datetime.now().strftime("%B %d, %Y")
    html = render_html(date_str, top_picks, buckets)
    text = render_text(date_str, top_picks)
    subject = f"🐟 Daily Caviar Digest — {date_str}"
    send_via_sendgrid(subject, html, text)
    print("Digest attempted for", TO_EMAIL)

if __name__ == "__main__":
    main()
