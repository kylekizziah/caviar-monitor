# Optional helper; main.py already sends via SendGrid.
# You can leave this file as-is or ignore it.

import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader, select_autoescape
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from caviar_scraper import run_scrape, group_and_pick

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
YAML_PATH = BASE_DIR / "price_sites.yaml"

load_dotenv()
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
FROM_EMAIL       = os.getenv("FROM_EMAIL")
TO_EMAIL         = os.getenv("TO_EMAIL", "kylekizziah@gmail.com")

env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)),
                  autoescape=select_autoescape(["html","xml"]))

def main():
    rows = run_scrape(str(YAML_PATH))
    buckets, top_picks = group_and_pick(rows)
    date_str = datetime.now().strftime("%B %d, %Y")
    html = env.get_template("digest_template.html").render(
        date=date_str, top_picks=top_picks, buckets=buckets
    )
    text = f"Daily Caviar Digest — {date_str}"
    msg = Mail(from_email=FROM_EMAIL, to_emails=TO_EMAIL,
               subject=f"🐟 Daily Caviar Digest — {date_str}",
               html_content=html, plain_text_content=text)
    sg = SendGridAPIClient(SENDGRID_API_KEY)
    resp = sg.send(msg)
    print("Email send status:", resp.status_code)

if __name__ == "__main__":
    main()
