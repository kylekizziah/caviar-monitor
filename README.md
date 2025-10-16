# 🐟 Daily Caviar Digest

Automated scraper + email digest for verified **sturgeon caviar tins/jars** in the U.S. 
Parses **species**, **grade**, **size (oz / g)**, price, and **$/g**. Sends a daily email via **SendGrid**.

## What you get
- Filters out accessories/gift sets and non-sturgeon roe.
- Only includes items where **species** is stated (Beluga, Kaluga, Amur, Osetra, Sevruga, Siberian, White Sturgeon, Sterlet, Hackleback).
- Buckets by size: For 2 (30–50 g), For 4 (~100 g), Specials (125–250 g), Bulk (500 g+).
- Picks cheapest options per bucket.

## Files

## One-time setup (Render)
1. Connect repo in **Render** as a **Cron Job** or **Background Worker**.
2. **Build Command:** `pip install -r requirements.txt`
3. **Start / Command:** `python main.py`
4. **Schedule:** `0 13 * * *` (9:00 AM ET)
5. **Env Vars:**
   - `SENDGRID_API_KEY` = your SendGrid key
   - `FROM_EMAIL` = a verified sender in SendGrid
   - `TO_EMAIL` = `kylekizziah@gmail.com` (or your address)
   - (optional) `RUN_LIMIT_SECONDS` = `150`

## Seed product URLs (important)
Edit `price_sites.yaml` and add a few **real product page URLs** under `seed_product_urls:` for 2–4 vendors so the first runs return data.

## Manual test
Trigger a Run in Render. Check logs for `Email send status: 202`.
