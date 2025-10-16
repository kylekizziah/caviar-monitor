def crawl_site(site_cfg, deadline):
    results = []
    vendor = site_cfg.get("name", "Vendor")

    # Safely normalize all optional fields to lists/dicts
    allow = set((site_cfg.get("allow_domains") or []))
    seed_urls = list(site_cfg.get("seed_product_urls") or [])
    start_urls = list(site_cfg.get("start_urls") or [])
    selectors = (site_cfg.get("selectors") or {})
    product_link_sel = selectors.get("product_link")

    def domain_ok(u):
        host = urlparse(u).netloc.lower().replace("www.", "")
        return (not allow) or (host in {d.lower().replace("www.", "") for d in allow})

    # ---- 1) Seed product URLs (robust to empty/None) ----
    for seed in seed_urls[:MAX_LINKS_PER_SITE]:
        if datetime.utcnow() > deadline:
            return results
        if not seed:
            continue
        low = seed.lower()
        if looks_like_product_url(low) and domain_ok(seed):
            results += scrape_product(seed, vendor)
            time.sleep(0.1)

    # ---- 2) Category pages -> discover product links ----
    for start in start_urls:
        if datetime.utcnow() > deadline:
            return results
        if not start:
            continue
        r = safe_get(start)
        if not (r and r.ok):
            continue
        soup = BeautifulSoup(r.text, "lxml")
        links = set()

        # Site-specific selector (if provided)
        if product_link_sel:
            for a in soup.select(product_link_sel):
                href = a.get("href")
                if not href:
                    continue
                full = urljoin(start, href)
                if domain_ok(full) and looks_like_product_url(full.lower()):
                    links.add(full)

        # Heuristic fallback
        for a in soup.find_all("a", href=True):
            full = urljoin(start, a["href"])
            if domain_ok(full) and looks_like_product_url(full.lower()):
                links.add(full)

        for u in list(links)[:MAX_LINKS_PER_SITE]:
            if datetime.utcnow() > deadline:
                break
            results += scrape_product(u, vendor)
            time.sleep(0.1)

    return results
