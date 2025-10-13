def scrape_product_page(url, site_selectors=None):
    """
    Extract one or more products from a product page.
    Prefers JSON-LD; falls back to common HTML selectors and meta tags.
    """
    if not robots_allowed(url): 
        print("ROBOTS disallow:", url)
        return []

    r = safe_get(url)
    if not (r and r.ok):
        print("GET failed:", url)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    out = []

    # 0) Try page <title>/H1 for product name fallback
    page_title = (soup.title.string if soup.title else "") or ""
    h1 = soup.find("h1")
    h1_text = h1.get_text(strip=True) if h1 else ""

    # 1) JSON-LD (best)
    for p in extract_ld_json_products(soup):
        size_g, size_label = parse_size(p["name"] + " " + p.get("desc",""))
        price = p["price"]
        if price is None:
            cur, price = norm_price_currency(soup.get_text(" "))
        else:
            cur = p["currency"] or "USD"
        name = p["name"] or h1_text or page_title
        if not name:
            continue
        # must say caviar somewhere in name
        if not CAVIAR_WORD.search(name.lower()):
            continue
        if not size_g or not price:
            # try to find size in other places
            if not size_g:
                size_g, size_label = parse_size(soup.get_text(" "))
            if not price:
                cur, price = norm_price_currency(soup.get_text(" "))
        if not (size_g and price):
            continue
        out.append({
            "name": name, "price": price, "currency": cur,
            "size_g": size_g, "size_label": size_label or "n/a",
            "per_g": price/size_g, "url": url
        })

    # 2) Fallback: HTML selectors and meta tags (common patterns)
    if not out:
        # (a) Name
        name = h1_text or page_title

        # (b) Price via common selectors/meta
        price = None
        currency = "USD"

        # CSS selectors from YAML (site-specific, optional)
        if site_selectors:
            try_selectors = []
            if site_selectors.get("price"): try_selectors.append(site_selectors["price"])
            if site_selectors.get("name") and not name:
                n = soup.select_one(site_selectors["name"])
                if n: name = n.get_text(strip=True)
        else:
            try_selectors = []

        # Generic price selectors (Shopify / common themes)
        generic_price_selectors = [
            "[itemprop='price']", ".price-item--regular", ".price", ".product-price",
            "meta[property='og:price:amount']::attr(content)",
            "meta[name='twitter:data1']::attr(content)",
            "[data-price]"
        ]
        try_selectors.extend(generic_price_selectors)

        # Evaluate selectors
        for sel in try_selectors:
            node = None
            attr = None
            if "::attr(" in sel:
                # attribute form like meta[property='og:price:amount']::attr(content)
                css, attr = sel.split("::attr(")
                attr = attr.replace(")","").strip()
                node = soup.select_one(css.strip())
                if node and node.has_attr(attr):
                    val = node[attr]
                else:
                    val = None
            else:
                node = soup.select_one(sel)
                val = node.get_text(" ", strip=True) if node else None

            if val:
                cur, maybe_price = norm_price_currency(val)
                if maybe_price:
                    price = maybe_price
                    currency = cur
                    break

        # Try meta currency if needed
        if price and currency == "USD":
            meta_cur = soup.select_one("meta[property='og:price:currency']")
            if meta_cur and meta_cur.has_attr("content"):
                currency = meta_cur["content"]

        # (c) Size from text anywhere
        size_g, size_label = parse_size((name or "") + " " + soup.get_text(" "))

        # Keep only legitimate caviar items with both price & size
        if name and CAVIAR_WORD.search((name or "").lower()) and price and size_g:
            out.append({
                "name": name, "price": price, "currency": currency,
                "size_g": size_g, "size_label": size_label or "n/a",
                "per_g": price/size_g, "url": url
            })

    return out


def crawl_site(site):
    """
    Crawl a site's category/start URLs; discover product links; visit them; return parsed products.
    Respects optional selectors from YAML: product_link, name, price.
    """
    results=[]
    start_urls = site.get("start_urls",[])
    sel = site.get("selectors",{}) or {}
    domain_whitelist = set(site.get("allow_domains") or [])
    site_name = site.get("name") or (domain_whitelist and next(iter(domain_whitelist))) or "site"

    for start in start_urls:
        r = safe_get(start)
        if not (r and r.ok):
            print("START failed:", start)
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
                if domain_whitelist and urlparse(full).netloc not in domain_whitelist: 
                    continue
                links.add(full)

        # 2) Heuristic fallback
        for a in soup.find_all("a", href=True):
            href=a["href"]
            lower=href.lower()
            if any(x in lower for x in ["product","products","/p/","/prod/","/item/","/collections/","/collection/","/shop/","/store/","caviar"]):
                full=urljoin(start, href)
                if domain_whitelist and urlparse(full).netloc not in domain_whitelist:
                    continue
                links.add(full)

        links_list = list(links)[:80]  # cap per site
        print(f"[{site_name}] found {len(links_list)} product-ish links on {start}")

        # Visit each product link
        kept=0
        for url in links_list:
            prods = scrape_product_page(url, site_selectors=sel)
            for p in prods:
                p["site"]=site_name
                results.append(p)
                kept+=1
        print(f"[{site_name}] kept {kept} products from {start}")

        time.sleep(0.5)  # politeness delay

    return results
