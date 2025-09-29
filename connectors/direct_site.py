# connectors/direct_site.py
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

def _extract_from_html(html: str, base_url: str, sel: dict, force_india=False):
    soup = BeautifulSoup(html, "html.parser")
    out, seen = [], set()
    card_sel = sel["card"]
    title_sel = sel.get("title") or "a"
    link_sel  = sel.get("link") or "a"
    loc_sel   = sel.get("location")
    posted_sel = sel.get("posted")

    for card in soup.select(card_sel):
        a = card.select_one(link_sel)
        if not a:
            continue
        href = a.get("href") or ""
        if href.startswith("/"):
            href = urljoin(base_url, href)
        title_el = card.select_one(title_sel) or a
        title = title_el.get_text(" ", strip=True) if title_el else None
        if not title or not href:
            continue

        loc = None
        if loc_sel:
            loc_el = card.select_one(loc_sel)
            if loc_el:
                loc = loc_el.get_text(" ", strip=True)
        if not loc and force_india:
            loc = "India"

        posted = None
        if posted_sel:
            p = card.select_one(posted_sel)
            if p:
                posted = p.get_text(" ", strip=True)

        key = (title, href)
        if key in seen:
            continue
        seen.add(key)

        out.append({
            "title": title,
            "location": loc,
            "detail_url": href,
            "description": None,
            "req_id": None,
            "posted": posted,
        })
    return out

def fetch(url: str, selectors: dict, max_pages: int = 1, page_param: str | None = None,
          start_page: int = 1, force_india: bool = False, delay_s: float = 0.5):
    """
    Generic static-page scraper.
    selectors = { card, title?, link?, location?, posted? }
    """
    all_rows = []
    base = url

    for page in range(start_page, start_page + max_pages):
        page_url = base
        if page_param:
            sep = "&" if "?" in base else "?"
            page_url = f"{base}{sep}{page_param}={page}"

        r = requests.get(page_url, headers=HDRS, timeout=45)
        if not r.ok:
            break
        rows = _extract_from_html(r.text, page_url, selectors, force_india=force_india)
        if not rows and page > start_page:
            break
        all_rows.extend(rows)
        time.sleep(delay_s)

    return all_rows
