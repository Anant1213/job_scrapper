# connectors/bnpp_group.py
import requests, time
from bs4 import BeautifulSoup
from urllib.parse import urljoin

HDRS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}

def fetch(india_landing_url: str, max_pages: int = 3):
    # The India landing page contains server-rendered job cards with links.
    # We keep it simple (one page) to avoid timeouts.
    r = requests.get(india_landing_url, headers=HDRS, timeout=45)
    if not r.ok:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    out, seen = [], set()
    for a in soup.select("a[href*='/job-offer/'], a[href*='/en/job/']"):
        title = a.get_text(" ", strip=True)
        href  = a.get("href") or ""
        if not title or not href: 
            continue
        if href.startswith("/"):
            href = urljoin(india_landing_url, href)
        # Try to find location nearby
        loc = None
        parent = a.find_parent("article") or a.parent
        if parent:
            el = parent.select_one(".job__item__location, .location, [class*='location']")
            if el:
                loc = el.get_text(" ", strip=True)
        key = (title, href)
        if key in seen:
            continue
        seen.add(key)
        out.append({"title": title, "location": loc or "India", "detail_url": href, "description": None, "req_id": None, "posted": None})
    return out
