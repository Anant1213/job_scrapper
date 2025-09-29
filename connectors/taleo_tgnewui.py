# connectors/taleo_tgnewui.py
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

HDRS = {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}

def _parse(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    out, seen = [], set()
    # job links look like .../JobDetail/<Title>/<JobId>
    for a in soup.select("a[href*='JobDetail/']"):
        title = a.get_text(" ", strip=True)
        href  = a.get("href") or ""
        if not title or not href:
            continue
        if href.startswith("/"):
            href = urljoin(base_url, href)
        loc = None
        root = a.find_parent("li") or a.find_parent("div")
        if root:
            el = root.select_one(".jobLocation, .job-location, .location")
            if el:
                loc = el.get_text(" ", strip=True)
        key = (title, href)
        if key in seen:
            continue
        seen.add(key)
        out.append({"title": title, "location": loc, "detail_url": href, "description": None, "req_id": None, "posted": None})
    return out

def fetch(search_url: str, max_pages: int = 4):
    # Many TGNewUI searches are server-rendered at either:
    #  - .../Search/home/HomeWithPreLoad?...locationSearch=India
    #  - .../Search/home/SearchResults?...keyword=India&locationSearch=India
    r = requests.get(search_url, headers=HDRS, timeout=45)
    if not r.ok:
        return []
    jobs = _parse(r.text, search_url)
    return jobs
