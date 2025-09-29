# connectors/citi_custom.py
import re, time, requests
from bs4 import BeautifulSoup

HDRS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}

def _is_india_collection(url: str) -> bool:
    return bool(re.search(r"/search-jobs/India/", url, re.I)) or "locationsearch=India" in url.lower()

def _parse(html, base, force_india: bool):
    soup = BeautifulSoup(html, "html.parser")
    out, seen = [], set()
    for a in soup.select("a[href*='/job/'], a[href*='/jobs/']"):
        title = a.get_text(" ", strip=True)
        href  = a.get("href") or ""
        if not title or not href:
            continue
        li = a.find_parent("li") or a.find_parent("article") or a.parent
        loc = None
        if li:
            loc_el = li.select_one(".job-location, .location, [data-ph-at-text='location']")
            if loc_el:
                loc = loc_el.get_text(" ", strip=True)
        if not loc and force_india:
            loc = "India"
        k = (title, href)
        if k in seen: 
            continue
        seen.add(k)
        out.append({"title": title, "location": loc, "detail_url": href, "description": None, "req_id": None, "posted": None})
    return out

def fetch(india_base_url: str, max_pages: int = 10):
    all_rows = []
    force_india = _is_india_collection(india_base_url)

    # simple pager support: ...?page=1,2,...
    base = india_base_url
    for p in range(1, max_pages+1):
        url = re.sub(r"([?&])page=\d+", rf"\g<1>page={p}", base) if "page=" in base else (base + ("&" if "?" in base else "?") + f"page={p}")
        r = requests.get(url, headers=HDRS, timeout=45)
        if not r.ok:
            break
        rows = _parse(r.text, india_base_url, force_india)
        if not rows:
            break
        all_rows.extend(rows)
        time.sleep(0.3)
    return all_rows
