# connectors/citi_custom.py
import re, time, requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urlencode

HDRS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}

def _page_url(base, page):
    u = list(urlparse(base))
    qs = parse_qs(u[4])
    qs["page"] = [str(page)]
    u[4] = urlencode(qs, doseq=True)
    return reassemble(u)

def reassemble(parts):
    return f"{parts[0]}://{parts[1]}{parts[2]}{'?' + parts[4] if parts[4] else ''}"

def _parse(html, base):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.select("a[href*='/job/'], a[href*='/jobs/']"):
        title = a.get_text(" ", strip=True)
        href  = a.get("href") or ""
        if not href.startswith("http"):
            # relative → keep as-is (site makes it absolute)
            pass
        li = a.find_parent("li") or a.find_parent("article") or a.parent
        loc = None
        if li:
            loc_el = li.select_one(".job-location, .location, [data-ph-at-text='location']")
            if loc_el:
                loc = loc_el.get_text(" ", strip=True)
        if title and href:
            out.append({
                "title": title, "location": loc, "detail_url": href,
                "description": None, "req_id": None, "posted": None
            })
    # de-dupe
    seen, dedup = set(), []
    for j in out:
        k = (j["title"], j["detail_url"])
        if k not in seen:
            seen.add(k); dedup.append(j)
    return dedup

def fetch(india_base_url: str, max_pages: int = 10):
    all_rows = []
    for p in range(1, max_pages+1):
        url = india_base_url if p == 1 else _page_url(india_base_url, p)
        r = requests.get(url, headers=HDRS, timeout=45)
        if not r.ok:
            break
        rows = _parse(r.text, india_base_url)
        if not rows:
            break
        all_rows.extend(rows)
        time.sleep(0.3)
    return all_rows
