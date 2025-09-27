# connectors/custom_barclays.py
import re, time
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "JobScoutBot/0.1 (+github actions)"}
REQ_TIMEOUT = 20

def _soup(url):
    r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def _next_page_url(current_url: str, next_page_number: int) -> str:
    parsed = urlparse(current_url)
    parts = parsed.path.rstrip("/").split("/")
    if parts and parts[-1].isdigit():
        parts[-1] = str(next_page_number)
    else:
        parts.append(str(next_page_number))
    new_path = "/".join(parts)
    return parsed._replace(path=new_path).geturl()

def fetch(endpoint_url: str, max_pages: int = 2):
    """Yield dicts: {title, detail_url, location, date_live, description}"""
    base = "https://search.jobs.barclays"
    out = []
    seen = set()
    for page in range(1, max_pages + 1):
        url = _next_page_url(endpoint_url, page)
        soup = _soup(url)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/job/" not in href: 
                continue
            detail_url = urljoin(base, href)
            title = a.get_text(strip=True)
            if not title or detail_url in seen: 
                continue
            seen.add(detail_url)

            # try to grab nearby location and date text
            location, date_live = None, None
            nxt, hops = a.find_next(string=True), 0
            while nxt and hops < 12:
                txt = (nxt.strip() if isinstance(nxt, str) else "").strip()
                if txt and ("," in txt or "India" in txt):
                    location = txt
                if re.search(r"\b(\d{1,2}\s+[A-Za-z]{3}|\d{1,2}/\d{2}/\d{4})\b", txt):
                    date_live = txt
                nxt = nxt.next_element
                hops += 1

            # fetch detail page (lightweight)
            try:
                dsoup = _soup(detail_url)
            except Exception:
                dsoup = None
            desc = None
            if dsoup:
                p = dsoup.find("p")
                desc = p.get_text(" ", strip=True) if p else None

            out.append({
                "title": title,
                "detail_url": detail_url,
                "location": location,
                "posted": date_live,
                "description": desc
            })
        time.sleep(2)
    return out
