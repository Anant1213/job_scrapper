# connectors/brassring_go.py
import time, requests
from bs4 import BeautifulSoup

HDRS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}

def fetch(go_page_url: str, max_pages: int = 6):
    r = requests.get(go_page_url, headers=HDRS, timeout=45)
    if not r.ok:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    jobs = []
    for card in soup.select("a[href*='/Nomura/job/'], a[href*='/go/']"):
        title = card.get_text(" ", strip=True)
        href  = card.get("href") or ""
        li = card.find_parent("li") or card.parent
        loc = None
        if li:
            loc_el = li.select_one(".jobLocation, .job-location, .location")
            if loc_el:
                loc = loc_el.get_text(" ", strip=True)
        if title and href:
            jobs.append({"title": title, "location": loc, "detail_url": href, "description": None, "req_id": None, "posted": None})
    # de-dupe
    seen, out = set(), []
    for j in jobs:
        k = (j["title"], j["detail_url"])
        if k not in seen:
            seen.add(k); out.append(j)
    return out
