# connectors/citi_custom.py
import time, re
import requests
from bs4 import BeautifulSoup

HDRS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

def _parse(html, base):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    # Citi uses list items with job cards; be permissive
    for card in soup.select("li, div"):
        a = card.find("a", href=True)
        if not a: 
            continue
        href = a["href"]
        text = a.get_text(" ", strip=True)
        # job detail links include /job/ or /job-details/
        if not re.search(r"/job[s\-]*/", href, re.I):
            continue
        title = text
        # location often in small/span element
        loc = None
        loc_el = card.select_one(".job-location, .location, .joblist-location, [data-test='job-location']")
        if loc_el:
            loc = loc_el.get_text(" ", strip=True)
        # normalize link
        if href.startswith("/"):
            href = "https://jobs.citi.com" + href
        out.append({
            "title": title, "location": loc, "detail_url": href,
            "description": None, "req_id": None, "posted": None
        })
    # dedupe
    seen, dedup = set(), []
    for j in out:
        k = (j["title"], j["detail_url"])
        if k in seen: 
            continue
        seen.add(k)
        dedup.append(j)
    return dedup

def fetch(india_base_url: str, max_pages: int = 8, sleep_s: float = 0.6):
    """
    india_base_url example:
      https://jobs.citi.com/search-jobs/India/287/1
    We will try appending ?page=<n> or /?page=<n> patterns depending on site.
    """
    s = requests.Session()
    s.headers.update(HDRS)
    all_jobs = []

    def get(url):
        r = s.get(url, timeout=30)
        r.raise_for_status()
        return r.text

    # normalize to first page
    base = india_base_url
    if "page=" not in base:
        sep = "&" if "?" in base else "?"
        base = f"{base}{sep}page=1"

    for page in range(1, max_pages+1):
        url = re.sub(r"page=\d+", f"page={page}", base)
        html = get(url)
        items = _parse(html, base)
        if not items:
            break
        all_jobs.extend(items)
        time.sleep(sleep_s)

    return all_jobs
