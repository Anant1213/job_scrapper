# connectors/bnpp_group.py
import time, re, urllib.parse
import requests
from bs4 import BeautifulSoup

HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

def _parse_jobs(html, base):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    # Cards often under links to /en/job-offer/...
    for a in soup.select('a[href*="/en/job-offer/"], a[href*="/en/job-offers/"]'):
        title = a.get_text(" ", strip=True)
        href  = a.get("href") or ""
        if not title or len(title) < 3:
            continue
        if href.startswith("/"):
            href = "https://group.bnpparibas" + href
        # hunt for location nearby
        loc = None
        card = a.find_parent(["article","div","li"])
        if card:
            loc_el = card.select_one(".job-location, .c-card-location, .location, [class*='location']")
            if loc_el:
                loc = loc_el.get_text(" ", strip=True)
            # sometimes location sits in a <p> with 'Mumbai' etc.
            if not loc:
                txt = card.get_text(" ", strip=True)
                m = re.search(r"(Bengaluru|Bangalore|Mumbai|Pune|Chennai|Hyderabad|Noida|Gurugram|Gurgaon|India)", txt, re.I)
                if m:
                    loc = m.group(0)
        out.append({
            "title": title, "location": loc, "detail_url": href,
            "description": None, "req_id": None, "posted": None
        })
    # de-dupe
    seen, dedup = set(), []
    for j in out:
        k = (j["title"], j["detail_url"])
        if k in seen: 
            continue
        seen.add(k)
        dedup.append(j)
    return dedup

def fetch(india_landing_url: str, max_pages: int = 6, sleep_s: float = 0.7):
    """
    india_landing_url:
      https://group.bnpparibas/en/careers/all-job-offers/india
    The site loads more via a 'See more job offers' link; if we can’t detect the ajax,
    we at least parse page 1. We also try adding ?page=N as a fallback.
    """
    s = requests.Session()
    s.headers.update(HDRS)
    all_jobs = []

    r = s.get(india_landing_url, timeout=30)
    r.raise_for_status()
    html = r.text
    items = _parse_jobs(html, india_landing_url)
    all_jobs.extend(items)

    # Try naive pagination with ?page=2,3,...
    base = india_landing_url
    for p in range(2, max_pages+1):
        sep = "&" if "?" in base else "?"
        url = f"{base}{sep}page={p}"
        r2 = s.get(url, timeout=30)
        if r2.status_code != 200:
            break
        more = _parse_jobs(r2.text, url)
        if not more:
            break
        all_jobs.extend(more)
        time.sleep(sleep_s)

    return all_jobs
