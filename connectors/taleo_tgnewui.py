# connectors/taleo_tgnewui.py
import time, re, math
import requests
from bs4 import BeautifulSoup

HDRS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

def _extract_jobs_from_html(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    # Taleo TGnewUI usually wraps cards in .oracletaleocwsv2-...
    # but we’ll be liberal:
    for a in soup.select('a[href*="/job/"], a[href*="JobDetail"], a[href*="/jobdetail"]'):
        title = a.get_text(strip=True)
        href = a.get("href") or ""
        if not title or len(title) < 3:
            continue
        # find a nearby location text
        loc = None
        card = a.find_parent(["div","li","tr"])
        if card:
            # common selectors around job cards
            loc_el = card.select_one('.jobLocation, [data-ph-id*="location"], .location, .job-location, span[title="Location"]')
            if loc_el:
                loc = loc_el.get_text(" ", strip=True)
        # normalize link
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            # resolve relative
            m = re.match(r"^https?://[^/]+", base_url)
            if m:
                href = m.group(0) + href
        items.append({
            "title": title,
            "location": loc,
            "detail_url": href,
            "description": None,
            "req_id": None,
            "posted": None,
        })
    # de-dupe by (title, url)
    seen, dedup = set(), []
    for it in items:
        k = (it["title"], it["detail_url"])
        if k in seen: 
            continue
        seen.add(k)
        dedup.append(it)
    return dedup

def fetch(base_search_url: str, max_pages: int = 5, sleep_s: float = 1.0):
    """
    base_search_url: the TGnewUI 'HomeWithPreLoad' url already filtered to India,
    e.g. https://jobs.ubs.com/TGnewUI/Search/home/HomeWithPreLoad?partnerid=25008&siteid=5012&PageType=searchResults&SearchType=linkquery&LinkID=4138&locationSearch=India
    We’ll try simple paging by &startrow=<n> when present; otherwise just scrape first page.
    """
    s = requests.Session()
    s.headers.update(HDRS)
    all_jobs = []

    # Page 1
    r = s.get(base_search_url, timeout=30)
    r.raise_for_status()
    page_jobs = _extract_jobs_from_html(r.text, base_search_url)
    all_jobs.extend(page_jobs)

    # Try the common startrow pagination (increments by 25)
    if max_pages > 1:
        # find total if present
        total = None
        m = re.search(r"(\d+)\s+(?i:jobs?)", r.text)
        if m:
            try:
                total = int(m.group(1))
            except:
                total = None
        per = max(1, len(page_jobs))
        pages = min(max_pages, math.ceil((total or (per*max_pages))/per))

        start = per
        for _ in range(2, pages+1):
            if "startrow=" in base_search_url:
                url = re.sub(r"startrow=\d+", f"startrow={start}", base_search_url)
            else:
                sep = "&" if "?" in base_search_url else "?"
                url = f"{base_search_url}{sep}startrow={start}"
            r2 = s.get(url, timeout=30)
            if r2.status_code != 200:
                break
            jobs = _extract_jobs_from_html(r2.text, base_search_url)
            if not jobs:
                break
            all_jobs.extend(jobs)
            start += per
            time.sleep(sleep_s)

    return all_jobs
