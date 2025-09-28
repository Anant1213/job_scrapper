# connectors/brassring_go.py
import time, re
import requests
from bs4 import BeautifulSoup

HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

def _extract(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    jobs = []
    # BrassRing "go" pages: anchors inside table rows / divs with a job title
    for a in soup.select('a[href*="/go/"], a[href*="/job/"], a.jobTitle'):
        title = a.get_text(" ", strip=True)
        if not title or len(title) < 3:
            continue
        href = a.get("href") or ""
        if href.startswith("/"):
            m = re.match(r"^https?://[^/]+", base_url)
            if m:
                href = m.group(0) + href
        # location usually in sibling span or next cell
        loc = None
        row = a.find_parent(["tr","div","li"])
        if row:
            cand = row.select_one(".jobLocation, .location, [data-automation='job-location']")
            if cand:
                loc = cand.get_text(" ", strip=True)
        jobs.append({
            "title": title, "location": loc, "detail_url": href,
            "description": None, "req_id": None, "posted": None
        })
    # dedupe
    seen, out = set(), []
    for j in jobs:
        k = (j["title"], j["detail_url"])
        if k in seen: 
            continue
        seen.add(k)
        out.append(j)
    return out

def fetch(go_page_url: str, max_pages: int = 4, sleep_s: float = 0.8):
    """
    go_page_url e.g. https://careers.nomura.com/Nomura/go/Career-Opportunities-India/9050900/
    BrassRing paginates by appending an offset number at the end like /25/, /50/ ...
    We'll try that pattern.
    """
    s = requests.Session()
    s.headers.update(HDRS)
    all_jobs = []

    def get(url):
        r = s.get(url, timeout=30)
        r.raise_for_status()
        return r.text

    html = get(go_page_url)
    jobs = _extract(html, go_page_url)
    all_jobs.extend(jobs)

    # find page size heuristically (count items on first page)
    page_size = max(25, len(jobs) or 25)
    offset = page_size
    for _ in range(2, max_pages+1):
        # try "/{offset}/" pattern
        if go_page_url.endswith("/"):
            url = f"{go_page_url}{offset}/"
        else:
            url = f"{go_page_url}/{offset}/"
        try:
            html = get(url)
        except requests.HTTPError:
            break
        more = _extract(html, go_page_url)
        if not more:
            break
        all_jobs.extend(more)
        offset += page_size
        time.sleep(sleep_s)

    return all_jobs
