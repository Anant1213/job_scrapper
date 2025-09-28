# connectors/oracle_cx.py
import time, json, requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlencode

BASE_HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
}
AJAX = {
    **BASE_HDRS,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
}

def _parse_html(html, base):
    soup = BeautifulSoup(html, "html.parser")
    jobs = []
    # broad selectors that match CE job cards
    cards = soup.select("a[href*='/requisitions/'], a[href*='/job/']")
    seen = set()
    for a in cards:
        title = a.get_text(" ", strip=True)
        href  = a.get("href") or ""
        if href.startswith("/"):
            href = urljoin(base, href)
        # try to find a nearby location label
        loc = None
        for sib in a.parents:
            lab = sib.select_one(".job-location, [data-automation-id='locations'], .location")
            if lab:
                loc = lab.get_text(" ", strip=True)
                break
        key = (title, href)
        if title and href and key not in seen:
            seen.add(key)
            jobs.append({"title": title, "location": loc, "detail_url": href, "description": None, "req_id": None, "posted": None})
    return jobs

def _json_search(site_base):
    url = urljoin(site_base if site_base.endswith("/") else site_base + "/", "rest/api/jobs/search")
    payload = {"keyword":"", "location":"India", "locationLevel":"country", "page":1}
    r = requests.post(url, headers=AJAX, data=json.dumps(payload), timeout=45)
    if r.ok and r.headers.get("content-type","").startswith("application/json"):
        return r.json()
    # fallback GET
    qs = urlencode({"keyword":"", "location":"India", "locationLevel":"country", "page":1})
    r2 = requests.get(f"{url}?{qs}", headers=AJAX, timeout=45)
    if r2.ok and r2.headers.get("content-type","").startswith("application/json"):
        return r2.json()
    return None

def fetch(endpoint_url: str, max_pages: int = 6):
    # endpoint: .../hcmUI/CandidateExperience/en/sites/<SITE>/requisitions
    site_base = endpoint_url.rsplit("/requisitions", 1)[0]
    all_jobs = []
    try:
        js = _json_search(site_base)
        if js and js.get("jobs"):
            for it in js["jobs"]:
                all_jobs.append({
                    "title": it.get("title"),
                    "location": it.get("location"),
                    "detail_url": urljoin(site_base, it.get("jobUrl") or ""),
                    "description": None,
                    "req_id": it.get("RequisitionNumber") or it.get("Id"),
                    "posted": it.get("postedDate"),
                })
            return all_jobs
    except Exception:
        pass

    # HTML fallback with simple paging
    for p in range(1, max_pages+1):
        sep = "&" if "?" in endpoint_url else "?"
        url = f"{endpoint_url}{sep}location=India&locationLevel=country&page={p}"
        r = requests.get(url, headers=BASE_HDRS, timeout=45)
        if not r.ok:
            break
        jobs = _parse_html(r.text, site_base)
        if not jobs:
            break
        all_jobs.extend(jobs)
        time.sleep(0.4)
    return all_jobs
