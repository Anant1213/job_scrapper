# connectors/oracle_cx.py
"""
Generic Oracle Recruiting Cloud (Candidate Experience) fetcher.
Strategy:
  1) Call the public requisitions listing endpoint (HTML) with India filters.
  2) If HTML, parse job cards.
  3) Also try a JSON endpoint used by many sites: '/rest/api/jobs/search' POST.
We keep it robust without paid APIs or auth.
"""
import time, re, json
from urllib.parse import urlencode, urljoin
import requests
from bs4 import BeautifulSoup

HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
}

def _parse_html(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    # Common ORC job card selectors
    for card in soup.select("[data-automation-id='jobCard'], li, article, div"):
        a = card.select_one("a[href*='/requisitions/'], a[href*='/jobs/'], a[href*='/job/']")
        if not a:
            continue
        title = a.get_text(" ", strip=True)
        href = a.get("href") or ""
        if href.startswith("/"):
            href = urljoin(base_url, href)
        # location
        loc = None
        loc_el = card.select_one("[data-automation-id='locations'], .job-location, .location")
        if loc_el:
            loc = loc_el.get_text(" ", strip=True)
        # posted date if present
        posted = None
        pd = card.find(attrs={"data-automation-id": "postedDate"})
        if pd:
            posted = pd.get_text(" ", strip=True)
        items.append({
            "title": title, "location": loc, "detail_url": href,
            "description": None, "req_id": None, "posted": posted
        })
    # dedupe
    seen, out = set(), []
    for it in items:
        k = (it["title"], it["detail_url"])
        if k in seen: 
            continue
        seen.add(k)
        out.append(it)
    return out

def _html_list(base_reqs_url, page=1):
    # try ?location=India&locationLevel=country plus paging
    params = {"location": "India", "locationLevel": "country", "page": page}
    sep = "&" if "?" in base_reqs_url else "?"
    url = f"{base_reqs_url}{sep}{urlencode(params)}"
    r = requests.get(url, headers=HDRS, timeout=45)
    if r.status_code != 200:
        return [], False
    jobs = _parse_html(r.text, base_reqs_url)
    # crude: if fewer than ~5 jobs arrived or repeated, assume no more pages
    more = len(jobs) >= 5
    return jobs, more

def _json_search(site_base):
    """
    Attempt JSON endpoint used on many ORC tenants:
      POST {site_base}/rest/api/jobs/search
      body: {"keyword":"","location":"India","locationLevel":"country","page":1}
    """
    url = urljoin(site_base if site_base.endswith("/") else (site_base + "/"), "rest/api/jobs/search")
    body = {"keyword": "", "location": "India", "locationLevel": "country", "page": 1}
    r = requests.post(url, headers={**HDRS, "Content-Type": "application/json"}, data=json.dumps(body), timeout=45)
    if r.status_code != 200:
        return []
    js = r.json()
    out = []
    for it in (js.get("jobs") or []):
        out.append({
            "title": it.get("title"),
            "location": it.get("location"),
            "detail_url": urljoin(site_base, it.get("jobUrl") or ""),
            "description": None,
            "req_id": it.get("RequisitionNumber") or it.get("Id"),
            "posted": it.get("postedDate")
        })
    return out

def fetch(endpoint_url: str, max_pages: int = 6):
    """
    endpoint_url examples (NO query string—just the base 'requisitions' page):
      https://hdpc.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/LateralHiring/requisitions
      https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions
      https://eofe.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions
    """
    all_jobs = []

    # 1) try vendor JSON search (works on many)
    site_base = endpoint_url.rsplit("/requisitions", 1)[0]
    try:
        js = _json_search(site_base)
        if js:
            return js
    except Exception:
        pass

    # 2) fallback: parse HTML list with India filters & simple pagination
    for page in range(1, max_pages+1):
        try:
            jobs, more = _html_list(endpoint_url, page=page)
            all_jobs.extend(jobs)
            if not more or not jobs:
                break
            time.sleep(0.6)
        except Exception:
            break

    return all_jobs
