# connectors/oracle_cx.py
import time, re, json
from urllib.parse import urljoin, urlencode
import requests
from bs4 import BeautifulSoup

BASE_HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
}

AJAX_HDRS = {
    **BASE_HDRS,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "",  # we set at runtime to site base
    "Referer": "", # set at runtime
}

def _parse_html_list(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    # fairly generic selectors
    for card in soup.select("[data-automation-id='jobCard'], li, article, div"):
        a = card.select_one("a[href*='/requisitions/'], a[href*='/job/'], a[href*='/jobs/']")
        if not a:
            continue
        title = a.get_text(" ", strip=True)
        href  = a.get("href") or ""
        if href.startswith("/"):
            href = urljoin(base_url, href)
        loc = None
        loc_el = card.select_one("[data-automation-id='locations'], .job-location, .location")
        if loc_el:
            loc = loc_el.get_text(" ", strip=True)
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

def _html_requisitions(endpoint_url, page=1):
    # try server-rendered list with India filter
    params = {
        "location": "India",
        "locationLevel": "country",
        "page": page
    }
    sep = "&" if "?" in endpoint_url else "?"
    url = f"{endpoint_url}{sep}{urlencode(params)}"
    r = requests.get(url, headers=BASE_HDRS, timeout=45)
    if r.status_code != 200:
        return [], False
    jobs = _parse_html_list(r.text, endpoint_url)
    return jobs, len(jobs) >= 5  # heuristic for "has more pages"

def _json_search(site_base):
    """
    Many ORC tenants expose: POST/GET {site_base}/rest/api/jobs/search
    body/query: location=India&locationLevel=country&page=1
    """
    ajax = dict(AJAX_HDRS)
    ajax["Origin"] = site_base
    ajax["Referer"] = site_base

    url = urljoin(site_base if site_base.endswith("/") else (site_base + "/"), "rest/api/jobs/search")
    payload = {"keyword":"", "location":"India", "locationLevel":"country", "page":1}

    # try POST
    r = requests.post(url, headers=ajax, data=json.dumps(payload), timeout=45)
    if r.status_code == 200 and r.headers.get("content-type","").startswith("application/json"):
        return r.json()

    # try GET as fallback
    qs = urlencode({"keyword":"","location":"India","locationLevel":"country","page":1})
    r2 = requests.get(f"{url}?{qs}", headers=ajax, timeout=45)
    if r2.status_code == 200 and r2.headers.get("content-type","").startswith("application/json"):
        return r2.json()

    return None

def fetch(endpoint_url: str, max_pages: int = 6):
    """
    endpoint_url like:
      https://hdpc.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/LateralHiring/requisitions
    """
    all_jobs = []

    site_base = endpoint_url.rsplit("/requisitions", 1)[0]
    try:
        js = _json_search(site_base)
        if js and isinstance(js, dict) and js.get("jobs"):
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

    # fallback: HTML pages with India filter
    for p in range(1, max_pages+1):
        try:
            jobs, more = _html_requisitions(endpoint_url, page=p)
            all_jobs.extend(jobs)
            if not more or not jobs:
                break
            time.sleep(0.5)
        except Exception:
            break

    return all_jobs
