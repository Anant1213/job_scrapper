# connectors/oracle_cx.py
import time, json, requests, re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlencode

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
)

BASE = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
AJAX = {
    "User-Agent": UA,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
}

def _parse_html(html, base):
    soup = BeautifulSoup(html, "html.parser")
    jobs, seen = [], set()

    # CE pages often have anchors to /requisitions/<id>/...
    for a in soup.select("a[href*='/requisitions/'], a[href*='/job/']"):
        title = a.get_text(" ", strip=True)
        href  = a.get("href") or ""
        if not title or not href: 
            continue
        if href.startswith("/"):
            href = urljoin(base, href)
        # nearby location tag
        loc = None
        for p in (a.parent, a.find_parent("li"), a.find_parent("article"), a.find_parent("div")):
            if not p: continue
            tag = p.select_one(".job-location, .location, [data-automation-id='locations']")
            if tag:
                loc = tag.get_text(" ", strip=True)
                break
        key = (title, href)
        if key in seen: 
            continue
        seen.add(key)
        jobs.append({"title": title, "location": loc, "detail_url": href, "description": None, "req_id": None, "posted": None})
    return jobs

def _json_search(session: requests.Session, site_base: str):
    """
    Try the documented CE JSON endpoint first. Some tenants require a prior GET
    to set cookies. We set Referer so the POST isn't dropped.
    """
    url = urljoin(site_base if site_base.endswith("/") else site_base + "/", "rest/api/jobs/search")
    payload = {"keyword":"", "location":"India", "locationLevel":"country", "page":1}
    headers = {**AJAX, "Referer": site_base}
    r = session.post(url, headers=headers, data=json.dumps(payload), timeout=45)
    if r.ok and r.headers.get("content-type","").startswith("application/json"):
        return r.json()
    # fallback GET with querystring (some tenants only allow GET)
    qs = urlencode({"keyword":"", "location":"India", "locationLevel":"country", "page":1})
    r2 = session.get(f"{url}?{qs}", headers=headers, timeout=45)
    if r2.ok and r2.headers.get("content-type","").startswith("application/json"):
        return r2.json()
    return None

def fetch(endpoint_url: str, max_pages: int = 6):
    # endpoint is ".../hcmUI/CandidateExperience/en/sites/<SITE>/requisitions"
    site_base = endpoint_url.rsplit("/requisitions", 1)[0]
    all_jobs = []

    with requests.Session() as s:
        # warm cookies
        s.get(site_base, headers=BASE, timeout=45)

        # JSON first
        try:
            js = _json_search(s, site_base)
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

        # HTML fallback with explicit India filters + pagination
        for p in range(1, max_pages+1):
            sep = "&" if "?" in endpoint_url else "?"
            url = f"{endpoint_url}{sep}location=India&locationLevel=country&page={p}"
            r = s.get(url, headers=BASE, timeout=45)
            if not r.ok:
                break
            batch = _parse_html(r.text, site_base)
            if not batch:
                break
            all_jobs.extend(batch)
            time.sleep(0.4)

    return all_jobs
