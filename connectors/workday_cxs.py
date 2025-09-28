# connectors/workday_cxs.py
import requests, time
from urllib.parse import urljoin

UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}

def _payload(search_text: str | None, limit: int, offset: int):
    p = {
        "appliedFacets": {"locationCountry": ["India"]},
        "limit": int(limit),
        "offset": int(offset),
    }
    if search_text:
        p["searchText"] = str(search_text)
    return p

def fetch(endpoint_url: str, search_text=None, limit=50, max_pages=6, india_only=True):
    """
    endpoint_url example:
      https://ms.wd5.myworkdayjobs.com/wday/cxs/ms/External/jobs
      https://db.wd3.myworkdayjobs.com/wday/cxs/db/External/jobs
      https://wf.wd1.myworkdayjobs.com/wday/cxs/wf/WellsFargoJobs/jobs
    """
    results = []
    base = endpoint_url.rstrip("/")
    for page in range(max_pages):
        offset = page * int(limit)
        r = requests.post(base, json=_payload(search_text if not india_only else None, limit, offset), headers=UA, timeout=45)
        if r.status_code != 200 or "jobPostings" not in r.text:
            break
        data = r.json()
        posts = data.get("jobPostings") or []
        if not posts:
            break
        for p in posts:
            title = p.get("title")
            loc   = p.get("locationsText") or p.get("location") or ""
            path  = p.get("externalPath") or p.get("externalUrl") or ""
            # canonical job URL:
            if path.startswith("/"):
                detail = urljoin(base, path)
            else:
                detail = path or base
            results.append({
                "title": title,
                "location": loc,
                "detail_url": detail,
                "description": None,
                "req_id": p.get("bulletFields", {}).get("Job Number") or p.get("externalPath"),
                "posted": p.get("postedOn"),
            })
        if len(posts) < int(limit):
            break
        time.sleep(0.4)
    return results
