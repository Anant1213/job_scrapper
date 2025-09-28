# connectors/workday_cxs.py
import time
import requests

INDIA_GUID = "c4f78be1a8f14da0ab49ce1162348a5e"

HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=UTF-8"
}

def _page(endpoint, search_text, offset, limit, india_only=True):
    body = {
        "limit": limit,
        "offset": offset,
        "searchText": search_text or "",
        "appliedFacets": {}
    }
    if india_only:
        body["appliedFacets"]["locationCountry"] = [INDIA_GUID]
    r = requests.post(endpoint, headers=HDRS, json=body, timeout=45)
    r.raise_for_status()
    return r.json()

def fetch(endpoint_url: str, search_text=None, limit=50, max_pages=5, india_only=True):
    """
    endpoint_url example: https://ms.wd5.myworkdayjobs.com/wday/cxs/ms/External/jobs
                           https://mq.wd3.myworkdayjobs.com/wday/cxs/mq/CareersatMQ/jobs
                           https://wf.wd1.myworkdayjobs.com/wday/cxs/wf/WellsFargoJobs/jobs
    """
    out = []
    offset = 0
    for _ in range(max_pages):
        data = _page(endpoint_url, search_text, offset, limit, india_only=india_only)
        # Workday payload shape:
        # { "jobPostings": [{title, locationsText, postedOn, externalPath, bulletFields, ...}], "total": ... }
        posts = (data or {}).get("jobPostings") or []
        if not posts:
            break
        for jp in posts:
            title = jp.get("title")
            loc = jp.get("locationsText")
            detail = jp.get("externalPath") or jp.get("applyUrl")
            if detail and detail.startswith("/"):
                # build full URL from endpoint host
                # endpoint host: https://<host>/wday/cxs/<tenant>/<site>/jobs
                base = endpoint_url.split("/wday/")[0]
                detail = base + detail
            out.append({
                "title": title,
                "location": loc,
                "detail_url": detail,
                "description": None,
                "req_id": jp.get("bulletFields", {}).get("Job_Req_ID") if isinstance(jp.get("bulletFields"), dict) else None,
                "posted": jp.get("postedOn")
            })
        offset += limit
        time.sleep(0.5)
    return out
