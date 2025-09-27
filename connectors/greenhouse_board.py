import requests, time

HEADERS = {"User-Agent": "JobScoutBot/0.1 (+github actions)"}
REQ_TIMEOUT = 25

def fetch(endpoint_url: str, max_pages: int = 1):
    out = []
    r = requests.get(endpoint_url, headers=HEADERS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    data = r.json() or {}
    for j in data.get("jobs", []):
        title = j.get("title")
        locs = j.get("locations") or []
        location = "; ".join([l.get("name","") for l in locs]) if locs else (j.get("location", {}) or {}).get("name")
        apply_url = j.get("absolute_url") or j.get("url")
        posted = j.get("updated_at") or j.get("created_at")
        desc = (j.get("content") or "")[:2000]
        req_id = str(j.get("id")) if j.get("id") else None
        out.append({
            "title": title,
            "detail_url": apply_url,
            "location": location,
            "posted": posted,
            "description": desc,
            "req_id": req_id
        })
    time.sleep(0.5)
    return out
