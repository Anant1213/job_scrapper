# connectors/lever_postings.py
"""
Lever Postings API:
  https://api.lever.co/v0/postings/<company>?mode=json
"""
import requests, time

HEADERS = {"User-Agent": "JobScoutBot/0.1 (+github actions)"}
REQ_TIMEOUT = 25

def fetch(endpoint_url: str):
    out = []
    r = requests.get(endpoint_url, headers=HEADERS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    data = r.json() or []
    for j in data:
        title = j.get("text")
        cats = j.get("categories") or {}
        location = cats.get("location") or j.get("additionalPlain")
        apply_url = j.get("applyUrl") or j.get("hostedUrl")
        posted = j.get("createdAt") or j.get("updatedAt")
        desc = (j.get("descriptionPlain") or "")[:2000]
        req_id = j.get("id")
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
