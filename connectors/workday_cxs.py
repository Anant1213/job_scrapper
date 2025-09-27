from urllib.parse import urlparse, urljoin
import requests, time

HEADERS = {
    "User-Agent": "JobScoutBot/0.1 (+github actions)",
    "Content-Type": "application/json",
    "Accept": "application/json"
}
REQ_TIMEOUT = 25

def _base_host(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"

def fetch(endpoint_url: str, search_text: str | None = None, limit: int = 50, max_pages: int = 3):
    out = []
    host = _base_host(endpoint_url)
    offset = 0
    for _ in range(max_pages):
        body = {"limit": limit, "offset": offset, "searchText": search_text or ""}
        r = requests.post(endpoint_url, headers=HEADERS, json=body, timeout=REQ_TIMEOUT)
        r.raise_for_status()
        data = r.json() or {}
        posts = data.get("jobPostings") or []
        if not posts: break
        for jp in posts:
            title = jp.get("title")
            locations_text = jp.get("locationsText") or jp.get("locations", "")
            external_path = jp.get("externalPath") or jp.get("applyUrl") or ""
            apply_url = urljoin(host, external_path)
            req_id = jp.get("id") or None
            posted = jp.get("postedOn") or None
            description = None
            out.append({
                "title": title,
                "detail_url": apply_url,
                "location": locations_text,
                "posted": posted,
                "description": description,
                "req_id": req_id
            })
        if len(posts) < limit: break
        offset += limit
        time.sleep(1.0)
    return out
