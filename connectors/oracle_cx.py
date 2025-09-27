# connectors/oracle_cx.py
from urllib.parse import urlparse
import requests, time

HEADERS = {
    "User-Agent": "JobScoutBot/0.1 (+github actions)",
    "Content-Type": "application/json",
    "Accept": "application/json"
}
REQ_TIMEOUT = 25

def _host_site(endpoint_url: str):
    """Extract base host and site name from an ORC search endpoint:
       https://<host>/hcmUI/CandidateExperience/en/sites/<SITE>/search
    """
    p = urlparse(endpoint_url)
    base = f"{p.scheme}://{p.netloc}"
    parts = [x for x in p.path.split("/") if x]
    # .../en/sites/<SITE>/search
    site = None
    for i in range(len(parts)):
        if i+2 < len(parts) and parts[i] == "sites":
            site = parts[i+1]
            break
    return base, site

def fetch(endpoint_url: str, searchText: str | None = None, page_size: int = 50, max_pages: int = 3):
    """
    Calls the public CandidateExperience search API:
      POST <host>/hcmUI/CandidateExperience/en/sites/<SITE>/search
      body: {"searchText": "...", "pageSize": 50, "page": 1, "sortBy": "POSTING_DATES_DESC"}
    Returns a list of {title, detail_url, location, posted, description, req_id}
    """
    base, site = _host_site(endpoint_url)
    if not site:
        raise ValueError("Could not infer ORC site from endpoint_url")

    out = []
    for page in range(1, max_pages + 1):
        body = {
            "searchText": searchText or "",
            "pageSize": int(page_size),
            "page": page,
            "sortBy": "POSTING_DATES_DESC"
        }
        r = requests.post(endpoint_url, headers=HEADERS, json=body, timeout=REQ_TIMEOUT)
        r.raise_for_status()
        data = r.json() or {}

        # Oracle frequently returns results under "items"
        items = data.get("items") or data.get("Items") or []
        if not items:
            # some tenants return "requisitions" or "searchResults"
            items = data.get("requisitions") or data.get("searchResults") or []

        if not items:
            break

        for j in items:
            # title
            title = j.get("title") or j.get("Title") or j.get("jobTitle")

            # requisition/job id
            req_id = (j.get("Id") or j.get("id") or j.get("jobId") or j.get("requisitionId"))
            req_id = str(req_id) if req_id is not None else None

            # location (varies a lot)
            loc = None
            if isinstance(j.get("locations"), list) and j["locations"]:
                loc = "; ".join([str(x.get("name") or x) for x in j["locations"]])
            loc = loc or j.get("primaryLocation") or j.get("Location") or j.get("location")

            # posted/updated
            posted = j.get("postedDate") or j.get("datePosted") or j.get("posted") or j.get("UpdatedDate")

            # apply/detail url
            if req_id:
                detail_url = f"{base}/hcmUI/CandidateExperience/en/sites/{site}/job/{req_id}"
            else:
                # fallback: many payloads include "externalPath" or "jobUrl"
                detail_url = j.get("externalPath") or j.get("jobUrl")

            # short description (optional)
            desc = (j.get("description") or j.get("shortDescription") or "")[:2000]

            out.append({
                "title": title,
                "detail_url": detail_url,
                "location": loc,
                "posted": posted,
                "description": desc,
                "req_id": req_id
            })
        time.sleep(0.7)

    return out
