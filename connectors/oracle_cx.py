# connectors/oracle_cx.py
import time
import requests
from urllib.parse import urlparse

DEFAULT_LIMIT = 200
DEFAULT_PAGES = 10
TIMEOUT = 30

def _host_base(endpoint_url: str) -> str:
    """Return 'https://<host>' for any Oracle CX URL you give me."""
    u = urlparse(endpoint_url)
    return f"{u.scheme}://{u.netloc}"

def _site_alias(endpoint_url: str) -> str | None:
    """
    Extract the CandidateExperience site alias from a URL like:
    https://<tenant>/hcmUI/CandidateExperience/en/sites/<ALIAS>/requisitions
    """
    try:
        p = urlparse(endpoint_url).path.strip("/").split("/")
        i = p.index("sites")
        return p[i+1]
    except Exception:
        return None

def _detail_url(base_candidate_url: str, site_alias: str, req_id: str) -> str:
    # canonical candidate detail
    return f"{base_candidate_url.rstrip('/')}/preview/{req_id}"

def fetch(endpoint_url: str,
          site_number: str,
          limit: int = DEFAULT_LIMIT,
          max_pages: int = DEFAULT_PAGES,
          india_only: bool = True):
    """
    Pull jobs from Oracle Recruiting 'recruitingCEJobRequisitions' using the 'findReqs' finder.
    Requires the correct site_number (e.g. CX_3002).
    Docs: finder=findReqs;siteNumber=..., with &limit &offset for paging.
    """
    base_host = _host_base(endpoint_url)
    api = f"{base_host}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"

    site_alias = _site_alias(endpoint_url)
    base_candidate = None
    if site_alias:
        base_candidate = f"{base_host}/hcmUI/CandidateExperience/en/sites/{site_alias}/requisitions"

    out = []
    session = requests.Session()
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    }

    offset = 0
    for _ in range(max_pages):
        params = {
            "onlyData": "true",
            # keep finder simple; use root limit/offset for consistent paging
            "finder": f"findReqs;siteNumber={site_number},facetsList=NONE",
            "limit": str(limit),
            "offset": str(offset),
            # optional: expand secondary locations if you want
            # "expand": "requisitionList.secondaryLocations",
        }
        r = session.get(api, headers=headers, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()

        # Shape: { items: [{ requisitionList: [...] }], hasMore, count, offset, limit, ... }
        items = (data or {}).get("items") or []
        if not items:
            break

        reqs = []
        for blk in items:
            reqs.extend(blk.get("requisitionList") or [])

        if not reqs:
            break

        for j in reqs:
            try:
                title = (j.get("Title") or "").strip()
                req_id = str(j.get("Id"))
                primary_country = (j.get("PrimaryLocationCountry") or "").strip()
                location = (j.get("PrimaryLocation") or "").strip() or None
                posted = j.get("PostedDate")  # ISO-like YYYY-MM-DD

                if india_only:
                    # Prefer structured country flag; fallback to text contains 'India'
                    if primary_country != "IN" and (not location or "india" not in location.lower()):
                        continue

                detail_url = None
                if base_candidate and req_id:
                    detail_url = _detail_url(base_candidate, site_alias, req_id)
                else:
                    # fallback to tenant preview pattern if alias unknown
                    detail_url = f"{base_host}/hcmUI/CandidateExperience/en/sites/CX/requisitions/preview/{req_id}"

                out.append({
                    "title": title,
                    "location": location or ("India" if india_only else None),
                    "detail_url": detail_url,
                    "description": None,
                    "req_id": req_id,
                    "posted": posted,
                })
            except Exception:
                continue

        # stop if API reports no more
        has_more = bool(data.get("hasMore"))
        if not has_more:
            break
        offset += limit
        time.sleep(0.5)

    return out
