# connectors/oracle_cx.py
import time
import requests
from urllib.parse import urlparse

DEFAULT_LIMIT = 200
DEFAULT_PAGES = 15
TIMEOUT = 30

def _host_base(endpoint_url: str) -> str:
    u = urlparse(endpoint_url)
    return f"{u.scheme}://{u.netloc}"

def _site_alias(endpoint_url: str) -> str | None:
    """
    Extracts the CandidateExperience site alias from:
    https://<tenant>/hcmUI/CandidateExperience/en/sites/<ALIAS>/requisitions
    """
    try:
        p = urlparse(endpoint_url).path.strip("/").split("/")
        i = p.index("sites")
        return p[i + 1]
    except Exception:
        return None

def _detail_url(base_host: str, site_alias: str, req_id: str) -> str:
    # Canonical candidate preview detail URL
    return f"{base_host}/hcmUI/CandidateExperience/en/sites/{site_alias}/jobs/preview/{req_id}"

def fetch(endpoint_url: str,
          site_number: str,
          limit: int = DEFAULT_LIMIT,
          max_pages: int = DEFAULT_PAGES,
          india_only: bool = True):
    """
    Fetch jobs from Oracle Recruiting CE using the documented finder:
      GET /hcmRestApi/resources/latest/recruitingCEJobRequisitions
          ?onlyData=true
          &finder=findReqs;siteNumber=<SITE>;limit=<N>&offset=<K>
    Ref: Oracle HCM REST docs for recruitingCEJobRequisitions (finder=findReqs + siteNumber).
    """
    if not site_number:
        raise ValueError("oracle_cx.fetch() requires site_number (e.g., 'CX_3002').")

    base_host = _host_base(endpoint_url)  # e.g., https://hdpc.fa.us2.oraclecloud.com
    api = f"{base_host}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"

    site_alias = _site_alias(endpoint_url) or "CX"
    out = []

    session = requests.Session()
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/123.0.0.0 Safari/537.36"),
    }

    offset = 0
    for _ in range(max_pages):
        params = {
            "onlyData": "true",
            "finder": f"findReqs;siteNumber={site_number},facetsList=NONE",
            "limit": str(limit),
            "offset": str(offset),
            # "expand": "requisitionList.secondaryLocations",  # optional
        }
        r = session.get(api, headers=headers, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json() or {}

        items = data.get("items") or []
        if not items:
            break

        # Each item contains requisitionList[]
        reqs = []
        for blk in items:
            reqs.extend(blk.get("requisitionList") or [])

        if not reqs:
            break

        for j in reqs:
            try:
                title = (j.get("Title") or "").strip()
                req_id = str(j.get("Id"))
                location = (j.get("PrimaryLocation") or "").strip() or None
                posted = j.get("PostedDate")  # YYYY-MM-DD
                country = (j.get("PrimaryLocationCountry") or "").strip()

                if india_only:
                    if country != "IN" and (not location or "india" not in location.lower()):
                        continue

                detail = _detail_url(base_host, site_alias, req_id)
                out.append({
                    "title": title,
                    "location": location or ("India" if india_only else None),
                    "detail_url": detail,
                    "description": None,
                    "req_id": req_id,
                    "posted": posted,
                })
            except Exception:
                continue

        if not data.get("hasMore"):
            break
        offset += limit
        time.sleep(0.4)

    return out
