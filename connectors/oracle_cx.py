# connectors/oracle_cx.py
"""
Oracle Recruiting Cloud (CX) connector for job scraping.
Supports companies using Oracle's HCM Cloud recruiting module.
"""
import time
from typing import Optional, List
from urllib.parse import urlparse
import requests


DEFAULT_LIMIT = 200
DEFAULT_PAGES = 15
TIMEOUT = 30


def _host_base(endpoint_url: str) -> str:
    """Extract base URL (scheme + netloc) from endpoint."""
    u = urlparse(endpoint_url)
    return f"{u.scheme}://{u.netloc}"


def _site_alias(endpoint_url: str) -> Optional[str]:
    """
    Extract the CandidateExperience alias from URL.
    
    Example: .../en/sites/<ALIAS>/requisitions -> ALIAS
    """
    try:
        p = urlparse(endpoint_url).path.strip("/").split("/")
        i = p.index("sites")
        return p[i + 1]
    except (ValueError, IndexError):
        return None


def _detail_url(base_host: str, site_alias: str, req_id: str) -> str:
    """Build the job detail URL."""
    return f"{base_host}/hcmUI/CandidateExperience/en/sites/{site_alias}/jobs/preview/{req_id}"


def fetch(
    endpoint_url: str,
    site_number: Optional[str] = None,
    limit: int = DEFAULT_LIMIT,
    max_pages: int = DEFAULT_PAGES,
    india_only: bool = True
) -> List[dict]:
    """
    Fetch jobs from Oracle Recruiting CE public REST API.
    
    API endpoint:
      /hcmRestApi/resources/latest/recruitingCEJobRequisitions
         ?onlyData=true
         &finder=findReqs;siteNumber=<SITE>;limit=<N>&offset=<K>
    
    Args:
        endpoint_url: Oracle careers page URL (used to derive base host and site alias)
        site_number: Site number (e.g., 'CX_3002'). If not provided, will try to extract.
        limit: Jobs per page (default 200)
        max_pages: Maximum pages to fetch (default 15)
        india_only: Filter to India only (default True)
        
    Returns:
        List of job dicts
    """
    # Handle missing site_number gracefully
    if not site_number:
        # Try to extract from URL or use common defaults
        site_alias = _site_alias(endpoint_url)
        if site_alias:
            site_number = site_alias
        else:
            print(f"  Warning: oracle_cx.fetch() - no site_number provided, trying CX_1001")
            site_number = "CX_1001"

    base_host = _host_base(endpoint_url)
    api = f"{base_host}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
    site_alias = _site_alias(endpoint_url) or "CX"
    
    out = []
    session = requests.Session()
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
    }

    offset = 0
    for page in range(max_pages):
        params = {
            "onlyData": "true",
            "finder": f"findReqs;siteNumber={site_number},facetsList=NONE",
            "limit": str(limit),
            "offset": str(offset),
        }
        
        try:
            r = session.get(api, headers=headers, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json() or {}

            items = data.get("items") or []
            if not items:
                break

            # Extract requisitions from items
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

                    # Filter for India if requested
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
            
        except requests.exceptions.RequestException as e:
            print(f"  Oracle CX request error: {e}")
            break
        except Exception as e:
            print(f"  Oracle CX parse error: {e}")
            break

    return out
