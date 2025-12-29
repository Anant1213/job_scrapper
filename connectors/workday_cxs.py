# connectors/workday_cxs.py
"""
Workday CXS API connector for job scraping.
Supports companies using Workday's candidate experience platform.
"""
import time
from typing import Optional, List
from urllib.parse import urljoin
import requests


UA = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
}


def _payload(search_text: Optional[str], limit: int, offset: int, india_only: bool = True) -> dict:
    """Build request payload for Workday CXS API."""
    p = {
        "appliedFacets": {},
        "limit": int(limit),
        "offset": int(offset),
    }
    
    # Apply India filter
    if india_only:
        p["appliedFacets"]["locationCountry"] = ["India"]
    
    # FIX: Apply search text even when india_only is True
    if search_text:
        p["searchText"] = str(search_text)
    
    return p


def _extract_req_id(posting: dict) -> Optional[str]:
    """Extract requisition ID from job posting."""
    # Try bulletFields first (can be a list or dict)
    bullet_fields = posting.get("bulletFields")
    if isinstance(bullet_fields, dict):
        req_id = bullet_fields.get("Job Number")
        if req_id:
            return str(req_id)
    elif isinstance(bullet_fields, list):
        # Sometimes bulletFields is a list of strings
        for field in bullet_fields:
            if isinstance(field, str) and field.strip():
                return field.strip()
    
    # Fallback to externalPath
    external_path = posting.get("externalPath")
    if external_path:
        return str(external_path)
    
    return None


def fetch(
    endpoint_url: str,
    search_text: Optional[str] = None,
    limit: int = 50,
    max_pages: int = 6,
    india_only: bool = True
) -> List[dict]:
    """
    Fetch jobs from Workday CXS API.
    
    Args:
        endpoint_url: Base URL for the Workday CXS endpoint
            Examples:
            - https://ms.wd5.myworkdayjobs.com/wday/cxs/ms/External/jobs
            - https://db.wd3.myworkdayjobs.com/wday/cxs/db/External/jobs
            - https://wf.wd1.myworkdayjobs.com/wday/cxs/wf/WellsFargoJobs/jobs
        search_text: Optional search query
        limit: Number of jobs per page (default 50)
        max_pages: Maximum pages to fetch (default 6)
        india_only: Filter to India locations only (default True)
        
    Returns:
        List of job dicts with keys: title, location, detail_url, description, req_id, posted
    """
    results = []
    base = endpoint_url.rstrip("/")
    
    for page in range(max_pages):
        offset = page * int(limit)
        
        try:
            r = requests.post(
                base,
                json=_payload(search_text, limit, offset, india_only),
                headers=UA,
                timeout=45
            )
            
            if r.status_code != 200:
                print(f"  Workday returned status {r.status_code}")
                break
                
            if "jobPostings" not in r.text:
                break
                
            data = r.json()
            posts = data.get("jobPostings") or []
            
            if not posts:
                break
            
            for p in posts:
                title = p.get("title")
                loc = p.get("locationsText") or p.get("location") or ""
                path = p.get("externalPath") or p.get("externalUrl") or ""
                
                # Build canonical job URL
                if path.startswith("/"):
                    detail = urljoin(base, path)
                else:
                    detail = path or base
                
                results.append({
                    "title": title,
                    "location": loc,
                    "detail_url": detail,
                    "description": None,
                    "req_id": _extract_req_id(p),
                    "posted": p.get("postedOn"),
                })
            
            # Stop if we got fewer results than requested
            if len(posts) < int(limit):
                break
                
            time.sleep(0.4)
            
        except requests.exceptions.RequestException as e:
            print(f"  Workday request error: {e}")
            break
        except Exception as e:
            print(f"  Workday parse error: {e}")
            break
    
    return results
