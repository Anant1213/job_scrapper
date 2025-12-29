# connectors/greenhouse_board.py
"""
Greenhouse job board API connector.
Supports companies using Greenhouse's public API.

Greenhouse API returns job listings in a standardized JSON format.
This is one of the most reliable job scraping methods.
"""
import time
from typing import List, Optional
import requests


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}
REQ_TIMEOUT = 30


def fetch(endpoint_url: str, max_pages: int = 1) -> List[dict]:
    """
    Fetch jobs from Greenhouse API endpoint.
    
    Greenhouse API returns JSON with a "jobs" array containing:
    - id: Job ID
    - title: Job title
    - location: Location object with "name" field
    - absolute_url: Apply URL
    - updated_at: Last update timestamp
    - content: HTML job description
    
    Args:
        endpoint_url: Greenhouse API URL 
            (e.g., https://api.greenhouse.io/v1/boards/company/jobs)
        max_pages: Maximum pages (usually 1, API returns all jobs)
        
    Returns:
        List of job dicts with normalized keys
    """
    out = []
    
    try:
        print(f"    Fetching: {endpoint_url}")
        r = requests.get(endpoint_url, headers=HEADERS, timeout=REQ_TIMEOUT)
        
        if r.status_code == 404:
            print(f"    Company not found on Greenhouse")
            return []
        
        r.raise_for_status()
        data = r.json() or {}
        
        jobs = data.get("jobs", [])
        print(f"    Found {len(jobs)} total jobs from API")
        
        for j in jobs:
            title = j.get("title")
            
            # Handle locations (can be list or nested dict)
            loc_obj = j.get("location") or {}
            if isinstance(loc_obj, dict):
                location = loc_obj.get("name", "")
            elif isinstance(loc_obj, list):
                location = "; ".join([l.get("name", "") for l in loc_obj if isinstance(l, dict)])
            else:
                location = str(loc_obj)
            
            apply_url = j.get("absolute_url") or j.get("url")
            posted = j.get("updated_at") or j.get("created_at")
            
            # Get description (HTML content)
            content = j.get("content") or ""
            # Strip HTML for plain text (basic)
            import re
            desc = re.sub(r'<[^>]+>', ' ', content)
            desc = re.sub(r'\s+', ' ', desc).strip()[:2000]
            
            req_id = str(j.get("id")) if j.get("id") else None
            
            out.append({
                "title": title,
                "detail_url": apply_url,
                "location": location,
                "posted": posted,
                "description": desc,
                "req_id": req_id,
            })
        
        time.sleep(0.3)
        
    except requests.exceptions.HTTPError as e:
        print(f"    Greenhouse HTTP error: {e}")
    except requests.exceptions.RequestException as e:
        print(f"    Greenhouse request error: {e}")
    except Exception as e:
        print(f"    Greenhouse parse error: {e}")
    
    return out
