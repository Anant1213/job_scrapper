# connectors/lever_postings.py
"""
Lever job postings API connector.
Supports companies using Lever's public postings API.
"""
import time
from typing import List
import requests


HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"}
REQ_TIMEOUT = 25


def fetch(endpoint_url: str) -> List[dict]:
    """
    Fetch jobs from Lever postings API.
    
    Lever API returns JSON array of job postings.
    
    Args:
        endpoint_url: Lever API URL (e.g., https://api.lever.co/v0/postings/company)
        
    Returns:
        List of job dicts
    """
    out = []
    
    try:
        r = requests.get(endpoint_url, headers=HEADERS, timeout=REQ_TIMEOUT)
        r.raise_for_status()
        data = r.json() or []
        
        for j in data:
            title = j.get("text")
            
            # Location from categories or additionalPlain
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
                "req_id": req_id,
            })
        
        time.sleep(0.5)
        
    except requests.exceptions.RequestException as e:
        print(f"  Lever request error: {e}")
    except Exception as e:
        print(f"  Lever parse error: {e}")
    
    return out
