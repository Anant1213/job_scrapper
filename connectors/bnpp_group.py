# connectors/bnpp_group.py
"""
BNP Paribas Group career site scraper.
Handles group.bnpparibas career pages.
"""
from typing import List
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
}


def fetch(india_landing_url: str, max_pages: int = 3) -> List[dict]:
    """
    Fetch jobs from BNP Paribas India careers page.
    
    Args:
        india_landing_url: URL for India job listings
        max_pages: Maximum pages (usually 1, page is server-rendered)
        
    Returns:
        List of job dicts
    """
    try:
        r = requests.get(india_landing_url, headers=HDRS, timeout=45)
        if not r.ok:
            print(f"  BNPP returned status {r.status_code}")
            return []
        
        soup = BeautifulSoup(r.text, "html.parser")
        out, seen = [], set()
        
        for a in soup.select("a[href*='/job-offer/'], a[href*='/en/job/']"):
            title = a.get_text(" ", strip=True)
            href = a.get("href") or ""
            
            if not title or not href:
                continue
            
            # Absolutize relative URLs
            if href.startswith("/"):
                href = urljoin(india_landing_url, href)
            
            # Try to find location nearby
            loc = None
            parent = a.find_parent("article") or a.parent
            if parent:
                el = parent.select_one(".job__item__location, .location, [class*='location']")
                if el:
                    loc = el.get_text(" ", strip=True)
            
            key = (title, href)
            if key in seen:
                continue
            seen.add(key)
            
            out.append({
                "title": title,
                "location": loc or "India",
                "detail_url": href,
                "description": None,
                "req_id": None,
                "posted": None,
            })
        
        return out
        
    except requests.exceptions.RequestException as e:
        print(f"  BNPP request error: {e}")
        return []
    except Exception as e:
        print(f"  BNPP parse error: {e}")
        return []
