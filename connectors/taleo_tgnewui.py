# connectors/taleo_tgnewui.py
"""
Taleo TGNewUI connector for job scraping.
Supports career sites using Oracle Taleo's TGNewUI interface.
"""
from typing import List
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


HDRS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
}


def _parse(html: str, base_url: str) -> List[dict]:
    """Parse job listings from Taleo TGNewUI page."""
    soup = BeautifulSoup(html, "html.parser")
    out, seen = [], set()
    
    # Job links look like .../JobDetail/<Title>/<JobId>
    for a in soup.select("a[href*='JobDetail/']"):
        title = a.get_text(" ", strip=True)
        href = a.get("href") or ""
        
        if not title or not href:
            continue
        
        # Absolutize relative URLs
        if href.startswith("/"):
            href = urljoin(base_url, href)
        
        # Find location from parent container
        loc = None
        root = a.find_parent("li") or a.find_parent("div")
        if root:
            el = root.select_one(".jobLocation, .job-location, .location")
            if el:
                loc = el.get_text(" ", strip=True)
        
        key = (title, href)
        if key in seen:
            continue
        seen.add(key)
        
        out.append({
            "title": title,
            "location": loc,
            "detail_url": href,
            "description": None,
            "req_id": None,
            "posted": None,
        })
    
    return out


def fetch(search_url: str, max_pages: int = 4) -> List[dict]:
    """
    Fetch jobs from Taleo TGNewUI search page.
    
    Many TGNewUI searches are server-rendered at:
    - .../Search/home/HomeWithPreLoad?...locationSearch=India
    - .../Search/home/SearchResults?...keyword=India&locationSearch=India
    
    Args:
        search_url: Taleo search URL
        max_pages: Maximum pages (often just 1 for Taleo)
        
    Returns:
        List of job dicts
    """
    try:
        r = requests.get(search_url, headers=HDRS, timeout=45)
        if not r.ok:
            print(f"  Taleo returned status {r.status_code}")
            return []
        
        jobs = _parse(r.text, search_url)
        return jobs
        
    except requests.exceptions.RequestException as e:
        print(f"  Taleo request error: {e}")
        return []
    except Exception as e:
        print(f"  Taleo parse error: {e}")
        return []
