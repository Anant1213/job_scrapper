# connectors/citi_custom.py
"""
Citi custom career site scraper.
Handles jobs.citi.com website.
"""
import re
import time
from typing import Optional, List
import requests
from bs4 import BeautifulSoup


HDRS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
}


def _is_india_collection(url: str) -> bool:
    """Check if URL is for India jobs."""
    return bool(re.search(r"/search-jobs/India/", url, re.I)) or "locationsearch=india" in url.lower()


def _parse(html: str, base: str, force_india: bool) -> List[dict]:
    """Parse job listings from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    out, seen = [], set()
    
    for a in soup.select("a[href*='/job/'], a[href*='/jobs/']"):
        title = a.get_text(" ", strip=True)
        href = a.get("href") or ""
        
        if not title or not href:
            continue
        
        # Find parent container for location
        li = a.find_parent("li") or a.find_parent("article") or a.parent
        loc = None
        if li:
            loc_el = li.select_one(".job-location, .location, [data-ph-at-text='location']")
            if loc_el:
                loc = loc_el.get_text(" ", strip=True)
        
        if not loc and force_india:
            loc = "India"
        
        k = (title, href)
        if k in seen:
            continue
        seen.add(k)
        
        out.append({
            "title": title,
            "location": loc,
            "detail_url": href,
            "description": None,
            "req_id": None,
            "posted": None,
        })
    
    return out


def fetch(india_base_url: str, max_pages: int = 10) -> List[dict]:
    """
    Fetch jobs from Citi career site.
    
    Args:
        india_base_url: Base URL for India jobs search
        max_pages: Maximum pages to scrape
        
    Returns:
        List of job dicts
    """
    all_rows = []
    force_india = _is_india_collection(india_base_url)
    base = india_base_url

    for p in range(1, max_pages + 1):
        # Build URL with page parameter
        if "page=" in base:
            url = re.sub(r"([?&])page=\d+", rf"\g<1>page={p}", base)
        else:
            sep = "&" if "?" in base else "?"
            url = f"{base}{sep}page={p}"
        
        try:
            r = requests.get(url, headers=HDRS, timeout=45)
            if not r.ok:
                print(f"  Citi returned status {r.status_code} on page {p}")
                break
            
            rows = _parse(r.text, india_base_url, force_india)
            if not rows:
                break
            
            all_rows.extend(rows)
            time.sleep(0.3)
            
        except requests.exceptions.RequestException as e:
            print(f"  Citi request error on page {p}: {e}")
            break
        except Exception as e:
            print(f"  Citi parse error on page {p}: {e}")
            continue
    
    return all_rows
