# connectors/brassring_go.py
"""
BrassRing GO connector for job scraping.
Supports career sites using IBM BrassRing platform.
"""
import re
from typing import Optional, List
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


HDRS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}

CITY_HINTS = [
    "Mumbai", "Bengaluru", "Bangalore", "Pune", "Hyderabad", "Chennai",
    "Noida", "Gurugram", "Gurgaon", "Kolkata", "New Delhi", "Delhi",
    "Ahmedabad", "Kochi", "Cochin", "Coimbatore", "Jaipur", "Indore", "Surat"
]


def _looks_india_collection(url: str, html: str) -> bool:
    """Check if page is India-focused."""
    if "/India/" in url or "country=India" in url:
        return True
    
    try:
        soup = BeautifulSoup(html, "html.parser")
        h = soup.find(["h1", "h2", "title"])
        if h and re.search(r"\bIndia\b", h.get_text(" ", strip=True), re.I):
            return True
    except Exception:
        pass
    
    return False


def _extract_company_from_url(url: str) -> Optional[str]:
    """Extract company name from URL for selector building."""
    # Try to find company name in URL path
    patterns = [
        r"/([A-Za-z]+)/job/",
        r"/([A-Za-z]+)/go/",
        r"careers\.([a-z]+)\.",
    ]
    for pat in patterns:
        m = re.search(pat, url, re.I)
        if m:
            return m.group(1)
    return None


def _nearest_location(node) -> Optional[str]:
    """Try to find location near a job card element."""
    if not node:
        return None
    
    # Try common BrassRing markup
    loc_el = node.select_one(".jobLocation, .job-location, .location, [data-ph-at-text='location']")
    if loc_el:
        return loc_el.get_text(" ", strip=True)

    # Walk up and look for a label with 'Location'
    parent = node
    for _ in range(3):
        if not parent:
            break
        label = parent.find(string=re.compile(r"Location", re.I))
        if label and label.parent:
            txt = label.parent.get_text(" ", strip=True)
            # Check for city-like content
            if any(c.lower() in txt.lower() for c in CITY_HINTS) or "India" in txt or " IN" in txt:
                return txt
        parent = parent.parent
    
    return None


def _parse_brassring_go(html: str, base_url: str, force_india: bool) -> List[dict]:
    """Parse job listings from BrassRing GO page."""
    soup = BeautifulSoup(html, "html.parser")
    jobs = []
    seen = set()
    
    # FIX: Build selector dynamically instead of hardcoding Nomura
    company = _extract_company_from_url(base_url)
    
    # Build selector patterns
    selectors = ["a[href*='/job/']"]
    if company:
        selectors.insert(0, f"a[href*='/{company}/job/']")
    
    # Find all matching anchors
    anchors = []
    for sel in selectors:
        anchors.extend(soup.select(sel))

    for a in anchors:
        title = a.get_text(" ", strip=True)
        href = a.get("href") or ""
        
        if not title or not href:
            continue

        # Absolutize URL
        if href.startswith("/"):
            href = urljoin(base_url, href)

        # Find nearby location
        container = a
        for _ in range(2):
            if container and container.name not in ("li", "article", "div"):
                container = container.parent
        loc = _nearest_location(container) if container else None

        # Fallback: if this is the India collection and no loc found
        if not loc and force_india:
            loc = "India"

        key = (title, href)
        if key in seen:
            continue
        seen.add(key)

        jobs.append({
            "title": title,
            "location": loc,
            "detail_url": href,
            "description": None,
            "req_id": None,
            "posted": None,
        })

    return jobs


def fetch(go_page_url: str, max_pages: int = 6) -> List[dict]:
    """
    Fetch jobs from BrassRing GO pages.
    
    Note: BrassRing GO pages are typically single-page lists;
    pagination is kept for API compatibility.
    
    Args:
        go_page_url: GO page URL
        max_pages: Maximum pages (usually 1 for BrassRing)
        
    Returns:
        List of job dicts
    """
    try:
        r = requests.get(go_page_url, headers=HDRS, timeout=45)
        if not r.ok:
            print(f"  BrassRing returned status {r.status_code}")
            return []

        force_india = _looks_india_collection(go_page_url, r.text)
        return _parse_brassring_go(r.text, go_page_url, force_india)
        
    except requests.exceptions.RequestException as e:
        print(f"  BrassRing request error: {e}")
        return []
    except Exception as e:
        print(f"  BrassRing parse error: {e}")
        return []
