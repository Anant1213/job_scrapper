# connectors/goldman_higher.py
"""
Goldman Sachs Higher platform scraper.
Scrapes higher.gs.com career pages.
"""
import re
import time
import json
from typing import Optional, List
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
import requests
from bs4 import BeautifulSoup


HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"}
REQ_TIMEOUT = 25


def _with_page(url: str, page: int) -> str:
    """Update URL with page number."""
    p = urlparse(url)
    qs = parse_qs(p.query)
    qs["page"] = [str(page)]
    new_q = urlencode({k: v[0] if isinstance(v, list) else v for k, v in qs.items()})
    return p._replace(query=new_q).geturl()


def _soup(url: str) -> BeautifulSoup:
    """Fetch URL and return BeautifulSoup."""
    r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def _from_json_ld(soup: BeautifulSoup) -> Optional[dict]:
    """Extract JobPosting from JSON-LD structured data."""
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "")
            if isinstance(data, list):
                for d in data:
                    if isinstance(d, dict) and d.get("@type") == "JobPosting":
                        return d
            elif isinstance(data, dict) and data.get("@type") == "JobPosting":
                return data
        except (json.JSONDecodeError, TypeError):
            continue
    return None


def _extract_location_from_json_ld(jd: dict) -> Optional[str]:
    """
    Extract location from JSON-LD data.
    
    FIX: Handle jobLocation as both dict and list.
    """
    loc_obj = jd.get("jobLocation")
    
    # Handle list of locations
    if isinstance(loc_obj, list) and loc_obj:
        loc_obj = loc_obj[0]
    
    if isinstance(loc_obj, dict):
        addr = loc_obj.get("address") or {}
        if isinstance(addr, list) and addr:
            addr = addr[0]
        if isinstance(addr, dict):
            city = addr.get("addressLocality")
            region = addr.get("addressRegion")
            country = addr.get("addressCountry")
            return ", ".join([x for x in [city, region, country] if x])
    
    return None


def _guess_location(text: str) -> Optional[str]:
    """Guess location from page text."""
    if not text:
        return None
    
    s = " ".join(text.split())
    
    # Try city + India pattern
    m = re.search(
        r"(Mumbai|Bengaluru|Bangalore|Hyderabad|Pune|Chennai|Gurugram|Gurgaon|Noida).*?India",
        s, re.I
    )
    if m:
        return m.group(0)
    
    # Try state + India pattern
    m2 = re.search(
        r"[A-Za-z ]+,\s*(Karnataka|Maharashtra|Telangana|Tamil Nadu|Delhi|Haryana),\s*India",
        s, re.I
    )
    return m2.group(0) if m2 else None


def fetch(endpoint_url: str, max_pages: int = 3) -> List[dict]:
    """
    Scrape Goldman Sachs Higher platform.
    
    Args:
        endpoint_url: Search results URL (e.g., https://higher.gs.com/results?search=data)
        max_pages: Maximum pages to scrape
        
    Returns:
        List of job dicts
    """
    base = f"{urlparse(endpoint_url).scheme}://{urlparse(endpoint_url).netloc}"
    out, seen = [], set()

    for page in range(1, max_pages + 1):
        try:
            url = _with_page(endpoint_url, page)
            soup = _soup(url)

            # Role links look like /roles/<id>
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if not re.match(r"^/roles/\d+", href):
                    continue
                    
                role_url = urljoin(base, href)
                if role_url in seen:
                    continue
                seen.add(role_url)

                # Fetch detail page
                try:
                    dsoup = _soup(role_url)
                except Exception:
                    continue

                # Extract title
                title = None
                h1 = dsoup.find("h1")
                if h1:
                    title = h1.get_text(" ", strip=True)

                # Try JSON-LD structured data
                jd = _from_json_ld(dsoup)
                location, posted, desc = None, None, None
                
                if jd:
                    title = jd.get("title") or title
                    posted = jd.get("datePosted") or jd.get("datePublished")
                    location = _extract_location_from_json_ld(jd)
                    desc = (jd.get("description") or "")[:2000]

                # Fallback location extraction
                if not location:
                    hero = dsoup.find(string=re.compile(r"India", re.I))
                    if hero:
                        location = _guess_location(hero.parent.get_text(" ", strip=True))
                    if not location:
                        location = _guess_location(dsoup.get_text(" ", strip=True))

                # Extract req_id from URL
                m = re.search(r"/roles/(\d+)", role_url)
                req_id = m.group(1) if m else None

                out.append({
                    "title": title,
                    "detail_url": role_url,
                    "location": location,
                    "posted": posted,
                    "description": desc,
                    "req_id": req_id,
                })
                
            time.sleep(1.0)
            
        except requests.exceptions.RequestException as e:
            print(f"  GS Higher request error on page {page}: {e}")
            break
        except Exception as e:
            print(f"  GS Higher parse error on page {page}: {e}")
            continue

    return out
