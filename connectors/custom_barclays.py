# connectors/custom_barclays.py
"""
Custom Barclays career site scraper.
Handles Barclays' search.jobs.barclays website.
"""
import re
import time
from typing import Optional, List
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup


HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"}
REQ_TIMEOUT = 25


def _soup(url: str) -> BeautifulSoup:
    """Fetch URL and return BeautifulSoup object."""
    r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def _next_page_url(current_url: str, next_page_number: int) -> str:
    """Build URL for the next page."""
    p = urlparse(current_url)
    parts = p.path.rstrip("/").split("/")
    
    if parts and parts[-1].isdigit():
        parts[-1] = str(next_page_number)
    else:
        parts.append(str(next_page_number))
    
    return p._replace(path="/".join(parts)).geturl()


def _extract_card_location(card) -> Optional[str]:
    """Try to extract location from job card."""
    india_cities = [
        "India", "Mumbai", "Bengaluru", "Bangalore", "Hyderabad",
        "Pune", "Chennai", "Gurugram", "Gurgaon", "Noida"
    ]
    
    for sel in ["span", "div", "p"]:
        for el in card.find_all(sel):
            txt = el.get_text(" ", strip=True)
            if not txt:
                continue
            pattern = r"\b(" + "|".join(india_cities) + r")\b"
            if re.search(pattern, txt, re.I):
                return txt
    return None


def _extract_detail_location(soup: BeautifulSoup) -> Optional[str]:
    """Extract location from job detail page."""
    text = soup.get_text(" ", strip=True)
    
    # Try to find "Location: X" pattern
    m = re.search(r"Location\s*[:\-]\s*([A-Za-z ,\-]+)", text, re.I)
    if m:
        return m.group(1).strip()
    
    # Fallback: look for common city names
    india_cities = [
        "Mumbai", "Bengaluru", "Bangalore", "Hyderabad",
        "Pune", "Chennai", "Gurugram", "Gurgaon", "Noida", "India"
    ]
    pattern = r"\b(" + "|".join(india_cities) + r")\b"
    m2 = re.search(pattern, text, re.I)
    return m2.group(0) if m2 else None


def fetch(endpoint_url: str, max_pages: int = 3) -> List[dict]:
    """
    Fetch jobs from Barclays career site.
    
    Args:
        endpoint_url: Base search URL
        max_pages: Maximum pages to scrape
        
    Returns:
        List of job dicts
    """
    base = "https://search.jobs.barclays"
    out, seen = [], set()
    
    for page in range(1, max_pages + 1):
        try:
            url = _next_page_url(endpoint_url, page)
            soup = _soup(url)

            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/job/" not in href:
                    continue
                    
                detail_url = urljoin(base, href)
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                title = a.get_text(strip=True) or None
                card_loc = _extract_card_location(a.parent or a)

                # Try to find posting date
                date_live = None
                nxt, hops = a.find_next(string=True), 0
                while nxt and hops < 15:
                    txt = (nxt.strip() if isinstance(nxt, str) else "").strip()
                    if re.search(r"\b(\d{1,2}\s+[A-Za-z]{3}|\d{1,2}/\d{2}/\d{4})\b", txt):
                        date_live = txt
                        break
                    nxt = nxt.next_element
                    hops += 1

                # Fetch detail page for better location and description
                desc, detail_loc = None, None
                try:
                    dsoup = _soup(detail_url)
                    detail_loc = _extract_detail_location(dsoup)
                    p = dsoup.find("p")
                    desc = p.get_text(" ", strip=True)[:2000] if p else None
                except Exception:
                    pass

                location = detail_loc or card_loc
                
                # FIX: Include req_id key (was missing before)
                out.append({
                    "title": title,
                    "detail_url": detail_url,
                    "location": location,
                    "posted": date_live,
                    "description": desc,
                    "req_id": None,  # FIX: Added missing key
                })
                
            time.sleep(1.5)
            
        except requests.exceptions.RequestException as e:
            print(f"  Barclays request error on page {page}: {e}")
            break
        except Exception as e:
            print(f"  Barclays parse error on page {page}: {e}")
            continue
    
    return out
