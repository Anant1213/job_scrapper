# connectors/direct_site.py
"""
Generic static page scraper for career sites.
Handles sites that don't require JavaScript rendering.
"""
import time
from typing import Optional, List, Dict
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


HDRS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}


def _extract_from_html(
    html: str,
    base_url: str,
    sel: Dict[str, str],
    force_india: bool = False
) -> List[dict]:
    """
    Extract job listings from HTML using CSS selectors.
    
    Args:
        html: Page HTML content
        base_url: Base URL for absolutizing links
        sel: Selector dict with keys: card, title?, link?, location?, posted?
        force_india: Default location to "India" if not found
        
    Returns:
        List of job dicts
    """
    soup = BeautifulSoup(html, "html.parser")
    out, seen = [], set()
    
    card_sel = sel["card"]
    title_sel = sel.get("title") or "a"
    link_sel = sel.get("link") or "a"
    loc_sel = sel.get("location")
    posted_sel = sel.get("posted")

    for card in soup.select(card_sel):
        a = card.select_one(link_sel)
        if not a:
            continue
        
        href = a.get("href") or ""
        if href.startswith("/"):
            href = urljoin(base_url, href)
        
        title_el = card.select_one(title_sel) or a
        title = title_el.get_text(" ", strip=True) if title_el else None
        
        if not title or not href:
            continue

        # Extract location
        loc = None
        if loc_sel:
            loc_el = card.select_one(loc_sel)
            if loc_el:
                loc = loc_el.get_text(" ", strip=True)
        if not loc and force_india:
            loc = "India"

        # Extract posted date
        posted = None
        if posted_sel:
            p = card.select_one(posted_sel)
            if p:
                posted = p.get_text(" ", strip=True)

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
            "posted": posted,
        })
    
    return out


def fetch(
    url: str,
    selectors: Dict[str, str],
    max_pages: int = 1,
    page_param: Optional[str] = None,
    start_page: int = 1,
    force_india: bool = False,
    delay_s: float = 0.5
) -> List[dict]:
    """
    Generic static-page scraper.
    
    Args:
        url: Base URL to scrape
        selectors: CSS selectors dict with keys: card, title?, link?, location?, posted?
        max_pages: Maximum pages to scrape
        page_param: Query parameter name for pagination
        start_page: Starting page number
        force_india: Default location to "India"
        delay_s: Delay between page requests
        
    Returns:
        List of job dicts
    """
    all_rows = []
    base = url

    for page in range(start_page, start_page + max_pages):
        page_url = base
        if page_param:
            sep = "&" if "?" in base else "?"
            page_url = f"{base}{sep}{page_param}={page}"

        try:
            r = requests.get(page_url, headers=HDRS, timeout=45)
            if not r.ok:
                print(f"  Direct site returned status {r.status_code} on page {page}")
                break
            
            rows = _extract_from_html(r.text, page_url, selectors, force_india=force_india)
            if not rows and page > start_page:
                break
            
            all_rows.extend(rows)
            time.sleep(delay_s)
            
        except requests.exceptions.RequestException as e:
            print(f"  Direct site request error on page {page}: {e}")
            break
        except Exception as e:
            print(f"  Direct site parse error on page {page}: {e}")
            continue

    return all_rows
