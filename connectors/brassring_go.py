# connectors/brassring_go.py
import re
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
    if "/India/" in url or "country=India" in url:
        return True
    # also check page heading
    try:
        soup = BeautifulSoup(html, "html.parser")
        h = soup.find(["h1","h2","title"])
        if h and re.search(r"\bIndia\b", h.get_text(" ", strip=True), re.I):
            return True
    except Exception:
        pass
    return False

def _nearest_location(node) -> str | None:
    # Try common BrassRing markup
    loc_el = node.select_one(".jobLocation, .job-location, .location, [data-ph-at-text='location']")
    if loc_el:
        return loc_el.get_text(" ", strip=True)

    # Walk up a bit and look for a label with 'Location'
    parent = node
    for _ in range(3):
        if not parent:
            break
        label = parent.find(string=re.compile(r"Location", re.I))
        if label and label.parent:
            txt = label.parent.get_text(" ", strip=True)
            # keep something city-like
            if any(c.lower() in txt.lower() for c in CITY_HINTS) or "India" in txt or " IN" in txt:
                return txt
        parent = parent.parent
    return None

def _parse_brassring_go(html: str, base_url: str, force_india: bool) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    jobs = []

    # BrassRing GO pages usually list jobs as anchors to /Nomura/job/...
    anchors = soup.select("a[href*='/Nomura/job/'], a[href*='/job/']")
    seen = set()

    for a in anchors:
        title = a.get_text(" ", strip=True)
        href = a.get("href") or ""
        if not title or not href:
            continue

        # absolutize
        if href.startswith("/"):
            href = urljoin(base_url, href)

        # find nearby location
        container = a
        for _ in range(2):
            if container and container.name not in ("li", "article", "div"):
                container = container.parent
        loc = _nearest_location(container) if container else None

        # fallback: if this is the *India* collection and no loc found, set "India"
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

def fetch(go_page_url: str, max_pages: int = 6) -> list[dict]:
    # BrassRing GO pages are typically single-page lists (no real pagination);
    # we still keep the signature compatible.
    r = requests.get(go_page_url, headers=HDRS, timeout=45)
    if not r.ok:
        return []

    force_india = _looks_india_collection(go_page_url, r.text)
    return _parse_brassring_go(r.text, go_page_url, force_india)
