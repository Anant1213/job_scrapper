# connectors/custom_barclays.py
import re, time
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "JobScoutBot/0.1 (+github actions)"}
REQ_TIMEOUT = 20

def _soup(url):
    r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def _next_page_url(current_url: str, next_page_number: int) -> str:
    p = urlparse(current_url)
    parts = p.path.rstrip("/").split("/")
    if parts and parts[-1].isdigit():
        parts[-1] = str(next_page_number)
    else:
        parts.append(str(next_page_number))
    return p._replace(path="/".join(parts)).geturl()

def _extract_card_location(card) -> str | None:
    # try a bunch of common patterns on Barclays search cards
    for sel in ["span", "div", "p"]:
        for el in card.find_all(sel):
            txt = el.get_text(" ", strip=True)
            if not txt: continue
            if re.search(r"\b(India|Mumbai|Bengaluru|Bangalore|Hyderabad|Pune|Chennai|Gurugram|Gurgaon|Noida)\b", txt, re.I):
                return txt
    return None

def _extract_detail_location(soup: BeautifulSoup) -> str | None:
    # job detail often has a "Location" label or breadcrumb
    text = soup.get_text(" ", strip=True)
    m = re.search(r"Location\s*[:\-]\s*([A-Za-z ,\-]+)", text, re.I)
    if m:
        return m.group(1)
    # final fallback: look for common city names
    m2 = re.search(r"\b(Mumbai|Bengaluru|Bangalore|Hyderabad|Pune|Chennai|Gurugram|Gurgaon|Noida|India)\b", text, re.I)
    return m2.group(0) if m2 else None

def fetch(endpoint_url: str, max_pages: int = 3):
    base = "https://search.jobs.barclays"
    out, seen = [], set()
    for page in range(1, max_pages + 1):
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

            date_live = None
            nxt, hops = a.find_next(string=True), 0
            while nxt and hops < 15:
                txt = (nxt.strip() if isinstance(nxt, str) else "").strip()
                if re.search(r"\b(\d{1,2}\s+[A-Za-z]{3}|\d{1,2}/\d{2}/\d{4})\b", txt):
                    date_live = txt
                    break
                nxt = nxt.next_element
                hops += 1

            # fetch detail to improve location and a short description
            desc, detail_loc = None, None
            try:
                dsoup = _soup(detail_url)
                detail_loc = _extract_detail_location(dsoup)
                p = dsoup.find("p")
                desc = p.get_text(" ", strip=True) if p else None
            except Exception:
                pass

            location = detail_loc or card_loc  # prefer detail
            out.append({
                "title": title,
                "detail_url": detail_url,
                "location": location,
                "posted": date_live,
                "description": desc
            })
        time.sleep(1.5)
    return out
