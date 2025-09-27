# connectors/goldman_higher.py
import re, time, json
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "JobScoutBot/0.1 (+github actions)"}
REQ_TIMEOUT = 25

def _with_page(url: str, page: int) -> str:
    p = urlparse(url)
    qs = parse_qs(p.query)
    qs["page"] = [str(page)]
    new_q = urlencode({k: v[0] if isinstance(v, list) else v for k, v in qs.items()})
    return p._replace(query=new_q).geturl()

def _soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def _from_json_ld(soup: BeautifulSoup):
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "")
            if isinstance(data, list):
                for d in data:
                    if isinstance(d, dict) and d.get("@type") == "JobPosting":
                        return d
            elif isinstance(data, dict) and data.get("@type") == "JobPosting":
                return data
        except Exception:
            continue
    return None

def _guess_location(text: str) -> str | None:
    if not text: return None
    s = " ".join(text.split())
    m = re.search(r"(Mumbai|Bengaluru|Bangalore|Hyderabad|Pune|Chennai|Gurugram|Gurgaon|Noida).*?India", s, re.I)
    if m: return m.group(0)
    m2 = re.search(r"[A-Za-z ]+,\s*(Karnataka|Maharashtra|Telangana|Tamil Nadu|Delhi|Haryana),\s*India", s, re.I)
    return m2.group(0) if m2 else None

def fetch(endpoint_url: str, max_pages: int = 3):
    """
    Scrape higher.gs.com result pages, follow role pages, and return a normalized list.
    endpoint_url example: https://higher.gs.com/results?search=data&sort=RELEVANCE
    """
    base = f"{urlparse(endpoint_url).scheme}://{urlparse(endpoint_url).netloc}"
    out, seen = [], set()

    for page in range(1, max_pages + 1):
        url = _with_page(endpoint_url, page)
        soup = _soup(url)

        # role links look like /roles/<id>
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not re.match(r"^/roles/\d+", href): 
                continue
            role_url = urljoin(base, href)
            if role_url in seen:
                continue
            seen.add(role_url)

            # fetch detail page for robust title/location/desc
            try:
                dsoup = _soup(role_url)
            except Exception:
                continue

            title = None
            h1 = dsoup.find("h1")
            if h1:
                title = h1.get_text(" ", strip=True)
            jd = _from_json_ld(dsoup)
            location, posted, desc = None, None, None
            if jd:
                title = jd.get("title") or title
                posted = jd.get("datePosted") or jd.get("datePublished")
                # location from JSON-LD
                loc_obj = jd.get("jobLocation") or jd.get("jobLocationType")
                if isinstance(loc_obj, dict):
                    addr = loc_obj.get("address") or {}
                    city = addr.get("addressLocality")
                    region = addr.get("addressRegion")
                    country = addr.get("addressCountry")
                    location = ", ".join([x for x in [city, region, country] if x])
                desc = (jd.get("description") or "")[:2000]

            if not location:
                # fallback: scan visible text near top
                hero = dsoup.find(text=re.compile(r"India", re.I))
                if hero:
                    location = _guess_location(hero.parent.get_text(" ", strip=True))
                if not location:
                    location = _guess_location(dsoup.get_text(" ", strip=True))

            # req id from URL if present
            m = re.search(r"/roles/(\d+)", role_url)
            req_id = m.group(1) if m else None

            out.append({
                "title": title,
                "detail_url": role_url,
                "location": location,
                "posted": posted,
                "description": desc,
                "req_id": req_id
            })
        time.sleep(1.0)

    return out
