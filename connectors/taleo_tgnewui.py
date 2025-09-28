# connectors/taleo_tgnewui.py
import time, requests
from bs4 import BeautifulSoup

HDRS = {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}

def fetch(base_search_url: str, max_pages: int = 4):
    """
    Example that works for many TGNewUI tenants:
      https://jobs.ubs.com/TGnewUI/Search/home/HomeWithPreLoad?partnerid=25008&siteid=5012&PageType=searchResults&SearchType=linkquery&LinkID=4138&locationSearch=India
    """
    all_rows = []
    # Most TGNewUI search URLs are server-rendered; just parse one page
    r = requests.get(base_search_url, headers=HDRS, timeout=45)
    if not r.ok:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.select("a[href*='JobDetail/']"):
        title = a.get_text(" ", strip=True)
        href  = a.get("href") or ""
        li = a.find_parent("li") or a.parent
        loc = None
        if li:
            loc_el = li.select_one(".jobLocation, .job-location, .location")
            if loc_el:
                loc = loc_el.get_text(" ", strip=True)
        if title and href:
            all_rows.append({"title": title, "location": loc, "detail_url": href, "description": None, "req_id": None, "posted": None})
    # de-dupe
    seen, out = set(), []
    for j in all_rows:
        k = (j["title"], j["detail_url"])
        if k not in seen:
            seen.add(k); out.append(j)
    return out
