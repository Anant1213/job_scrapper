# connectors/play_renderer.py
from contextlib import contextmanager
from playwright.sync_api import sync_playwright
from urllib.parse import urljoin

DEFAULT_TIMEOUT = 30000  # ms

@contextmanager
def browser_ctx(headless=True):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        try:
            yield context
        finally:
            context.close()
            browser.close()

def render_and_extract(url: str, selectors: dict, max_pages: int = 1,
                       next_selector: str | None = None, wait_for: str | None = None,
                       force_india=False):
    """
    selectors = { card, title?, link?, location?, posted? }
    If next_selector provided, click it up to max_pages.
    """
    out, seen = [], set()
    with browser_ctx(headless=True) as ctx:
        page = ctx.new_page()
        page.goto(url, timeout=DEFAULT_TIMEOUT, wait_until="domcontentloaded")
        if wait_for:
            page.wait_for_selector(wait_for, timeout=DEFAULT_TIMEOUT)

        def scrape_one():
            cards = page.query_selector_all(selectors["card"])
            for card in cards:
                link_el = card.query_selector(selectors.get("link", "a"))
                if not link_el:
                    continue
                href = link_el.get_attribute("href") or ""
                # absolutize if needed
                if href.startswith("/"):
                    href = urljoin(page.url, href)
                title_el = card.query_selector(selectors.get("title", "a")) or link_el
                title = title_el.inner_text().strip() if title_el else None

                loc = None
                loc_sel = selectors.get("location")
                if loc_sel:
                    le = card.query_selector(loc_sel)
                    if le:
                        loc = le.inner_text().strip()
                if not loc and force_india:
                    loc = "India"

                posted = None
                posted_sel = selectors.get("posted")
                if posted_sel:
                    pe = card.query_selector(posted_sel)
                    if pe:
                        posted = pe.inner_text().strip()

                if not title or not href:
                    continue
                k = (title, href)
                if k in seen:
                    continue
                seen.add(k)
                out.append({
                    "title": title, "location": loc, "detail_url": href,
                    "description": None, "req_id": None, "posted": posted
                })

        scrape_one()

        # paginate
        if next_selector:
            for _ in range(max_pages - 1):
                btn = page.query_selector(next_selector)
                if not btn:
                    break
                btn.click()
                if wait_for:
                    page.wait_for_selector(wait_for)
                scrape_one()

    return out
