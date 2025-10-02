# connectors/play_renderer.py
from contextlib import contextmanager
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from playwright.sync_api import sync_playwright, Route, Request

DEFAULT_TIMEOUT = 60000  # ms (many career sites need >30s)

BLOCK_HOST_SNIPPETS = (
    "datadoghq", "googletagmanager", "google-analytics", "doubleclick",
    "hotjar", "segment", "mixpanel", "newrelic", "sentry.io"
)

def _should_block(req: Request) -> bool:
    url = req.url.lower()
    return any(snippet in url for snippet in BLOCK_HOST_SNIPPETS)

@contextmanager
def browser_ctx(headless: bool = True):
    """
    Chromium context with a realistic UA and a couple of stability flags.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/123.0.0.0 Safari/537.36"),
            viewport={"width": 1366, "height": 768},
            locale="en-US",
        )

        # Trim noisy third-party analytics that can slow/flake runs
        context.route("**/*", lambda route, req: route.abort() if _should_block(req) else route.continue_())

        try:
            yield context
        finally:
            context.close()
            browser.close()

def _update_query_param(url: str, key: str, value) -> str:
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q[str(key)] = str(value)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))

def _load(page, url: str, wait_for: str | None):
    page.goto(url, timeout=DEFAULT_TIMEOUT, wait_until="domcontentloaded")
    # settle network → many SPAs finish rendering after XHRs
    page.wait_for_load_state("networkidle", timeout=DEFAULT_TIMEOUT)
    if wait_for:
        page.wait_for_selector(wait_for, timeout=DEFAULT_TIMEOUT)

def _smart_scroll(page, steps: int = 12):
    """
    Nudge infinite/virtual lists to render more rows.
    """
    h = page.evaluate("() => document.body.scrollHeight")
    for _ in range(steps):
        page.mouse.wheel(0, h)
        page.wait_for_load_state("networkidle", timeout=DEFAULT_TIMEOUT // 2)

def render_and_extract(
    url: str,
    selectors: dict,
    max_pages: int = 1,
    next_selector: str | None = None,
    wait_for: str | None = None,
    force_india: bool = False,
    page_param: str | None = None,
    step: int = 1,
    do_scroll: bool = True,
):
    """
    Universal renderer/scraper for JS-heavy job boards.

    selectors = { card, title?, link?, location?, posted? }
    Pagination:
      - If page_param is given, we call the same URL with page_param=1..N (or startrow+=step)
      - Else if next_selector is given, we click it up to max_pages-1 times
      - Else we just scrape the first page
    """
    out, seen = [], set()

    def scrape_one(page):
        # Some sites need a tiny scroll to render anchors/cards
        if do_scroll:
            _smart_scroll(page)

        cards = page.query_selector_all(selectors["card"])
        for card in cards:
            link_sel = selectors.get("link", "a")
            link_el = card.query_selector(link_sel)
            if not link_el:
                continue
            href = (link_el.get_attribute("href") or "").strip()
            if not href:
                continue
            # absolutize relative hrefs
            if href.startswith("/"):
                # build absolute based on current page URL
                p = urlparse(page.url)
                href = f"{p.scheme}://{p.netloc}{href}"

            title_sel = selectors.get("title", link_sel)
            title_el = card.query_selector(title_sel) or link_el
            title = (title_el.inner_text().strip() if title_el else None)

            loc = None
            loc_sel = selectors.get("location")
            if loc_sel:
                le = card.query_selector(loc_sel)
                if le:
                    loc = (le.inner_text() or "").strip()
            if not loc and force_india:
                loc = "India"

            posted = None
            posted_sel = selectors.get("posted")
            if posted_sel:
                pe = card.query_selector(posted_sel)
                if pe:
                    posted = (pe.inner_text() or "").strip()

            if not title:
                continue
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
                "posted": posted,
            })

    with browser_ctx(headless=True) as ctx:
        page = ctx.new_page()

        # --- Pagination strategy A: query parameter (page/startrow)
        if page_param:
            for i in range(max_pages):
                val = i * step if step != 1 else (i + 1)
                pg_url = _update_query_param(url, page_param, val)
                _load(page, pg_url, wait_for)
                scrape_one(page)

        else:
            # --- Strategy B: click next button
            _load(page, url, wait_for)
            scrape_one(page)

            if next_selector:
                for _ in range(max_pages - 1):
                    btn = page.query_selector(next_selector)
                    if not btn:
                        break
                    btn.click()
                    page.wait_for_load_state("networkidle", timeout=DEFAULT_TIMEOUT)
                    if wait_for:
                        page.wait_for_selector(wait_for, timeout=DEFAULT_TIMEOUT)
                    scrape_one(page)

    return out
