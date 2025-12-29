# connectors/play_renderer.py
"""
Playwright-based browser renderer for JavaScript-heavy job sites.
Provides a universal scraper for SPAs and dynamic content.
"""
from contextlib import contextmanager
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

try:
    from playwright.sync_api import sync_playwright, Request
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("Warning: playwright not installed. Browser rendering disabled.")


DEFAULT_TIMEOUT = 60000  # ms (many career sites need >30s)

# Analytics/tracking domains to block for faster loading
BLOCK_HOST_SNIPPETS = (
    "datadoghq", "googletagmanager", "google-analytics", "doubleclick",
    "hotjar", "segment", "mixpanel", "newrelic", "sentry.io"
)


def _should_block(req) -> bool:
    """Check if request should be blocked (analytics, tracking, etc)."""
    url = req.url.lower()
    return any(snippet in url for snippet in BLOCK_HOST_SNIPPETS)


@contextmanager
def browser_ctx(headless: bool = True):
    """
    Create a Chromium browser context with realistic settings.
    
    Features:
    - Realistic user agent
    - Blocks analytics/tracking requests
    - Anti-bot detection measures
    """
    if not PLAYWRIGHT_AVAILABLE:
        raise RuntimeError("Playwright not installed. Run: pip install playwright && playwright install chromium")
    
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
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 768},
            locale="en-US",
        )

        # Block noisy third-party analytics
        context.route("**/*", lambda route, req: route.abort() if _should_block(req) else route.continue_())

        try:
            yield context
        finally:
            context.close()
            browser.close()


def _update_query_param(url: str, key: str, value: Any) -> str:
    """Update or add a query parameter to URL."""
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q[str(key)] = str(value)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))


def _load(page, url: str, wait_for: Optional[str]):
    """Load page and wait for content."""
    page.goto(url, timeout=DEFAULT_TIMEOUT, wait_until="domcontentloaded")
    # Wait for network to settle
    page.wait_for_load_state("networkidle", timeout=DEFAULT_TIMEOUT)
    if wait_for:
        page.wait_for_selector(wait_for, timeout=DEFAULT_TIMEOUT)


def _smart_scroll(page, steps: int = 12):
    """
    Scroll page to trigger lazy loading of content.
    Useful for infinite scroll or virtual lists.
    """
    try:
        h = page.evaluate("() => document.body.scrollHeight")
        for _ in range(steps):
            page.mouse.wheel(0, h)
            page.wait_for_load_state("networkidle", timeout=DEFAULT_TIMEOUT // 2)
    except Exception:
        pass  # Scroll failure is not critical


def render_and_extract(
    url: str,
    selectors: Dict[str, str],
    max_pages: int = 1,
    next_selector: Optional[str] = None,
    wait_for: Optional[str] = None,
    force_india: bool = False,
    page_param: Optional[str] = None,
    step: int = 1,
    do_scroll: bool = True,
) -> List[dict]:
    """
    Universal renderer/scraper for JS-heavy job boards.
    
    Args:
        url: Starting URL
        selectors: Dict with keys: card, title?, link?, location?, posted?
        max_pages: Maximum pages to scrape
        next_selector: CSS selector for "next" button (pagination strategy B)
        wait_for: CSS selector to wait for before scraping
        force_india: Default location to "India" if not found
        page_param: Query parameter for pagination (strategy A: ?page=N)
        step: Page number increment (1 for page=1,2,3 or 25 for start=0,25,50)
        do_scroll: Whether to scroll to trigger lazy loading
        
    Pagination strategies:
        A. Query parameter: Uses page_param with incrementing values
        B. Click next: Clicks next_selector button
        C. Single page: Just scrape the first page
        
    Returns:
        List of job dicts
    """
    if not PLAYWRIGHT_AVAILABLE:
        print("  Error: Playwright not available")
        return []
    
    out, seen = [], set()

    def scrape_one(page):
        """Scrape jobs from current page state."""
        if do_scroll:
            _smart_scroll(page)

        try:
            cards = page.query_selector_all(selectors["card"])
        except Exception:
            cards = []
        
        for card in cards:
            try:
                link_sel = selectors.get("link", "a")
                link_el = card.query_selector(link_sel)
                if not link_el:
                    continue
                    
                href = (link_el.get_attribute("href") or "").strip()
                if not href:
                    continue
                    
                # Absolutize relative hrefs
                if href.startswith("/"):
                    p = urlparse(page.url)
                    href = f"{p.scheme}://{p.netloc}{href}"

                title_sel = selectors.get("title", link_sel)
                title_el = card.query_selector(title_sel) or link_el
                title = (title_el.inner_text().strip() if title_el else None)

                # Extract location
                loc = None
                loc_sel = selectors.get("location")
                if loc_sel:
                    le = card.query_selector(loc_sel)
                    if le:
                        loc = (le.inner_text() or "").strip()
                if not loc and force_india:
                    loc = "India"

                # Extract posted date
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
            except Exception:
                continue

    try:
        with browser_ctx(headless=True) as ctx:
            page = ctx.new_page()

            # --- Pagination strategy A: query parameter (page/startrow)
            if page_param:
                for i in range(max_pages):
                    # FIX: Correct pagination value calculation
                    # For step=1: pages 1, 2, 3, 4, 5
                    # For step=25: start=0, 25, 50, 75, 100
                    if step == 1:
                        val = i + 1  # 1-indexed pages
                    else:
                        val = i * step  # 0-indexed with step
                    
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

    except Exception as e:
        print(f"  Playwright error: {e}")

    return out
