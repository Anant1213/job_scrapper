# connectors/official_sites.py
"""
Scraper for official company career sites using Playwright.
These are the actual career pages of financial institutions.
"""
from typing import List, Optional
import re
import time

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("Warning: playwright not installed")


def _create_browser():
    """Create browser context with common settings."""
    p = sync_playwright().start()
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(
        user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        viewport={'width': 1920, 'height': 1080}
    )
    return p, browser, context


def fetch_goldman_sachs(max_jobs: int = 100) -> List[dict]:
    """
    Fetch jobs from Goldman Sachs official career site (higher.gs.com).
    
    Returns:
        List of job dicts from official Goldman Sachs careers
    """
    if not PLAYWRIGHT_AVAILABLE:
        print("Playwright not available")
        return []
    
    jobs = []
    print("  Scraping higher.gs.com (official GS careers)...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        try:
            # Goldman Sachs Higher platform
            url = 'https://higher.gs.com/roles?location=India'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(5000)
            
            # Scroll to load more jobs
            for _ in range(3):
                page.mouse.wheel(0, 2000)
                page.wait_for_timeout(1000)
            
            # Find role links
            role_links = page.query_selector_all('a[href*="/roles/"]')
            seen = set()
            
            for link in role_links[:max_jobs]:
                try:
                    href = link.get_attribute('href') or ''
                    text = link.inner_text().strip()
                    
                    if not href or href in seen or not text:
                        continue
                    seen.add(href)
                    
                    # Extract role ID from URL
                    match = re.search(r'/roles/(\d+)', href)
                    req_id = match.group(1) if match else None
                    
                    # Make URL absolute
                    if href.startswith('/'):
                        href = 'https://higher.gs.com' + href
                    
                    jobs.append({
                        'title': text[:100],
                        'detail_url': href,
                        'location': 'India',  # Filtered by location=India
                        'posted': None,
                        'description': None,
                        'req_id': req_id,
                        'source': 'Goldman Sachs Official'
                    })
                except Exception:
                    continue
            
            print(f"    Found {len(jobs)} jobs")
            
        except Exception as e:
            print(f"    Error: {e}")
        finally:
            browser.close()
    
    return jobs


def fetch_barclays(max_jobs: int = 100) -> List[dict]:
    """
    Fetch jobs from Barclays official career site.
    
    Returns:
        List of job dicts from official Barclays careers
    """
    if not PLAYWRIGHT_AVAILABLE:
        print("Playwright not available")
        return []
    
    jobs = []
    print("  Scraping search.jobs.barclays (official Barclays careers)...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        try:
            url = 'https://search.jobs.barclays/jobs?location=India&stretch=0&stretchUnit=MILES'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(5000)
            
            # Scroll to load more
            for _ in range(3):
                page.mouse.wheel(0, 2000)
                page.wait_for_timeout(1000)
            
            # Find job links
            job_links = page.query_selector_all('a[href*="/job/"]')
            seen = set()
            
            for link in job_links[:max_jobs]:
                try:
                    href = link.get_attribute('href') or ''
                    text = link.inner_text().strip()
                    
                    if not href or href in seen or not text or len(text) < 5:
                        continue
                    seen.add(href)
                    
                    # Make URL absolute
                    if href.startswith('/'):
                        href = 'https://search.jobs.barclays' + href
                    
                    jobs.append({
                        'title': text[:100],
                        'detail_url': href,
                        'location': 'India',
                        'posted': None,
                        'description': None,
                        'req_id': None,
                        'source': 'Barclays Official'
                    })
                except Exception:
                    continue
            
            print(f"    Found {len(jobs)} jobs")
            
        except Exception as e:
            print(f"    Error: {e}")
        finally:
            browser.close()
    
    return jobs


def fetch_jpmorgan(max_jobs: int = 100) -> List[dict]:
    """
    Fetch jobs from JPMorgan official career site.
    
    Returns:
        List of job dicts from official JPMorgan careers
    """
    if not PLAYWRIGHT_AVAILABLE:
        print("Playwright not available")
        return []
    
    jobs = []
    print("  Scraping careers.jpmorgan.com (official JPM careers)...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        try:
            # JPMorgan uses Oracle HCM
            url = 'https://careers.jpmorgan.com/global/en/search?q=*&location=India'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(8000)
            
            # Accept cookies if present
            try:
                accept_btn = page.query_selector('button:has-text("Accept")')
                if accept_btn:
                    accept_btn.click()
                    page.wait_for_timeout(2000)
            except:
                pass
            
            # Look for job cards
            job_cards = page.query_selector_all('[data-testid="job-card"], .job-tile, article, .result-item')
            
            for card in job_cards[:max_jobs]:
                try:
                    # Try to get title
                    title_el = card.query_selector('h2, h3, .job-title, a')
                    if not title_el:
                        continue
                    
                    title = title_el.inner_text().strip()
                    
                    # Try to get link
                    link_el = card.query_selector('a[href]')
                    href = link_el.get_attribute('href') if link_el else None
                    
                    if not title or len(title) < 5:
                        continue
                    
                    if href and href.startswith('/'):
                        href = 'https://careers.jpmorgan.com' + href
                    
                    jobs.append({
                        'title': title[:100],
                        'detail_url': href or 'https://careers.jpmorgan.com',
                        'location': 'India',
                        'posted': None,
                        'description': None,
                        'req_id': None,
                        'source': 'JPMorgan Official'
                    })
                except Exception:
                    continue
            
            print(f"    Found {len(jobs)} jobs")
            
        except Exception as e:
            print(f"    Error: {e}")
        finally:
            browser.close()
    
    return jobs


def fetch_deutsche_bank(max_jobs: int = 100) -> List[dict]:
    """
    Fetch jobs from Deutsche Bank official career site.
    """
    if not PLAYWRIGHT_AVAILABLE:
        print("Playwright not available")
        return []
    
    jobs = []
    print("  Scraping careers.db.com (official Deutsche Bank careers)...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        try:
            url = 'https://careers.db.com/professionals/search-jobs?locations=India'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(5000)
            
            # Accept cookies
            try:
                accept = page.query_selector('[data-testid="uc-accept-all-button"], button:has-text("Accept")')
                if accept:
                    accept.click()
                    page.wait_for_timeout(2000)
            except:
                pass
            
            # Find job elements
            job_links = page.query_selector_all('a[href*="/job/"], a[href*="/jobs/"]')
            seen = set()
            
            for link in job_links[:max_jobs]:
                try:
                    href = link.get_attribute('href') or ''
                    text = link.inner_text().strip()
                    
                    if not href or href in seen or not text or len(text) < 5:
                        continue
                    seen.add(href)
                    
                    if href.startswith('/'):
                        href = 'https://careers.db.com' + href
                    
                    jobs.append({
                        'title': text[:100],
                        'detail_url': href,
                        'location': 'India',
                        'posted': None,
                        'description': None,
                        'req_id': None,
                        'source': 'Deutsche Bank Official'
                    })
                except Exception:
                    continue
            
            print(f"    Found {len(jobs)} jobs")
            
        except Exception as e:
            print(f"    Error: {e}")
        finally:
            browser.close()
    
    return jobs


def fetch_all_official() -> List[dict]:
    """
    Fetch jobs from all official career sites.
    
    Returns:
        Combined list of jobs from all official sites
    """
    all_jobs = []
    
    print("\n" + "="*60)
    print("SCRAPING OFFICIAL COMPANY CAREER SITES")
    print("="*60 + "\n")
    
    # Goldman Sachs - higher.gs.com
    try:
        gs_jobs = fetch_goldman_sachs()
        all_jobs.extend(gs_jobs)
    except Exception as e:
        print(f"GS Error: {e}")
    
    time.sleep(2)
    
    # Barclays - search.jobs.barclays
    try:
        barclays_jobs = fetch_barclays()
        all_jobs.extend(barclays_jobs)
    except Exception as e:
        print(f"Barclays Error: {e}")
    
    time.sleep(2)
    
    # JPMorgan
    try:
        jpm_jobs = fetch_jpmorgan()
        all_jobs.extend(jpm_jobs)
    except Exception as e:
        print(f"JPM Error: {e}")
    
    time.sleep(2)
    
    # Deutsche Bank
    try:
        db_jobs = fetch_deutsche_bank()
        all_jobs.extend(db_jobs)
    except Exception as e:
        print(f"DB Error: {e}")
    
    print(f"\nTotal from official sites: {len(all_jobs)} jobs")
    return all_jobs


if __name__ == "__main__":
    jobs = fetch_all_official()
    for job in jobs[:10]:
        print(f"- {job['title']} ({job['source']})")
