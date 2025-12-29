# connectors/improved_scrapers.py
"""
Improved scrapers for problematic sites.
Uses advanced techniques to bypass anti-bot protection.
"""
from typing import List
import re
import time
import json

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


def fetch_hsbc_improved(max_jobs: int = 50) -> List[dict]:
    """
    IMPROVED HSBC scraper using direct API approach.
    """
    if not PLAYWRIGHT_AVAILABLE:
        return []
    
    jobs = []
    print("  Scraping HSBC (IMPROVED - API approach)...")
    
    import requests
    
    try:
        # HSBC uses an API endpoint
        url = 'https://mycareer.hsbc.com/api/jobs'
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Referer': 'https://mycareer.hsbc.com/en_GB/external/SearchJobs',
        }
        
        params = {
            'location': 'India',
            'page': 1,
            'limit': max_jobs,
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.ok:
            data = response.json()
            job_list = data.get('jobs', []) or data.get('items', []) or data
            
            for job in job_list[:max_jobs]:
                if isinstance(job, dict):
                    title = job.get('title') or job.get('jobTitle') or job.get('name')
                    location = job.get('location') or job.get('city') or 'India'
                    job_url = job.get('url') or job.get('jobUrl') or 'https://mycareer.hsbc.com'
                    
                    if title and 'india' in str(location).lower():
                        jobs.append({
                            'title': str(title)[:100],
                            'location': str(location),
                            'detail_url': job_url,
                            'description': job.get('description', ''),
                            'posted': job.get('postedDate') or job.get('created'),
                            'req_id': job.get('id') or job.get('requisitionId'),
                            'source': 'HSBC Official'
                        })
            
            print(f"    Found {len(jobs)} jobs via API")
        else:
            print(f"    API returned status {response.status_code}, falling back to browser")
            # Fallback to browser scraping
            jobs = _hsbc_browser_fallback(max_jobs)
            
    except Exception as e:
        print(f"    API Error: {e}, trying browser fallback")
        jobs = _hsbc_browser_fallback(max_jobs)
    
    return jobs


def _hsbc_browser_fallback(max_jobs: int) -> List[dict]:
    """Fallback for HSBC using browser."""
    jobs = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            # Try simpler URL
            url = 'https://www.hsbc.com/careers/find-a-job#?country=India'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(10000)
            
            # Accept cookies
            try:
                accept = page.query_selector('#onetrust-accept-btn-handler')
                if accept:
                    accept.click()
                    page.wait_for_timeout(2000)
            except:
                pass
            
            # Scroll to load jobs
            for _ in range(5):
                page.mouse.wheel(0, 1500)
                page.wait_for_timeout(1500)
            
            # Get job links
            job_links = page.query_selector_all('a[href*="job"], a[href*="careers"], .job-title')
            
            for link in job_links[:max_jobs]:
                try:
                    text = link.inner_text().strip()
                    href = link.get_attribute('href') or ''
                    
                    if text and len(text) > 15:
                        jobs.append({
                            'title': text[:100],
                            'location': 'India',
                            'detail_url': href if href.startswith('http') else 'https://www.hsbc.com' + href,
                            'description': None,
                            'posted': None,
                            'req_id': None,
                            'source': 'HSBC Official'
                        })
                except:
                    continue
            
        except Exception as e:
            print(f"    Browser fallback error: {e}")
        finally:
            browser.close()
    
    return jobs


def fetch_wells_fargo_improved(max_jobs: int = 50) -> List[dict]:
    """
    IMPROVED Wells Fargo scraper.
    """
    if not PLAYWRIGHT_AVAILABLE:
        return []
    
    jobs = []
    print("  Scraping Wells Fargo (IMPROVED)...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        try:
            # Use search with India parameter
            url = 'https://www.wellsfargojobs.com/en/jobs/?q=&location=India'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(10000)
            
            # Accept cookies
            try:
                accept = page.query_selector('#onetrust-accept-btn-handler, button:has-text("Accept")')
                if accept:
                    accept.click()
                    page.wait_for_timeout(3000)
            except:
                pass
            
            # Wait for jobs to load
            page.wait_for_timeout(5000)
            
            # Scroll to load more
            for _ in range(5):
                page.mouse.wheel(0, 1500)
                page.wait_for_timeout(1500)
            
            # Find job cards/links
            job_elements = page.query_selector_all('a[href*="/job/"], .job-title, [data-job-id]')
            
            for elem in job_elements[:max_jobs]:
                try:
                    text = elem.inner_text().strip()
                    href = elem.get_attribute('href') or ''
                    
                    # Filter for India
                    if text and len(text) > 15:
                        if href.startswith('/'):
                            href = 'https://www.wellsfargojobs.com' + href
                        
                        jobs.append({
                            'title': text[:100],
                            'location': 'India',
                            'detail_url': href,
                            'description': None,
                            'posted': None,
                            'req_id': None,
                            'source': 'Wells Fargo Official'
                        })
                except:
                    continue
            
            print(f"    Found {len(jobs)} jobs")
            
        except Exception as e:
            print(f"    Error: {e}")
        finally:
            browser.close()
    
    return jobs


def fetch_deutsche_bank_improved(max_jobs: int = 50) -> List[dict]:
    """
    IMPROVED Deutsche Bank scraper.
    """
    if not PLAYWRIGHT_AVAILABLE:
        return []
    
    jobs = []
    print("  Scraping Deutsche Bank (IMPROVED)...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        try:
            # Direct search URL with India
            url = 'https://careers.db.com/search/?q=&locationsearch=India'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(10000)
            
            # Accept cookies
            try:
                accept = page.query_selector('[data-testid="uc-accept-all-button"], #onetrust-accept-btn-handler')
                if accept:
                    accept.click()
                    page.wait_for_timeout(3000)
            except:
                pass
            
            # Scroll
            for _ in range(5):
                page.mouse.wheel(0, 1500)
                page.wait_for_timeout(1000)
            
            # Find job elements
            job_links = page.query_selector_all('a[href*="/job/"], a.jobTitle, .job-item a')
            
            for link in job_links[:max_jobs]:
                try:
                    text = link.inner_text().strip()
                    href = link.get_attribute('href') or ''
                    
                    if text and len(text) > 15:
                        if href.startswith('/'):
                            href = 'https://careers.db.com' + href
                        
                        jobs.append({
                            'title': text[:100],
                            'location': 'India',
                            'detail_url': href,
                            'description': None,
                            'posted': None,
                            'req_id': None,
                            'source': 'Deutsche Bank Official'
                        })
                except:
                    continue
            
            print(f"    Found {len(jobs)} jobs")
            
        except Exception as e:
            print(f"    Error: {e}")
        finally:
            browser.close()
    
    return jobs


def fetch_blackrock_improved(max_jobs: int = 50) -> List[dict]:
    """
    IMPROVED BlackRock scraper with better targeting.
    """
    if not PLAYWRIGHT_AVAILABLE:
        return []
    
    jobs = []
    print("  Scraping BlackRock (IMPROVED)...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        try:
            # BlackRock India jobs
            url = 'https://careers.blackrock.com/early-careers/search-jobs/India'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(10000)
            
            # Accept cookies
            try:
                accept = page.query_selector('#onetrust-accept-btn-handler')
                if accept:
                    accept.click()
                    page.wait_for_timeout(3000)
            except:
                pass
            
            # Scroll to load jobs
            for _ in range(5):
                page.mouse.wheel(0, 1500)
                page.wait_for_timeout(1500)
            
            # Find job elements
            job_elements = page.query_selector_all('a[href*="/job"], .job-title, [data-job-title]')
            
            # Also try extracting from text
            body = page.inner_text('body')
            lines = body.split('\n')
            
            for line in lines:
                line = line.strip()
                if any(word in line.lower() for word in ['analyst', 'associate', 'engineer', 'manager', 'specialist']):
                    if 15 < len(line) < 100 and line not in [j['title'] for j in jobs]:
                        jobs.append({
                            'title': line,
                            'location': 'India',
                            'detail_url': 'https://careers.blackrock.com/early-careers/search-jobs/India',
                            'description': None,
                            'posted': None,
                            'req_id': None,
                            'source': 'BlackRock Official'
                        })
                        
                        if len(jobs) >= max_jobs:
                            break
            
            print(f"    Found {len(jobs)} jobs")
            
        except Exception as e:
            print(f"    Error: {e}")
        finally:
            browser.close()
    
    return jobs


def fetch_ubs_improved(max_jobs: int = 50) -> List[dict]:
    """
    IMPROVED UBS scraper.
    """
    if not PLAYWRIGHT_AVAILABLE:
        return []
    
    jobs = []
    print("  Scraping UBS (IMPROVED)...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        try:
            # UBS Taleo system with India search
            url = 'https://jobs.ubs.com/TGnewUI/Search/Home/Home?partnerid=25008&siteid=5012#jobSearch'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(10000)
            
            # Try to search for India
            try:
                location_input = page.query_selector('input[placeholder*="Location"], input[name*="location"]')
                if location_input:
                    location_input.fill('India')
                    page.wait_for_timeout(2000)
                    page.keyboard.press('Enter')
                    page.wait_for_timeout(5000)
            except:
                pass
            
            # Scroll
            for _ in range(3):
                page.mouse.wheel(0, 1500)
                page.wait_for_timeout(1000)
            
            # Find jobs
            job_links = page.query_selector_all('a[href*="jobDetails"], .jobTitle, [data-job-id]')
            
            for link in job_links[:max_jobs]:
                try:
                    text = link.inner_text().strip()
                    href = link.get_attribute('href') or ''
                    
                    if text and len(text) > 10:
                        jobs.append({
                            'title': text[:100],
                            'location': 'India',
                            'detail_url': href if href.startswith('http') else 'https://jobs.ubs.com' + href,
                            'description': None,
                            'posted': None,
                            'req_id': None,
                            'source': 'UBS Official'
                        })
                except:
                    continue
            
            print(f"    Found {len(jobs)} jobs")
            
        except Exception as e:
            print(f"    Error: {e}")
        finally:
            browser.close()
    
    return jobs


if __name__ == "__main__":
    # Test all improved scrapers
    print("Testing improved scrapers...\n")
    
    all_results = {
        'HSBC': fetch_hsbc_improved(20),
        'Wells Fargo': fetch_wells_fargo_improved(20),
        'Deutsche Bank': fetch_deutsche_bank_improved(20),
        'BlackRock': fetch_blackrock_improved(20),
        'UBS': fetch_ubs_improved(20),
    }
    
    print("\n=== RESULTS ===")
    for company, jobs in all_results.items():
        print(f"{company}: {len(jobs)} jobs")
        for job in jobs[:3]:
            print(f"  - {job['title'][:60]}")
