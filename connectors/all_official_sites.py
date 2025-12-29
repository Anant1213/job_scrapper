# connectors/all_official_sites.py
"""
Comprehensive scraper for all official company career sites.
Uses Playwright for JavaScript-rendered pages.
"""
from typing import List, Dict
import re
import time

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


INDIA_CITIES = ['India', 'Bengaluru', 'Bangalore', 'Mumbai', 'Hyderabad', 'Pune', 
                'Chennai', 'Gurgaon', 'Gurugram', 'Noida', 'Delhi', 'Kolkata']


def _extract_jobs_from_text(body: str, max_jobs: int = 100) -> List[dict]:
    """Extract jobs from page text content."""
    jobs = []
    lines = [l.strip() for l in body.split('\n') if l.strip()]
    
    i = 0
    while i < len(lines) - 1 and len(jobs) < max_jobs:
        line = lines[i]
        
        # Skip navigation/UI text
        skip_words = ['skip to', 'search', 'filter', 'posting date', 'categories', 
                      'open jobs', 'near location', 'cookie', 'privacy', 'accept',
                      'sign in', 'log in', 'create account', 'refine', 'sort by']
        if any(skip in line.lower() for skip in skip_words):
            i += 1
            continue
        
        # Check if this looks like a job title
        if 15 < len(line) < 150 and not any(city in line for city in INDIA_CITIES):
            # Check if next line is an India location
            if i + 1 < len(lines):
                next_line = lines[i + 1]
                if any(city in next_line for city in INDIA_CITIES):
                    jobs.append({
                        'title': line,
                        'location': next_line,
                        'description': lines[i + 2] if i + 2 < len(lines) else None,
                    })
                    i += 2
                    continue
        
        i += 1
    
    return jobs


def fetch_goldman_sachs(max_jobs: int = 100) -> List[dict]:
    """Scrape Goldman Sachs from higher.gs.com."""
    if not PLAYWRIGHT_AVAILABLE:
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
            url = 'https://higher.gs.com/roles?location=India'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(5000)
            
            # Scroll to load more
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
                    
                    if href.startswith('/'):
                        href = 'https://higher.gs.com' + href
                    
                    jobs.append({
                        'title': text[:100],
                        'detail_url': href,
                        'location': 'India',
                        'posted': None,
                        'description': None,
                        'req_id': re.search(r'/roles/(\d+)', href).group(1) if re.search(r'/roles/(\d+)', href) else None,
                        'source': 'Goldman Sachs Official'
                    })
                except:
                    continue
            
            print(f"    Found {len(jobs)} jobs")
            
        except Exception as e:
            print(f"    Error: {e}")
        finally:
            browser.close()
    
    return jobs


def fetch_barclays(max_jobs: int = 100) -> List[dict]:
    """Scrape Barclays from search.jobs.barclays."""
    if not PLAYWRIGHT_AVAILABLE:
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
            
            for _ in range(3):
                page.mouse.wheel(0, 2000)
                page.wait_for_timeout(1000)
            
            job_links = page.query_selector_all('a[href*="/job/"]')
            seen = set()
            
            for link in job_links[:max_jobs]:
                try:
                    href = link.get_attribute('href') or ''
                    text = link.inner_text().strip()
                    
                    if not href or href in seen or not text or len(text) < 5:
                        continue
                    seen.add(href)
                    
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
                except:
                    continue
            
            print(f"    Found {len(jobs)} jobs")
            
        except Exception as e:
            print(f"    Error: {e}")
        finally:
            browser.close()
    
    return jobs


def fetch_jpmorgan_india(max_jobs: int = 100) -> List[dict]:
    """Scrape JPMorgan from Oracle HCM."""
    if not PLAYWRIGHT_AVAILABLE:
        return []
    
    jobs = []
    print("  Scraping jpmc.fa.oraclecloud.com (official JPMorgan careers)...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        try:
            url = 'https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/jobs'
            page.goto(url, timeout=60000, wait_until='networkidle')
            page.wait_for_timeout(8000)
            
            # Accept privacy notice
            try:
                accept = page.query_selector('button:has-text("ACCEPT")')
                if accept:
                    accept.click()
                    page.wait_for_timeout(2000)
            except:
                pass
            
            # Filter by India
            location_input = page.query_selector('input[placeholder*="City"]')
            if location_input:
                location_input.click()
                page.wait_for_timeout(500)
                location_input.fill('India')
                page.wait_for_timeout(2000)
                page.keyboard.press('Enter')
                page.wait_for_timeout(5000)
            
            # Scroll
            for _ in range(5):
                page.mouse.wheel(0, 1000)
                page.wait_for_timeout(1000)
            
            body = page.inner_text('body')
            
            match = re.search(r'(\d+)\s*(?:Open Jobs|OPEN JOBS)', body)
            if match:
                print(f"    Total jobs available: {match.group(1)}")
            
            raw_jobs = _extract_jobs_from_text(body, max_jobs)
            
            for job in raw_jobs:
                jobs.append({
                    'title': job['title'],
                    'location': job['location'],
                    'detail_url': 'https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/jobs',
                    'description': job.get('description'),
                    'posted': None,
                    'req_id': None,
                    'source': 'JPMorgan Official'
                })
            
            print(f"    Extracted {len(jobs)} jobs")
            
        except Exception as e:
            print(f"    Error: {e}")
        finally:
            browser.close()
    
    return jobs


def fetch_morgan_stanley(max_jobs: int = 100) -> List[dict]:
    """Scrape Morgan Stanley from Eightfold AI platform."""
    if not PLAYWRIGHT_AVAILABLE:
        return []
    
    jobs = []
    print("  Scraping morganstanley.eightfold.ai...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            url = 'https://morganstanley.eightfold.ai/careers?location=India'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(8000)
            
            for _ in range(5):
                page.mouse.wheel(0, 1000)
                page.wait_for_timeout(1000)
            
            body = page.inner_text('body')
            
            match = re.search(r'(\d+)\s*jobs', body, re.I)
            if match:
                print(f"    Total jobs available: {match.group(1)}")
            
            raw_jobs = _extract_jobs_from_text(body, max_jobs)
            
            for job in raw_jobs:
                jobs.append({
                    'title': job['title'],
                    'location': job['location'],
                    'detail_url': 'https://morganstanley.eightfold.ai/careers?location=India',
                    'description': job.get('description'),
                    'posted': None,
                    'req_id': None,
                    'source': 'Morgan Stanley Official'
                })
            
            print(f"    Extracted {len(jobs)} jobs")
            
        except Exception as e:
            print(f"    Error: {e}")
        finally:
            browser.close()
    
    return jobs


def fetch_hsbc(max_jobs: int = 100) -> List[dict]:
    """Scrape HSBC careers - IMPROVED."""
    if not PLAYWRIGHT_AVAILABLE:
        return []
    
    jobs = []
    print("  Scraping mycareer.hsbc.com (IMPROVED)...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        try:
            url = 'https://mycareer.hsbc.com/en_GB/external/SearchJobs?q=&locationsearch=India'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(8000)
            
            # Accept cookies
            try:
                accept = page.query_selector('#onetrust-accept-btn-handler')
                if accept:
                    accept.click()
                    page.wait_for_timeout(2000)
            except:
                pass
            
            # Scroll to load more
            for _ in range(5):
                page.mouse.wheel(0, 1500)
                page.wait_for_timeout(1500)
            
            # Get job cards by looking for specific patterns
            job_cards = page.query_selector_all('a[href*="jobdetails"], a[href*="job"], .job-title, .job-list-item')
            
            if not job_cards:
                # Fallback: extract from text
                body = page.inner_text('body')
                lines = body.split('\n')
                
                for line in lines:
                    line = line.strip()
                    if any(word in line.lower() for word in ['analyst', 'manager', 'engineer', 'associate', 'director', 'officer', 'specialist']):
                        if 20 < len(line) < 100:
                            jobs.append({
                                'title': line,
                                'location': 'India',
                                'detail_url': 'https://mycareer.hsbc.com/en_GB/external/SearchJobs',
                                'description': None,
                                'posted': None,
                                'req_id': None,
                                'source': 'HSBC Official'
                            })
                            if len(jobs) >= max_jobs:
                                break
            else:
                for card in job_cards[:max_jobs]:
                    try:
                        text = card.inner_text().strip()
                        href = card.get_attribute('href') or ''
                        
                        if text and len(text) > 10:
                            jobs.append({
                                'title': text[:100],
                                'location': 'India',
                                'detail_url': href if href.startswith('http') else 'https://mycareer.hsbc.com' + href,
                                'description': None,
                                'posted': None,
                                'req_id': None,
                                'source': 'HSBC Official'
                            })
                    except:
                        continue
            
            print(f"    Found {len(jobs)} jobs")
            
        except Exception as e:
            print(f"    Error: {e}")
        finally:
            browser.close()
    
    return jobs


def fetch_citi(max_jobs: int = 100) -> List[dict]:
    """Scrape Citi careers - IMPROVED."""
    if not PLAYWRIGHT_AVAILABLE:
        return []
    
    jobs = []
    print("  Scraping jobs.citi.com (IMPROVED)...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        try:
            url = 'https://jobs.citi.com/search-jobs/India/287/1'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(8000)
            
            # Accept cookies
            try:
                accept = page.query_selector('#onetrust-accept-btn-handler')
                if accept:
                    accept.click()
                    page.wait_for_timeout(2000)
            except:
                pass
            
            # Scroll multiple times
            for _ in range(8):
                page.mouse.wheel(0, 1500)
                page.wait_for_timeout(1000)
            
            # Find job links
            job_links = page.query_selector_all('a[href*="/job/"]')
            seen = set()
            
            for link in job_links[:max_jobs]:
                try:
                    href = link.get_attribute('href') or ''
                    text = link.inner_text().strip()
                    
                    if not text or len(text) < 10 or text in seen:
                        continue
                    seen.add(text)
                    
                    if href.startswith('/'):
                        href = 'https://jobs.citi.com' + href
                    
                    jobs.append({
                        'title': text[:100],
                        'location': 'India',
                        'detail_url': href,
                        'description': None,
                        'posted': None,
                        'req_id': None,
                        'source': 'Citi Official'
                    })
                except:
                    continue
            
            print(f"    Found {len(jobs)} jobs")
            
        except Exception as e:
            print(f"    Error: {e}")
        finally:
            browser.close()
    
    return jobs


def fetch_nomura(max_jobs: int = 100) -> List[dict]:
    """Scrape Nomura careers (BrassRing)."""
    if not PLAYWRIGHT_AVAILABLE:
        return []
    
    jobs = []
    print("  Scraping careers.nomura.com...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            url = 'https://careers.nomura.com/Nomura/go/Career-Opportunities-India/9050900/'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(5000)
            
            links = page.query_selector_all('a[href*="/job/"]')
            seen = set()
            
            for link in links[:max_jobs]:
                try:
                    href = link.get_attribute('href') or ''
                    text = link.inner_text().strip()
                    
                    if not text or len(text) < 5 or text in seen:
                        continue
                    seen.add(text)
                    
                    if href.startswith('/'):
                        href = 'https://careers.nomura.com' + href
                    
                    jobs.append({
                        'title': text,
                        'location': 'India',
                        'detail_url': href,
                        'description': None,
                        'posted': None,
                        'req_id': None,
                        'source': 'Nomura Official'
                    })
                except:
                    continue
            
            print(f"    Extracted {len(jobs)} jobs")
            
        except Exception as e:
            print(f"    Error: {e}")
        finally:
            browser.close()
    
    return jobs


def fetch_deutsche_bank(max_jobs: int = 100) -> List[dict]:
    """Scrape Deutsche Bank careers."""
    if not PLAYWRIGHT_AVAILABLE:
        return []
    
    jobs = []
    print("  Scraping careers.db.com...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            url = 'https://careers.db.com/professionals/search-jobs#702702020'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(8000)
            
            # Accept cookies
            try:
                accept = page.query_selector('[data-testid="uc-accept-all-button"]')
                if accept:
                    accept.click()
                    page.wait_for_timeout(2000)
            except:
                pass
            
            # Try to filter by India
            try:
                location = page.query_selector('input[placeholder*="Location"], input[name*="location"]')
                if location:
                    location.fill('India')
                    page.wait_for_timeout(3000)
            except:
                pass
            
            # Scroll
            for _ in range(5):
                page.mouse.wheel(0, 1500)
                page.wait_for_timeout(1000)
            
            # Get job links
            job_links = page.query_selector_all('a[href*="/job/"], a[href*="/professionals/"]')
            seen = set()
            
            for link in job_links[:max_jobs]:
                try:
                    href = link.get_attribute('href') or ''
                    text = link.inner_text().strip()
                    
                    if not text or len(text) < 10 or text in seen:
                        continue
                    if 'india' not in text.lower() and 'mumbai' not in text.lower() and 'bangalore' not in text.lower():
                        continue
                    seen.add(text)
                    
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


def fetch_wells_fargo(max_jobs: int = 100) -> List[dict]:
    """Scrape Wells Fargo careers."""
    if not PLAYWRIGHT_AVAILABLE:
        return []
    
    jobs = []
    print("  Scraping wellsfargojobs.com...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            url = 'https://www.wellsfargojobs.com/en/jobs/?search=&location=India'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(8000)
            
            # Accept cookies
            try:
                accept = page.query_selector('#onetrust-accept-btn-handler, button:has-text("Accept")')
                if accept:
                    accept.click()
                    page.wait_for_timeout(2000)
            except:
                pass
            
            # Scroll
            for _ in range(5):
                page.mouse.wheel(0, 1500)
                page.wait_for_timeout(1000)
            
            # Get job links
            job_links = page.query_selector_all('a[href*="/job/"], a[href*="jobs/"]')
            seen = set()
            
            for link in job_links[:max_jobs]:
                try:
                    href = link.get_attribute('href') or ''
                    text = link.inner_text().strip()
                    
                    if not text or len(text) < 10 or text in seen:
                        continue
                    seen.add(text)
                    
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


def fetch_blackrock(max_jobs: int = 100) -> List[dict]:
    """Scrape BlackRock careers."""
    if not PLAYWRIGHT_AVAILABLE:
        return []
    
    jobs = []
    print("  Scraping careers.blackrock.com...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            url = 'https://careers.blackrock.com/job-search-results/?location=India'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(8000)
            
            # Accept cookies
            try:
                accept = page.query_selector('#onetrust-accept-btn-handler, button:has-text("Accept")')
                if accept:
                    accept.click()
                    page.wait_for_timeout(2000)
            except:
                pass
            
            # Scroll
            for _ in range(5):
                page.mouse.wheel(0, 1500)
                page.wait_for_timeout(1000)
            
            # Get job content
            body = page.inner_text('body')
            
            # Look for job titles
            lines = body.split('\n')
            for line in lines:
                line = line.strip()
                if any(word in line.lower() for word in ['analyst', 'manager', 'engineer', 'associate', 'director', 'specialist', 'developer']):
                    if 15 < len(line) < 100:
                        jobs.append({
                            'title': line,
                            'location': 'India',
                            'detail_url': 'https://careers.blackrock.com/job-search-results/?location=India',
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


def fetch_ubs(max_jobs: int = 100) -> List[dict]:
    """Scrape UBS careers."""
    if not PLAYWRIGHT_AVAILABLE:
        return []
    
    jobs = []
    print("  Scraping ubs.com/careers...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            url = 'https://jobs.ubs.com/TGnewUI/Search/home/HomeWithPreLoad?PageType=JobSearch&partnerid=25008&siteid=5012#keyWordSearch=&locationSearch=India'
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(8000)
            
            # Look for job elements
            job_links = page.query_selector_all('a[href*="Job"], a[href*="job"], .jobTitle')
            
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


def fetch_all_companies(max_jobs_per_company: int = 50) -> Dict[str, List[dict]]:
    """
    Fetch jobs from all working official career sites.
    """
    all_jobs = {}
    
    scrapers = [
        ('Goldman Sachs', fetch_goldman_sachs),
        ('JPMorgan Chase', fetch_jpmorgan_india),
        ('Morgan Stanley', fetch_morgan_stanley),
        ('Barclays', fetch_barclays),
        ('HSBC', fetch_hsbc),
        ('Citi', fetch_citi),
        ('Nomura', fetch_nomura),
        ('Deutsche Bank', fetch_deutsche_bank),
        ('Wells Fargo', fetch_wells_fargo),
        ('BlackRock', fetch_blackrock),
        ('UBS', fetch_ubs),
    ]
    
    print("\n" + "="*60)
    print("SCRAPING ALL OFFICIAL CAREER SITES")
    print("="*60 + "\n")
    
    for company, fetch_func in scrapers:
        print(f"\n[{company}]")
        try:
            jobs = fetch_func(max_jobs=max_jobs_per_company)
            all_jobs[company] = jobs
            print(f"  ✓ Got {len(jobs)} jobs")
        except Exception as e:
            print(f"  ✗ Error: {e}")
            all_jobs[company] = []
        
        time.sleep(2)
    
    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    total = 0
    for company, jobs in sorted(all_jobs.items(), key=lambda x: -len(x[1])):
        if jobs:
            print(f"  {company}: {len(jobs)} jobs")
            total += len(jobs)
    print(f"\nTOTAL: {total} jobs from official sites")
    
    return all_jobs


if __name__ == "__main__":
    results = fetch_all_companies(max_jobs_per_company=30)
