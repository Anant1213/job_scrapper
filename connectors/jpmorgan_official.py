# connectors/jpmorgan_official.py
"""
JPMorgan official career site scraper.
Scrapes from jpmc.fa.oraclecloud.com (Oracle HCM).
"""
from typing import List, Optional
import re
import time

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


def fetch_jpmorgan_india(max_jobs: int = 100) -> List[dict]:
    """
    Scrape jobs from JPMorgan official career site.
    URL: https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/jobs
    
    Returns:
        List of job dicts with India-based positions
    """
    if not PLAYWRIGHT_AVAILABLE:
        print("    Playwright not available")
        return []
    
    jobs = []
    print("  Scraping jpmc.fa.oraclecloud.com (official JPMorgan careers)...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
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
            
            # Find and fill location input with "India"
            location_input = page.query_selector('input[placeholder*="City"]')
            if location_input:
                location_input.click()
                page.wait_for_timeout(500)
                location_input.fill('India')
                page.wait_for_timeout(2000)
                page.keyboard.press('Enter')
                page.wait_for_timeout(5000)
            
            # Scroll to load more jobs
            for _ in range(5):
                page.mouse.wheel(0, 1000)
                page.wait_for_timeout(1000)
            
            # Get page text
            body = page.inner_text('body')
            
            # Find job count
            count_match = re.search(r'(\d+)\s*(?:Open Jobs|OPEN JOBS)', body)
            if count_match:
                print(f"    Found {count_match.group(1)} total India jobs on JPMorgan")
            
            # Parse job listings from the text
            # Jobs appear in pattern: Title\nLocation\nCategory\nDescription
            lines = [l.strip() for l in body.split('\n') if l.strip()]
            
            india_cities = ['India', 'Bengaluru', 'Bangalore', 'Mumbai', 'Hyderabad', 'Pune', 'Chennai', 'Gurgaon', 'Gurugram', 'Noida', 'Delhi']
            
            i = 0
            while i < len(lines) - 1 and len(jobs) < max_jobs:
                line = lines[i]
                
                # Skip navigation/UI text
                if any(skip in line.lower() for skip in ['skip to', 'search jobs', 'find jobs', 'filter', 'posting date', 'categories', 'open jobs', 'near location', 'city, state']):
                    i += 1
                    continue
                
                # Check if this looks like a job title (reasonable length, not just location)
                if 15 < len(line) < 150 and not any(city in line for city in india_cities):
                    # Check if next line is an India location
                    if i + 1 < len(lines):
                        next_line = lines[i + 1]
                        if any(city in next_line for city in india_cities):
                            title = line
                            location = next_line
                            
                            # Get description if available (usually 2-3 lines after)
                            description = ''
                            if i + 3 < len(lines):
                                desc_line = lines[i + 3]
                                if len(desc_line) > 50 and not any(city in desc_line for city in india_cities):
                                    description = desc_line[:500]
                            
                            jobs.append({
                                'title': title,
                                'location': location,
                                'detail_url': 'https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/jobs',
                                'description': description,
                                'posted': None,
                                'req_id': None,
                                'source': 'JPMorgan Official'
                            })
                            i += 3
                            continue
                
                i += 1
            
            print(f"    Extracted {len(jobs)} jobs from page")
            
        except Exception as e:
            print(f"    Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            browser.close()
    
    return jobs


if __name__ == "__main__":
    jobs = fetch_jpmorgan_india(max_jobs=50)
    print(f"\nFound {len(jobs)} JPMorgan India jobs:")
    for job in jobs[:10]:
        print(f"  - {job['title']}")
        print(f"    📍 {job['location']}")
