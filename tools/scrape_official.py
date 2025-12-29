# tools/scrape_official.py
"""
Scrape jobs from official company career sites only.
Uses Playwright for JavaScript-rendered pages.
"""
import os
import sys
import time

# Set local database mode
os.environ["USE_LOCAL_DB"] = "true"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.local_db import init_db, upsert_company, upsert_jobs, get_stats
from tools.normalize import normalize_job
from connectors.official_sites import (
    fetch_goldman_sachs,
    fetch_barclays,
)
from connectors.jpmorgan_official import fetch_jpmorgan_india


def main():
    """Main entry point."""
    init_db()
    
    print("\n" + "="*60)
    print("OFFICIAL CAREER SITE SCRAPER")
    print("="*60)
    print("\nThis scrapes directly from official company career pages.\n")
    
    # Define companies and their scrapers
    sources = [
        ("Goldman Sachs", fetch_goldman_sachs, 100),
        ("Barclays", fetch_barclays, 100),
        ("JPMorgan Chase", fetch_jpmorgan_india, 50),
    ]
    
    total_jobs = 0
    
    for company_name, fetch_func, max_jobs in sources:
        print(f"\n{'='*40}")
        print(f"Company: {company_name}")
        print('='*40)
        
        # Ensure company exists
        company_id = upsert_company(
            name=company_name,
            careers_url=None,
            ats_type="official",
            active=True,
            comp_gate_status="pass"
        )
        
        try:
            # Fetch jobs from official site
            raw_jobs = fetch_func(max_jobs=max_jobs)
            
            if not raw_jobs:
                print(f"  No jobs found")
                continue
            
            # Normalize jobs
            normalized = []
            for job in raw_jobs:
                _, rec = normalize_job(
                    company_id=company_id,
                    title=job.get("title"),
                    apply_url=job.get("detail_url"),
                    location=job.get("location"),
                    description=job.get("description"),
                    req_id=job.get("req_id"),
                    posted_at=job.get("posted"),
                )
                normalized.append(rec)
            
            # Upsert to database
            count = upsert_jobs(company_id, normalized)
            total_jobs += count
            print(f"  ✓ Saved {count} jobs to database")
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            import traceback
            traceback.print_exc()
        
        time.sleep(2)  # Be nice to servers
    
    print("\n" + "="*60)
    print(f"TOTAL: {total_jobs} jobs from official career sites")
    print("="*60)
    
    # Show stats
    stats = get_stats()
    print(f"\nDatabase now has:")
    print(f"  - {stats['job_count']} total jobs")
    print(f"  - {stats['company_count']} companies")
    print(f"  - {stats['high_score_count']} high-score jobs (80+)")
    
    print("\n" + "="*60)
    print("Jobs by company:")
    for company in stats['top_companies']:
        if company['job_count'] > 0:
            print(f"  - {company['name']}: {company['job_count']} jobs")


if __name__ == "__main__":
    main()
