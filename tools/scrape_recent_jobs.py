# tools/scrape_recent_jobs.py
"""
Scrape ONLY RECENT jobs (last 30 days) from all official company career sites.
This ensures we don't pull old job postings.
"""
import os
import sys
import time
from datetime import datetime, timedelta

# Set local database mode
os.environ["USE_LOCAL_DB"] = "true"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.local_db import init_db, upsert_company, upsert_jobs, get_stats, update_job_score, get_jobs
from tools.normalize import normalize_job
from tools.scoring import score_job
from connectors.all_official_sites import (
    fetch_goldman_sachs,
    fetch_barclays,
    fetch_jpmorgan_india,
    fetch_morgan_stanley,
    fetch_citi,
    fetch_nomura,
)


def is_recent_job(job: dict, days_threshold: int = 30) -> bool:
    """
    Check if a job is recent (posted within last N days).
    
    Args:
        job: Job dict with optional 'posted' or 'posted_at' field
        days_threshold: Number of days to consider "recent"
        
    Returns:
        True if job is recent or date is unknown (assume recent)
    """
    posted = job.get('posted') or job.get('posted_at')
    
    if not posted:
        # If no date, assume it's recent (better to include than exclude)
        return True
    
    try:
        # Try parsing different date formats
        posted_str = str(posted)
        
        # ISO format: 2024-12-28
        if '-' in posted_str:
            posted_date = datetime.fromisoformat(posted_str[:10])
        else:
            # Unknown format, assume recent
            return True
        
        # Check if within threshold
        cutoff = datetime.now() - timedelta(days=days_threshold)
        return posted_date >= cutoff
        
    except (ValueError, AttributeError):
        # If parsing fails, assume recent
        return True


def filter_recent_jobs(jobs: list, days: int = 30) -> list:
    """Filter jobs to only include recent postings."""
    recent = [j for j in jobs if is_recent_job(j, days)]
    
    if len(recent) < len(jobs):
        print(f"    Filtered: {len(jobs)} → {len(recent)} recent jobs (last {days} days)")
    
    return recent


def score_all_jobs():
    """Score all jobs in the database."""
    print("\n" + "="*70)
    print("  SCORING ALL JOBS")
    print("="*70 + "\n")
    
    jobs = get_jobs(limit=1000, offset=0)
    
    scored = 0
    high_score = 0
    
    for job in jobs:
        try:
            job_for_scoring = {
                'title': job.get('title', ''),
                'description': job.get('description', '') or '',
                'location_city': job.get('location_city', '') or '',
                'remote': job.get('remote'),
                'min_exp': job.get('min_exp'),
                'max_exp': job.get('max_exp'),
                'posted_at': job.get('posted_at'),
            }
            
            score, ctc_pass = score_job(job_for_scoring)
            update_job_score(job['id'], score, ctc_pass)
            scored += 1
            
            if score >= 80:
                high_score += 1
            
        except Exception as e:
            print(f"  Error scoring job {job.get('id')}: {e}")
    
    print(f"  ✓ Scored {scored} jobs")
    print(f"  ⭐ High-score jobs (80+): {high_score}")
    
    return scored, high_score


def main():
    """Main entry point - scrape only recent jobs."""
    init_db()
    
    print("\n" + "="*70)
    print("  SCRAPING RECENT JOBS ONLY (Last 30 Days)")
    print("="*70)
    print("\nOnly scraping jobs posted in the last 30 days.\n")
    
    # Define working scrapers
    scrapers = [
        ('Goldman Sachs', fetch_goldman_sachs, 50),
        ('JPMorgan Chase', fetch_jpmorgan_india, 50),
        ('Morgan Stanley', fetch_morgan_stanley, 50),
        ('Barclays', fetch_barclays, 50),
        ('Citi', fetch_citi, 50),
        ('Nomura', fetch_nomura, 50),
    ]
    
    total_saved = 0
    total_filtered = 0
    
    for company_name, fetch_func, max_jobs in scrapers:
        print(f"\n[{company_name}]")
        
        # Ensure company exists
        company_id = upsert_company(
            name=company_name,
            careers_url=None,
            ats_type="official",
            active=True,
            comp_gate_status="pass"
        )
        
        try:
            # Fetch jobs
            all_jobs = fetch_func(max_jobs=max_jobs)
            
            if not all_jobs:
                print(f"  No jobs found")
                continue
            
            # Filter for recent jobs only
            recent_jobs = filter_recent_jobs(all_jobs, days=30)
            total_filtered += (len(all_jobs) - len(recent_jobs))
            
            if not recent_jobs:
                print(f"  No recent jobs (all are older than 30 days)")
                continue
            
            # Normalize jobs
            normalized = []
            for job in recent_jobs:
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
            total_saved += count
            print(f"  ✓ Saved {count} recent jobs")
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            import traceback
            traceback.print_exc()
        
        time.sleep(2)
    
    print(f"\n  Filtered out {total_filtered} old jobs (older than 30 days)")
    
    # Score all jobs
    scored, high_score = score_all_jobs()
    
    # Final stats
    print("\n" + "="*70)
    print("  FINAL RESULTS")
    print("="*70)
    
    stats = get_stats()
    print(f"\nDatabase now has:")
    print(f"  📊 Total Jobs: {stats['job_count']}")
    print(f"  🏢 Companies: {stats['company_count']}")
    print(f"  ⭐ High-Score Jobs (80+): {stats['high_score_count']}")
    
    print("\n" + "="*70)
    print("  TOP 15 SCORING JOBS")
    print("="*70)
    
    top_jobs = get_jobs(limit=15, sort_by='relevance_score', sort_order='DESC')
    for i, job in enumerate(top_jobs, 1):
        score = job.get('relevance_score') or 0
        title = job.get('title', 'Unknown')[:45]
        company = job.get('company_name', 'Unknown')[:20]
        location = (job.get('location_city', '') or 'India')[:20]
        print(f"  {i:2}. [{score:3}] {title:45} | {company:20} | {location}")
    
    print("\n" + "="*70)
    print("  JOBS BY COMPANY")
    print("="*70)
    
    import sqlite3
    conn = sqlite3.connect('jobs.db')
    cursor = conn.cursor()
    cursor.execute('''
        SELECT c.name, COUNT(j.id) as count 
        FROM companies c 
        LEFT JOIN jobs j ON c.id = j.company_id 
        GROUP BY c.id 
        HAVING count > 0
        ORDER BY count DESC
    ''')
    
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} jobs")
    
    conn.close()
    
    print("\n" + "="*70)
    print(f"  ✅ DONE! Saved {total_saved} recent jobs, scored {scored} jobs")
    print("="*70)
    print("\n🌐 View jobs at: http://localhost:5001\n")


if __name__ == "__main__":
    main()
