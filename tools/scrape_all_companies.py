# tools/scrape_all_companies.py
"""
Scrape jobs from ALL official company career sites and score them.
Uses Playwright for JavaScript-rendered pages.
"""
import os
import sys
import time

# Set local database mode
os.environ["USE_LOCAL_DB"] = "true"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.local_db import init_db, upsert_company, upsert_jobs, get_stats, update_job_score, get_jobs
from tools.normalize import normalize_job
from tools.scoring import score_job
from connectors.all_official_sites import fetch_all_companies


def score_all_jobs():
    """Score all jobs in the database."""
    print("\n" + "="*70)
    print("  SCORING ALL JOBS")
    print("="*70 + "\n")
    
    # Get all jobs
    jobs = get_jobs(limit=1000, offset=0)
    
    scored = 0
    high_score = 0
    
    for job in jobs:
        try:
            # Build job dict for scoring
            job_for_scoring = {
                'title': job.get('title', ''),
                'description': job.get('description', '') or '',
                'location_city': job.get('location_city', '') or job.get('location', ''),
                'remote': job.get('remote'),
                'min_exp': job.get('min_exp'),
                'max_exp': job.get('max_exp'),
                'posted_at': job.get('posted_at'),
            }
            
            # Score the job
            score, ctc_pass = score_job(job_for_scoring)
            
            # Update score in database
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
    """Main entry point."""
    init_db()
    
    print("\n" + "="*70)
    print("  COMPREHENSIVE JOB SCRAPER - ALL OFFICIAL CAREER SITES")
    print("="*70)
    print("\nScraping from official career portals of major financial institutions.\n")
    
    # Fetch all jobs
    all_jobs = fetch_all_companies(max_jobs_per_company=50)
    
    total_saved = 0
    
    print("\n" + "="*70)
    print("  SAVING TO DATABASE")
    print("="*70 + "\n")
    
    for company_name, jobs in all_jobs.items():
        if not jobs:
            continue
        
        print(f"[{company_name}] Saving {len(jobs)} jobs...")
        
        # Ensure company exists
        company_id = upsert_company(
            name=company_name,
            careers_url=None,
            ats_type="official",
            active=True,
            comp_gate_status="pass"
        )
        
        # Normalize jobs
        normalized = []
        for job in jobs:
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
        try:
            count = upsert_jobs(company_id, normalized)
            total_saved += count
            print(f"  ✓ Saved {count} jobs")
        except Exception as e:
            print(f"  ✗ Error: {e}")
    
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
    print("  TOP SCORING JOBS")
    print("="*70)
    
    # Get top jobs by score
    top_jobs = get_jobs(limit=10, sort_by='relevance_score', sort_order='DESC')
    for job in top_jobs:
        score = job.get('relevance_score') or 0
        title = job.get('title', 'Unknown')[:50]
        company = job.get('company_name', 'Unknown')
        print(f"  [{score:3}] {title} - {company}")
    
    print("\n" + "="*70)
    print("  JOBS BY COMPANY")
    print("="*70)
    
    # Get job counts by company
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
    print(f"  ✅ DONE! Saved {total_saved} jobs, scored {scored} jobs")
    print("="*70)
    print("\n🌐 View jobs at: http://localhost:5001\n")


if __name__ == "__main__":
    main()
