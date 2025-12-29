# tools/scrape_final.py
"""
FINAL COMPREHENSIVE SCRAPER:
- Only scrapes RECENT jobs (last 30 days)
- Uses only WORKING official career sites
- Includes job scoring and ranking
"""
import os
import sys
import time
from datetime import datetime, timedelta

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
    """Check if job is recent (posted within last N days)."""
    posted = job.get('posted') or job.get('posted_at')
    
    if not posted:
        return True  # Assume recent if no date
    
    try:
        posted_str = str(posted)
        if '-' in posted_str:
            posted_date = datetime.fromisoformat(posted_str[:10])
            cutoff = datetime.now() - timedelta(days=days_threshold)
            return posted_date >= cutoff
    except (ValueError, AttributeError):
        pass
    
    return True


def score_all_jobs():
    """Score all jobs in database."""
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
            
            if score >= 60:
                high_score += 1
        except Exception as e:
            print(f"  Error scoring job {job.get('id')}: {e}")
    
    print(f"  ✓ Scored {scored} jobs")
    print(f"  ⭐ Jobs with score ≥ 60: {high_score}")
    
    return scored, high_score


def main():
    """Main scraper - only recent jobs from working sites."""
    init_db()
    
    print("\n" + "="*70)
    print("  FINAL JOB SCRAPER - RECENT JOBS ONLY (LAST 30 DAYS)")
    print("="*70)
    print("\n✅ Only scraping jobs from WORKING official career sites")
    print("🗓️  Filtering for jobs posted in the last 30 days")
    print()
    
    # Only use WORKING scrapers
    working_scrapers = [
        ('Goldman Sachs', fetch_goldman_sachs, 50,  'higher.gs.com'),
        ('JPMorgan Chase', fetch_jpmorgan_india, 50, 'jpmc.fa.oraclecloud.com'),
        ('Morgan Stanley', fetch_morgan_stanley, 50, 'morganstanley.eightfold.ai'),
        ('Barclays', fetch_barclays, 50, 'search.jobs.barclays'),
        ('Citi', fetch_citi, 50, 'jobs.citi.com'),
        ('Nomura', fetch_nomura, 50, 'careers.nomura.com'),
    ]
    
    total_saved = 0
    total_filtered_old = 0
    
    for company_name, fetch_func, max_jobs, source_url in working_scrapers:
        print(f"\n{'='*50}")
        print(f"[{company_name}]")
        print(f"Source: {source_url}")
        print('='*50)
        
        company_id = upsert_company(
            name=company_name,
            careers_url=source_url,
            ats_type="official",
            active=True,
            comp_gate_status="pass"
        )
        
        try:
            # Fetch jobs
            all_jobs = fetch_func(max_jobs=max_jobs)
            
            if not all_jobs:
                print(f"  ⚠️  No jobs found")
                continue
            
            print(f"  📥 Fetched: {len(all_jobs)} jobs from {source_url}")
            
            # Filter for recent jobs
            recent_jobs = [j for j in all_jobs if is_recent_job(j, days_threshold=30)]
            filtered_count = len(all_jobs) - len(recent_jobs)
            total_filtered_old += filtered_count
            
            if filtered_count > 0:
                print(f"  🗑️  Filtered out: {filtered_count} old jobs (>30 days)")
            
            if not recent_jobs:
                print(f"  ⚠️  No recent jobs (all older than 30 days)")
                continue
            
            # Normalize
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
            
            # Save to database
            count = upsert_jobs(company_id, normalized)
            total_saved += count
            print(f"  ✅ Saved: {count} recent jobs to database")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback
            traceback.print_exc()
        
        time.sleep(2)  # Be nice to servers
    
    # Score all jobs
    print()
    scored, high_score = score_all_jobs()
    
    # Final results
    print("\n" + "="*70)
    print("  ✨ FINAL RESULTS")
    print("="*70)
    
    stats = get_stats()
    print(f"\n📊 Database Statistics:")
    print(f"  • Total Jobs: {stats['job_count']}")
    print(f"  • Companies: {stats['company_count']}")
    print(f"  • High-Score Jobs (80+): {stats['high_score_count']}")
    print(f"  • Filtered Old Jobs: {total_filtered_old}")
    
    print("\n" + "="*70)
    print("  🏆 TOP 20 JOBS BY RELEVANCE SCORE")
    print("="*70)
    print(f"\n{'Rank':<5} {'Score':<6} {'Title':<50} {'Company':<22} {'Location':<20}")
    print("-" * 103)
    
    top_jobs = get_jobs(limit=20, sort_by='relevance_score', sort_order='DESC')
    for i, job in enumerate(top_jobs, 1):
        score = job.get('relevance_score') or 0
        title = job.get('title', 'Unknown')[:48]
        company = job.get('company_name', 'Unknown')[:20]
        location = (job.get('location_city', '') or 'India')[:18]
        print(f"{i:<5} {score:<6} {title:<50} {company:<22} {location:<20}")
    
    print("\n" + "="*70)
    print("  📈 JOBS BY COMPANY")
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
    
    print(f"\n{'Company':<30} {'Jobs':<10}")
    print("-" * 40)
    for row in cursor.fetchall():
        print(f"{row[0]:<30} {row[1]:<10}")
    
    conn.close()
    
    print("\n" + "="*70)
    print(f"  ✅ COMPLETE! Saved {total_saved} recent jobs")
    print(f"  🎯 Scored {scored} jobs")
    print(f"  🗑️  Filtered {total_filtered_old} old jobs")
    print("="*70)
    print("\n🌐 View all jobs at: http://localhost:5001")
    print("💡 Jobs are sorted by relevance score (0-100)")
    print()


if __name__ == "__main__":
    main()
