# tools/scheduler.py
"""
Background scheduler for daily job scraping and matching.
Runs alongside Flask app to automate job discovery.
"""
import os
import sys
import time
import schedule
import threading
from datetime import datetime, timedelta
import subprocess

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.local_db import get_connection, get_jobs
from tools.ollama_client import match_job_to_cv, test_connection
import json


def get_cv_data():
    """Get stored CV data from database."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT extracted_skills, years_experience, domain_expertise, 
                   preferred_roles, education, location_preference 
            FROM user_cv 
            ORDER BY id DESC LIMIT 1
        """)
        row = cursor.fetchone()
        
        if not row:
            return None
        
        return {
            'technical_skills': json.loads(row[0]) if row[0] else [],
            'years_experience': row[1] or 0,
            'domain_expertise': json.loads(row[2]) if row[2] else [],
            'preferred_roles': json.loads(row[3]) if row[3] else [],
            'education': row[4] or '',
            'location_preference': json.loads(row[5]) if row[5] else []
        }


def get_jobs_needing_matching():
    """Get jobs that haven't been matched yet or are new."""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Get jobs with no AI score or updated recently
        cursor.execute("""
            SELECT id, title, description, location_city
            FROM jobs
            WHERE ai_match_score IS NULL OR ai_match_score = 0
            ORDER BY first_seen_at DESC
            LIMIT 200
        """)
        
        jobs = []
        for row in cursor.fetchall():
            jobs.append({
                'id': row[0],
                'title': row[1],
                'description': row[2],
                'location_city': row[3]
            })
        
        return jobs


def match_new_jobs():
    """Match newly scraped jobs with stored CV."""
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting job matching...")
    
    # Check if Ollama is running
    if not test_connection():
        print("  ⚠️  Ollama not running - skipping matching")
        return
    
    # Get CV data
    cv_data = get_cv_data()
    if not cv_data:
        print("  ⚠️  No CV uploaded - skipping matching")
        return
    
    # Get jobs needing matching
    jobs = get_jobs_needing_matching()
    if not jobs:
        print("  ✓ No new jobs to match")
        return
    
    print(f"  Matching {len(jobs)} jobs with CV...")
    
    matched = 0
    high_matches = 0
    
    for i, job in enumerate(jobs, 1):
        try:
            # Match job with CV
            score, reasoning = match_job_to_cv(
                cv_summary=cv_data,
                job_title=job['title'][:100],
                job_description=(job['description'] or '')[:1000],
                job_location=job['location_city'] or ''
            )
            
            # Update database
            with get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE jobs 
                    SET ai_match_score = ?, match_reasoning = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (score, reasoning, job['id']))
                conn.commit()
            
            matched += 1
            if score >= 70:
                high_matches += 1
                print(f"    🎯 [{score}] {job['title'][:50]}")
            
            # Small delay to avoid overwhelming Ollama
            if i % 10 == 0:
                print(f"    Progress: {i}/{len(jobs)} matched...")
                time.sleep(1)
            else:
                time.sleep(0.3)
                
        except Exception as e:
            print(f"    ❌ Error matching job {job['id']}: {e}")
    
    print(f"  ✓ Matched {matched} jobs ({high_matches} high matches)")


def daily_scrape_and_match():
    """Daily job: Scrape new jobs and auto-match with CV."""
    print("\n" + "="*70)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] DAILY JOB STARTED")
    print("="*70)
    
    # Step 1: Scrape jobs
    print("\n[1/2] Scraping jobs from official sites...")
    try:
        result = subprocess.run(
            [sys.executable, "tools/scrape_final.py"],
            capture_output=True,
            text=True,
            timeout=900,  # 15 minutes max
            cwd=os.path.dirname(os.path.dirname(__file__)),
            env={**os.environ, "USE_LOCAL_DB": "true"}
        )
        
        if result.returncode == 0:
            print("  ✓ Scraping complete")
            # Parse output to see how many new jobs
            output = result.stdout
            if "Saved" in output:
                print(f"  {output.split('Saved')[1].split('jobs')[0].strip()} new jobs found")
        else:
            print(f"  ⚠️  Scraping had issues: {result.stderr[:200]}")
    except subprocess.TimeoutExpired:
        print("  ⚠️  Scraping timed out (took >15 min)")
    except Exception as e:
        print(f"  ❌ Scraping error: {e}")
    
    # Step 2: Match new jobs
    print("\n[2/2] Matching new jobs with CV...")
    try:
        match_new_jobs()
    except Exception as e:
        print(f"  ❌ Matching error: {e}")
    
    print("\n" + "="*70)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] DAILY JOB COMPLETE")
    print("="*70 + "\n")


def run_scheduler():
    """Run the scheduler in a separate thread."""
    print("\n" + "="*70)
    print("  JOB SCHEDULER STARTED")
    print("="*70)
    print(f"\n  Daily scraping scheduled for: 2:00 AM")
    print(f"  Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Next run: Tomorrow at 2:00 AM")
    print("\n  The scheduler is running in the background...")
    print("="*70 + "\n")
    
    # Schedule daily job at 2 AM
    schedule.every().day.at("02:00").do(daily_scrape_and_match)
    
    # For testing: also run every hour (comment out in production)
    # schedule.every().hour.do(daily_scrape_and_match)
    
    # Keep running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


def start_scheduler():
    """Start scheduler in background thread."""
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    return scheduler_thread


def run_now():
    """Run the daily job immediately (for testing)."""
    print("Running daily job NOW (test mode)...")
    daily_scrape_and_match()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "now":
        # Run immediately for testing
        run_now()
    else:
        # Run scheduler
        run_scheduler()
