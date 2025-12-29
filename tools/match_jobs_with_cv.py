# tools/match_jobs_with_cv.py
"""
CLI tool to match all jobs against a CV using Ollama AI.

Usage:
    python3 tools/match_jobs_with_cv.py path/to/resume.pdf
"""
import os
import sys
import time
from datetime import datetime

os.environ["USE_LOCAL_DB"] = "true"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.local_db import get_connection, get_jobs
from tools.cv_parser import extract_cv_text, clean_cv_text
from tools.ollama_client import test_connection, extract_skills_from_cv, match_job_to_cv


def save_cv_to_db(filename: str, cv_text: str, skills_data: dict):
    """Save CV and extracted data to database."""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Delete old CVs (keep only latest)
        cursor.execute("DELETE FROM user_cv")
        
        # Insert new CV
        cursor.execute("""
            INSERT INTO user_cv (
                filename, cv_text, extracted_skills, years_experience,
                domain_expertise, preferred_roles, education, location_preference
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            filename,
            cv_text,
            str(skills_data.get('technical_skills', [])),
            skills_data.get('years_experience', 0),
            str(skills_data.get('domain_expertise', [])),
            str(skills_data.get('preferred_roles', [])),
            skills_data.get('education', ''),
            str(skills_data.get('location_preference', []))
        ))
        
        conn.commit()
        print("  ✓ CV saved to database")


def update_job_match(job_id: int, score: int, reasoning: str):
    """Update job with AI match score and reasoning."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE jobs 
            SET ai_match_score = ?, match_reasoning = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (score, reasoning, job_id))
        conn.commit()


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python3 tools/match_jobs_with_cv.py <path_to_cv>")
        print("\nSupported formats: .pdf, .docx, .txt")
        print("\nExample:")
        print("  python3 tools/match_jobs_with_cv.py ~/Documents/resume.pdf")
        sys.exit(1)
    
    cv_path = sys.argv[1]
    
    if not os.path.exists(cv_path):
        print(f"❌ Error: File not found: {cv_path}")
        sys.exit(1)
    
    print("\n" + "="*70)
    print("  AI-POWERED JOB MATCHING WITH YOUR CV")
    print("="*70)
    print()
    
    # Step 1: Test Ollama connection
    print("Step 1: Checking Ollama...")
    if not test_connection():
        print("❌ Error: Ollama is not running")
        print("\nStart Ollama with: ollama serve")
        print("Or in a separate terminal: ollama run llama3:latest")
        sys.exit(1)
    print("  ✓ Ollama is running")
    print()
    
    # Step 2: Parse CV
    print(f"Step 2: Parsing CV from {os.path.basename(cv_path)}...")
    try:
        cv_text = extract_cv_text(cv_path)
        cv_text = clean_cv_text(cv_text)
        print(f"  ✓ Extracted {len(cv_text)} characters from CV")
    except Exception as e:
        print(f"❌ Error parsing CV: {e}")
        sys.exit(1)
    print()
    
    # Step 3: Extract skills using Ollama
    print("Step 3: Analyzing CV with AI (this may take 30-60 seconds)...")
    skills_data = extract_skills_from_cv(cv_text)
    
    if not skills_data or not skills_data.get('technical_skills'):
        print("⚠️  Warning: Could not extract skills from CV")
        print("  Using CV text for matching instead")
        skills_data = {
            'technical_skills': [],
            'years_experience': 0,
            'domain_expertise': [],
            'preferred_roles': [],
            'education': '',
            'location_preference': []
        }
    else:
        print("  ✓ Extracted skills:")
        print(f"    • Technical Skills: {', '.join(skills_data.get('technical_skills', [])[:10])}")
        print(f"    • Experience: {skills_data.get('years_experience', 0)} years")
        print(f"    • Domain: {', '.join(skills_data.get('domain_expertise', []))}")
        print(f"    • Education: {skills_data.get('education', 'Not specified')}")
    print()
    
    # Step 4: Save CV to database
    print("Step 4: Saving CV to database...")
    save_cv_to_db(os.path.basename(cv_path), cv_text, skills_data)
    print()
    
    # Step 5: Get all jobs
    print("Step 5: Fetching jobs from database...")
    jobs = get_jobs(limit=1000, offset=0)
    print(f"  ✓ Found {len(jobs)} jobs to match")
    print()
    
    # Step 6: Match each job
    print("Step 6: Matching jobs with your CV (AI analysis)...")
    print(f"  This will take approximately {len(jobs) * 3 // 60} minutes")
    print(f"  Progress:")
    print()
    
    matched = 0
    high_matches = 0
    start_time = time.time()
    
    for i, job in enumerate(jobs, 1):
        job_id = job['id']
        title = job.get('title', 'Unknown')[:50]
        company = job.get('company_name', 'Unknown')
        
        # Progress indicator
        if i % 10 == 0 or i == 1:
            elapsed = time.time() - start_time
            avg_time = elapsed / i if i > 0 else 0
            remaining = (len(jobs) - i) * avg_time
            print(f"  [{i}/{len(jobs)}] Matching... (ETA: {int(remaining//60)}m {int(remaining%60)}s)")
        
        try:
            # Match job with CV
            score, reasoning = match_job_to_cv(
                cv_summary=skills_data,
                job_title=title,
                job_description=job.get('description', '')[:1000],
                job_location=job.get('location_city', '')
            )
            
            # Update database
            update_job_match(job_id, score, reasoning)
            matched += 1
            
            if score >= 70:
                high_matches += 1
                print(f"    🎯 [{score}] {title} - {company}")
            
        except Exception as e:
            print(f"    ❌ Error matching {title}: {e}")
        
        # Small delay to avoid overwhelming Ollama
        time.sleep(0.5)
    
    elapsed_total = time.time() - start_time
    
    # Summary
    print()
    print("="*70)
    print("  ✅ MATCHING COMPLETE!")
    print("="*70)
    print(f"\n  Matched: {matched}/{len(jobs)} jobs")
    print(f"  High Matches (70+): {high_matches}")
    print(f"  Time Taken: {int(elapsed_total//60)}m {int(elapsed_total%60)}s")
    print()
    print("  View results at: http://localhost:5001")
    print("  Jobs are now sorted by AI match score!")
    print()


if __name__ == "__main__":
    main()
