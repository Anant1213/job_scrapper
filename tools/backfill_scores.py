# tools/backfill_scores.py
"""
Backfill relevance scores for all jobs in the database.
One-time script to update existing jobs with scores.
"""
import os
import sys
import time
import traceback
import urllib.parse
import requests
from typing import List

from tools.scoring import score_job


def _get_env_var(name: str) -> str:
    """Get required environment variable."""
    value = os.environ.get(name)
    if not value:
        print(f"ERROR: Missing required environment variable: {name}")
        sys.exit(1)
    return value.rstrip("/") if name == "SUPABASE_URL" else value


SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SERVICE_ROLE = os.environ.get("SUPABASE_SERVICE_ROLE", "")


def _get_headers() -> dict:
    """Get Supabase API headers."""
    return {
        "apikey": SERVICE_ROLE,
        "Authorization": f"Bearer {SERVICE_ROLE}",
        "Accept-Profile": "public",
        "Content-Type": "application/json",
    }


SELECT = "id,title,apply_url,location_city,remote,posted_at,description,min_exp,max_exp,company_id,companies(name,comp_gate_status)"


def fetch_page(offset: int = 0, limit: int = 500) -> List[dict]:
    """Fetch a page of jobs from the database."""
    params = {
        "select": SELECT,
        "order": "id.asc",
        "limit": str(limit),
        "offset": str(offset)
    }
    url = f"{SUPABASE_URL}/rest/v1/jobs?{urllib.parse.urlencode(params)}"
    
    try:
        r = requests.get(url, headers=_get_headers(), timeout=40)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Error fetching page at offset {offset}: {e}")
        return []


def patch_job_eval(job_id: int, score: int, ctc_pass: bool) -> bool:
    """Update job with relevance score and CTC prediction."""
    url = f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}"
    
    try:
        r = requests.patch(
            url,
            headers=_get_headers(),
            json={"relevance_score": score, "ctc_predicted_pass": ctc_pass}
        )
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"  Patch failed for job {job_id}: {e}")
        return False


def main():
    """Main entry point for backfill."""
    if not SUPABASE_URL or not SERVICE_ROLE:
        print("ERROR: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE environment variables")
        sys.exit(1)
    
    print("Starting backfill of relevance scores...")
    
    offset = 0
    total = 0
    errors = 0
    batch_size = 500
    
    while True:
        rows = fetch_page(offset, batch_size)
        if not rows:
            break
        
        for j in rows:
            try:
                score, ctc_pass = score_job(j)
                if patch_job_eval(j["id"], score, ctc_pass):
                    total += 1
                else:
                    errors += 1
            except Exception as e:
                print(f"  Error scoring job {j.get('id')}: {e}")
                traceback.print_exc()
                errors += 1
        
        print(f"  Backfilled {total} jobs (errors: {errors})...")
        offset += len(rows)
        time.sleep(0.3)
    
    print(f"Backfill complete. Total: {total}, Errors: {errors}")


if __name__ == "__main__":
    main()
