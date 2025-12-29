# tools/score_and_alert.py
"""
Score recent jobs and send Telegram alerts for high-quality matches.
"""
import os
import sys
import datetime as dt
import urllib.parse
import traceback
import requests
from typing import Optional, List

from tools.scoring import score_job
from tools.alert_telegram import send as tg_send


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


def _iso_minutes_ago(mins: int = 180) -> str:
    """Get ISO timestamp for N minutes ago."""
    return (dt.datetime.utcnow() - dt.timedelta(minutes=mins)).isoformat(timespec="seconds") + "Z"


def fetch_recent_jobs(since_minutes: int = 180) -> List[dict]:
    """Fetch jobs created in the last N minutes."""
    if not SUPABASE_URL or not SERVICE_ROLE:
        print("ERROR: SUPABASE_URL or SUPABASE_SERVICE_ROLE not set")
        return []
    
    since = _iso_minutes_ago(since_minutes)
    params = {
        "select": "id,title,apply_url,location_city,remote,posted_at,description,min_exp,max_exp,company_id,companies(name,comp_gate_status),first_seen_at",
        "order": "first_seen_at.desc",
        "first_seen_at": f"gte.{since}",
    }
    url = f"{SUPABASE_URL}/rest/v1/jobs?{urllib.parse.urlencode(params)}"
    
    try:
        r = requests.get(url, headers=_get_headers(), timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Error fetching recent jobs: {e}")
        return []


def patch_job_eval(job_id: int, score: int, ctc_pass: bool) -> bool:
    """Update job with relevance score and CTC prediction."""
    if not SUPABASE_URL or not SERVICE_ROLE:
        return False
    
    url = f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}"
    try:
        r = requests.patch(
            url,
            headers=_get_headers(),
            json={"ctc_predicted_pass": ctc_pass, "relevance_score": score}
        )
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"Patch failed for job {job_id}: {e}")
        return False


def _should_skip_by_title(title: str) -> bool:
    """Check if job should be skipped based on title."""
    title_l = (title or "").lower()
    skip_keywords = ["intern", "trainee", "manager", "lead", "vp", "director", "principal", "head"]
    return any(x in title_l for x in skip_keywords)


def _experience_matches(min_exp: Optional[int], max_exp: Optional[int]) -> bool:
    """
    Check if experience range overlaps with 1-3 years.
    
    Returns True if:
    - No experience info (give benefit of doubt)
    - Experience range overlaps with 1-3 years
    """
    # No experience info: allow
    if min_exp is None and max_exp is None:
        return True
    
    # FIX: Properly handle experience range
    lo = 0 if min_exp is None else min_exp
    hi = 99 if max_exp is None else max_exp
    
    # Check overlap with 1-3 years range
    # Job range [lo, hi] must overlap with target range [1, 3]
    # Overlap exists if: lo <= 3 AND hi >= 1
    return lo <= 3 and hi >= 1


def main():
    """Main entry point for scoring and alerting."""
    if not SUPABASE_URL or not SERVICE_ROLE:
        print("ERROR: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE environment variables")
        sys.exit(1)
    
    jobs = fetch_recent_jobs(180)
    print(f"Found {len(jobs)} recent jobs to process")
    
    sent = 0
    scored = 0
    
    for j in jobs:
        try:
            score, ctc_pass = score_job(j)
            scored += 1

            # Write score + CTC flag for visibility
            patch_job_eval(j["id"], score, ctc_pass)

            # Apply filters
            if _should_skip_by_title(j.get("title")):
                continue
            
            if not _experience_matches(j.get("min_exp"), j.get("max_exp")):
                continue

            # Send alert for high-quality matches
            if score >= 80 and ctc_pass:
                company_name = (j.get("companies") or {}).get("name", "Company")
                loc = j.get("location_city") or "Location N/A"
                url = j.get("apply_url") or ""
                title = j.get("title") or "Job"
                
                msg = (
                    f"<b>{company_name}</b> — <b>{title}</b>\n"
                    f"{loc}\n"
                    f"Score: <b>{score}</b>\n"
                    f"{url}"
                )
                
                try:
                    tg_send(msg)
                    sent += 1
                except Exception as e:
                    print(f"Telegram send failed: {e}")
                    
        except Exception as e:
            print(f"Error processing job {j.get('id')}: {e}")
            traceback.print_exc()

    print(f"Score+Alert done. Scored={scored}, Sent={sent}")


if __name__ == "__main__":
    main()
