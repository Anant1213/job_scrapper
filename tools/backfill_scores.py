# tools/backfill_scores.py
import os, urllib.parse, requests, time
from tools.scoring import score_job

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_ROLE = os.environ["SUPABASE_SERVICE_ROLE"]
HDRS = {
    "apikey": SERVICE_ROLE,
    "Authorization": f"Bearer {SERVICE_ROLE}",
    "Accept-Profile": "public",
    "Content-Type": "application/json",
}

SELECT = "id,title,apply_url,location_city,remote,posted_at,description,min_exp,max_exp,company_id,companies(name,comp_gate_status)"

def fetch_page(offset=0, limit=500):
    params = {
        "select": SELECT,
        "order": "id.asc",
        "limit": str(limit),
        "offset": str(offset)
    }
    url = f"{SUPABASE_URL}/rest/v1/jobs?{urllib.parse.urlencode(params)}"
    r = requests.get(url, headers=HDRS, timeout=40)
    r.raise_for_status()
    return r.json()

def patch_job_eval(job_id: int, score: int, ctc_pass: bool):
    url = f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}"
    r = requests.patch(url, headers=HDRS, json={"relevance_score": score, "ctc_predicted_pass": ctc_pass})
    r.raise_for_status()

def main():
    offset, total = 0, 0
    while True:
        rows = fetch_page(offset, 500)
        if not rows: break
        for j in rows:
            score, ctc_pass = score_job(j)
            try:
                patch_job_eval(j["id"], score, ctc_pass)
            except Exception as e:
                print("patch failed:", e)
        total += len(rows)
        offset += len(rows)
        print(f"Backfilled {total} jobs...")
        time.sleep(0.3)
    print("Backfill complete.")

if __name__ == "__main__":
    main()
