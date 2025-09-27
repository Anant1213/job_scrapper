# tools/score_and_alert.py
import os, datetime as dt, urllib.parse, requests
from tools.scoring import score_job
from tools.alert_telegram import send as tg_send

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_ROLE = os.environ["SUPABASE_SERVICE_ROLE"]
HDRS = {
    "apikey": SERVICE_ROLE,
    "Authorization": f"Bearer {SERVICE_ROLE}",
    "Accept-Profile": "public",
    "Content-Type": "application/json",
}

def _iso_minutes_ago(mins=180):
    return (dt.datetime.utcnow() - dt.timedelta(minutes=mins)).isoformat(timespec="seconds")+"Z"

def fetch_recent_jobs(since_minutes=180):
    since = _iso_minutes_ago(since_minutes)
    params = {
        "select": "id,title,apply_url,location_city,remote,posted_at,description,min_exp,max_exp,company_id,companies(name,comp_gate_status),first_seen_at",
        "order": "first_seen_at.desc",
        "first_seen_at": f"gte.{since}",
    }
    url = f"{SUPABASE_URL}/rest/v1/jobs?{urllib.parse.urlencode(params)}"
    r = requests.get(url, headers=HDRS, timeout=30)
    r.raise_for_status()
    return r.json()

def patch_job_ctc(job_id: int, ctc_pass: bool):
    url = f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}"
    r = requests.patch(url, headers=HDRS, json={"ctc_predicted_pass": ctc_pass})
    r.raise_for_status()

def main():
    jobs = fetch_recent_jobs(180)
    sent = 0
    for j in jobs:
        score, ctc_pass = score_job(j)

        # hard gates: avoid interns/managers; ensure overlap with 1–3 yrs if exp present
        title_l = (j.get("title") or "").lower()
        if any(x in title_l for x in ["intern","trainee","manager","lead","vp","director","principal","head"]):
            continue
        lo, hi = j.get("min_exp"), j.get("max_exp")
        if isinstance(lo,int) or isinstance(hi,int):
            lo = 0 if lo is None else lo
            hi = 99 if hi is None else hi
            if not (lo <= 3 and hi >= 1):
                continue

        try:
            patch_job_ctc(j["id"], ctc_pass)
        except Exception as e:
            print("ctc patch failed:", e)

        if score >= 80 and ctc_pass:
            c = (j.get("companies") or {}).get("name", "Company")
            loc = j.get("location_city") or "Location N/A"
            url = j.get("apply_url") or ""
            msg = f"<b>{c}</b> — <b>{j.get('title')}</b>\n{loc}\nScore: <b>{score}</b>\n{url}"
            try:
                tg_send(msg)
                sent += 1
            except Exception as e:
                print("Telegram send failed:", e)

    print(f"Score+Alert done. Sent={sent}")

if __name__ == "__main__":
    main()
