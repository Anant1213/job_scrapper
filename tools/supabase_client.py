# tools/supabase_client.py
import os, requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_ROLE = os.environ["SUPABASE_SERVICE_ROLE"]

HDRS = {
    "apikey": SUPABASE_SERVICE_ROLE,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

def upsert_jobs_raw(company_id: int, source_id, page_url: str, payload: dict):
    url = f"{SUPABASE_URL}/rest/v1/jobs_raw?select=id"
    rows = [{
        "company_id": company_id,
        "source_id": source_id,
        "page_url": page_url,
        "payload": payload,
    }]
    r = requests.post(url, headers=HDRS, json=rows, timeout=45)
    r.raise_for_status()
    return r.json()[0]["id"] if r.content else None

def upsert_jobs(company_id: int, rows: list[dict]):
    # >>> important: if nothing to insert, skip the POST (PostgREST 400 on empty body)
    if not rows:
        return 0
    for r in rows:
        r["company_id"] = company_id
    url = f"{SUPABASE_URL}/rest/v1/jobs?on_conflict=company_id,canonical_key&select=id"
    r = requests.post(url, headers=HDRS, json=rows, timeout=60)
    r.raise_for_status()
    return len(r.json()) if r.content else 0

def fetch_companies():
    url = f"{SUPABASE_URL}/rest/v1/companies?select=id,name,ats_type,careers_url,active"
    r = requests.get(url, headers=HDRS, timeout=45)
    r.raise_for_status()
    return r.json()
