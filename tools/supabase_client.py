# tools/supabase_client.py
import os, json, requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_ROLE = os.environ["SUPABASE_SERVICE_ROLE"]
HDRS = {
    "apikey": SERVICE_ROLE,
    "Authorization": f"Bearer {SERVICE_ROLE}",
    "Content-Type": "application/json",
    "Accept-Profile": "public",
    # 👇 upsert for jobs too
    "Prefer": "resolution=merge-duplicates,return=representation",
}

def upsert_jobs_raw(company_id: int, source_id: int | None, page_url: str, payload: dict):
    url = f"{SUPABASE_URL}/rest/v1/jobs_raw"
    body = [{"company_id": company_id, "source_id": source_id, "page_url": page_url, "payload": payload}]
    r = requests.post(url, headers=HDRS, data=json.dumps(body), params={"select":"id"})
    r.raise_for_status()
    return r.json()[0]["id"] if r.content else None

def upsert_jobs(company_id: int, rows: list[dict]):
    if not rows:
        return 0
    url = f"{SUPABASE_URL}/rest/v1/jobs?on_conflict=company_id,canonical_key"
    r = requests.post(url, headers=HDRS, data=json.dumps(rows), params={"select":"id"})
    r.raise_for_status()
    return len(r.json()) if r.content else 0

def fetch_companies():
    url = f"{SUPABASE_URL}/rest/v1/companies?select=id,name,ats_type,active,comp_gate_status"
    r = requests.get(url, headers=HDRS)
    r.raise_for_status()
    return r.json()
