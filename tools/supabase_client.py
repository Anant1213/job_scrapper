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
    # skip empty batch
    if not rows:
        return 0

    # de-duplicate within the batch to avoid "ON CONFLICT ... cannot affect row a second time"
    seen, cleaned = set(), []
    for rec in rows:
        # key: prefer req_id or apply_url; fallback to title+location
        k = (
            (rec.get("req_id") or "").strip().lower(),
            (rec.get("apply_url") or "").strip().lower(),
            (rec.get("title") or "").strip().lower(),
            (rec.get("location_city") or "").strip().lower(),
        )
        if k in seen:
            continue
        seen.add(k)

        # strip generated column
        r = {kk: vv for kk, vv in rec.items() if kk != "canonical_key"}
        r["company_id"] = company_id
        cleaned.append(r)

    if not cleaned:
        return 0

    url = f"{SUPABASE_URL}/rest/v1/jobs?on_conflict=company_id,canonical_key&select=id"
    resp = requests.post(url, headers=HDRS, json=cleaned, timeout=60)
    resp.raise_for_status()
    return len(resp.json()) if resp.content else 0

def fetch_companies():
    url = f"{SUPABASE_URL}/rest/v1/companies?select=id,name,ats_type,careers_url,active"
    r = requests.get(url, headers=HDRS, timeout=45)
    r.raise_for_status()
    return r.json()
