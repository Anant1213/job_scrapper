# tools/supabase_client.py
"""
Database client for job scraper.
Supports both local SQLite and remote Supabase.
"""
import os
import sys

# Check if we should use local database
USE_LOCAL_DB = os.environ.get("USE_LOCAL_DB", "true").lower() == "true"

if USE_LOCAL_DB:
    # Use local SQLite database
    try:
        from database.local_db import (
            fetch_companies,
            upsert_jobs_raw,
            upsert_jobs,
            init_db,
        )
        print("Using local SQLite database")
    except ImportError:
        # Fallback to creating inline if import fails
        print("Warning: Could not import local_db, will try Supabase")
        USE_LOCAL_DB = False

if not USE_LOCAL_DB:
    # Use Supabase (remote database)
    import requests
    from typing import Optional

    def _get_env_var(name: str) -> str:
        """Get required environment variable or exit with helpful message."""
        value = os.environ.get(name)
        if not value:
            print(f"ERROR: Missing required environment variable: {name}")
            print(f"Please set {name} before running this script.")
            print(f"Example: export {name}='your_value_here'")
            print(f"\nOr use local database: export USE_LOCAL_DB=true")
            sys.exit(1)
        return value.rstrip("/") if name == "SUPABASE_URL" else value

    SUPABASE_URL = _get_env_var("SUPABASE_URL")
    SUPABASE_SERVICE_ROLE = _get_env_var("SUPABASE_SERVICE_ROLE")

    def _get_headers() -> dict:
        """Get headers for Supabase API requests."""
        return {
            "apikey": SUPABASE_SERVICE_ROLE,
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=representation",
        }

    def upsert_jobs_raw(
        company_id: int,
        source_id: Optional[int],
        page_url: str,
        payload: dict
    ) -> Optional[int]:
        """Store raw job fetch data."""
        url = f"{SUPABASE_URL}/rest/v1/jobs_raw?select=id"
        rows = [{
            "company_id": company_id,
            "source_id": source_id,
            "page_url": page_url,
            "payload": payload,
        }]
        
        r = requests.post(url, headers=_get_headers(), json=rows, timeout=45)
        r.raise_for_status()
        
        if r.ok and r.content:
            try:
                data = r.json()
                if data and len(data) > 0:
                    return data[0].get("id")
            except (ValueError, KeyError, IndexError):
                pass
        return None

    def upsert_jobs(company_id: int, rows: list) -> int:
        """Upsert normalized job records."""
        if not rows:
            return 0

        seen, cleaned = set(), []
        for rec in rows:
            k = (
                (rec.get("req_id") or "").strip().lower(),
                (rec.get("apply_url") or "").strip().lower(),
                (rec.get("title") or "").strip().lower(),
                (rec.get("location_city") or "").strip().lower(),
            )
            if k in seen:
                continue
            seen.add(k)

            r = {kk: vv for kk, vv in rec.items() if kk != "canonical_key"}
            r["company_id"] = company_id
            cleaned.append(r)

        if not cleaned:
            return 0

        url = f"{SUPABASE_URL}/rest/v1/jobs?on_conflict=company_id,canonical_key&select=id"
        resp = requests.post(url, headers=_get_headers(), json=cleaned, timeout=60)
        resp.raise_for_status()
        
        if resp.ok and resp.content:
            try:
                return len(resp.json())
            except (ValueError, TypeError):
                return 0
        return 0

    def fetch_companies() -> list:
        """Fetch all companies from database."""
        url = f"{SUPABASE_URL}/rest/v1/companies?select=id,name,ats_type,careers_url,active"
        r = requests.get(url, headers=_get_headers(), timeout=45)
        r.raise_for_status()
        return r.json() if r.ok else []
