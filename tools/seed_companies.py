# tools/seed_companies.py
"""
Seed companies from config/firms.csv into the database.
Supports both local SQLite and Supabase.
"""
import csv
import os
import sys
import time
import traceback

# Check database mode
USE_LOCAL_DB = os.environ.get("USE_LOCAL_DB", "true").lower() == "true"

if USE_LOCAL_DB:
    from database.local_db import init_db, upsert_company
else:
    import json
    import requests
    
    def _get_env_var(name: str) -> str:
        value = os.environ.get(name)
        if not value:
            raise KeyError(name)
        return value.rstrip("/") if name == "SUPABASE_URL" else value

CSV_PATH = "config/firms.csv"


def seed_local():
    """Seed companies to local SQLite database."""
    # Initialize database
    init_db()
    
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        names = []
        errors = 0
        
        for row in reader:
            if row.get("active", "true").lower() != "true":
                continue
            
            try:
                company_id = upsert_company(
                    name=row["company"].strip(),
                    careers_url=row.get("careers_url") or None,
                    ats_type=row.get("ats_type") or None,
                    active=True,
                    comp_gate_status=row.get("comp_gate_status") or "pass",
                )
                names.append(row["company"].strip())
            except Exception as e:
                print(f"  Error upserting {row.get('company')}: {e}")
                errors += 1
        
        print(f"Seeded/updated {len(names)} companies (errors: {errors}):")
        print(" - " + "\n - ".join(names))


def seed_supabase():
    """Seed companies to Supabase."""
    try:
        supabase_url = _get_env_var("SUPABASE_URL")
        service_role = _get_env_var("SUPABASE_SERVICE_ROLE")
    except KeyError as e:
        print(f"Missing env var: {e}. Use USE_LOCAL_DB=true for local database.")
        sys.exit(1)
    
    headers = {
        "apikey": service_role,
        "Authorization": f"Bearer {service_role}",
        "Content-Type": "application/json",
        "Accept-Profile": "public",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }
    
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        names = []
        errors = 0
        
        for row in reader:
            if row.get("active", "true").lower() != "true":
                continue
            
            payload = {
                "name": row["company"].strip(),
                "careers_url": row.get("careers_url") or None,
                "ats_type": row.get("ats_type") or None,
                "active": True,
                "comp_gate_status": row.get("comp_gate_status") or "pass",
            }
            
            try:
                url = f"{supabase_url}/rest/v1/companies?on_conflict=name"
                r = requests.post(
                    url,
                    headers=headers,
                    data=json.dumps([payload]),
                    params={"select": "id,name"},
                    timeout=30
                )
                r.raise_for_status()
                names.append(payload["name"])
            except Exception as e:
                print(f"  Error upserting {payload['name']}: {e}")
                errors += 1
            
            time.sleep(0.2)
        
        print(f"Seeded/updated {len(names)} companies (errors: {errors}):")
        print(" - " + "\n - ".join(names))


def main():
    """Main entry point."""
    if USE_LOCAL_DB:
        print("Using local SQLite database")
        seed_local()
    else:
        print("Using Supabase")
        seed_supabase()


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError:
        print(f"ERROR: Config file not found: {CSV_PATH}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
