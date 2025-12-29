# tools/ingest_jobs.py
"""
Main job ingestion script that reads sources.csv and fetches jobs from various ATS platforms.
Supports both local SQLite and Supabase databases.
"""
import csv
import json
import os
import time
import traceback
from typing import Optional, List

# Set local database mode
USE_LOCAL_DB = os.environ.get("USE_LOCAL_DB", "true").lower() == "true"

if USE_LOCAL_DB:
    from database.local_db import fetch_companies, upsert_jobs_raw, upsert_jobs, init_db, upsert_company
else:
    from tools.supabase_client import fetch_companies, upsert_jobs_raw, upsert_jobs

from tools.normalize import normalize_job, india_location_ok

from connectors.workday_cxs import fetch as fetch_workday
from connectors.oracle_cx import fetch as fetch_oracle
from connectors.citi_custom import fetch as fetch_citi
from connectors.taleo_tgnewui import fetch as fetch_taleo
from connectors.brassring_go import fetch as fetch_brassring
from connectors.custom_barclays import fetch as fetch_barclays
from connectors.bnpp_group import fetch as fetch_bnpp
from connectors.greenhouse_board import fetch as fetch_greenhouse


def _company_map() -> dict:
    """Build a mapping of company name -> company ID for active companies."""
    companies = fetch_companies()
    return {c["name"]: c["id"] for c in companies if c.get("active")}


def _ensure_company(company_name: str, company_map: dict) -> Optional[int]:
    """Ensure company exists in database, create if needed."""
    if company_name in company_map:
        return company_map[company_name]
    
    if USE_LOCAL_DB:
        # Auto-create company for local testing
        company_id = upsert_company(
            name=company_name,
            careers_url=None,
            ats_type="greenhouse",
            active=True,
            comp_gate_status="pass",
        )
        company_map[company_name] = company_id
        print(f"  Auto-created company: {company_name} (id={company_id})")
        return company_id
    
    return None


def _truthy(v, default: bool = True) -> bool:
    """Parse a string value as a boolean."""
    if v is None or v == "":
        return default
    return str(v).strip().lower() in ("true", "1", "yes", "y")


def _filter_india_jobs(jobs: List[dict], india_only: bool = True) -> List[dict]:
    """Filter jobs to only include India-based positions."""
    if not india_only:
        return jobs
    
    india_keywords = [
        'india', 'bengaluru', 'bangalore', 'mumbai', 'hyderabad', 
        'pune', 'chennai', 'gurgaon', 'gurugram', 'noida', 'delhi',
        'kolkata', 'ahmedabad', 'remote - india', 'in-'
    ]
    
    filtered = []
    for job in jobs:
        location = str(job.get('location', '')).lower()
        if any(kw in location for kw in india_keywords):
            filtered.append(job)
    
    return filtered


def run_from_sources_csv():
    """
    Read sources.csv and ingest jobs from each configured source.
    """
    # Initialize local database if needed
    if USE_LOCAL_DB:
        init_db()
    
    with open("config/sources.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        company_map = _company_map()
        total_jobs = 0

        for row in reader:
            company = (row.get("company") or "").strip()
            
            # Skip comments and empty rows
            if not company or company.startswith("#"):
                continue
            
            # Skip inactive sources
            if not _truthy(row.get("active"), True):
                continue

            # Ensure company exists
            company_id = _ensure_company(company, company_map)
            if not company_id:
                print(f"Skip source for unknown company: {company}")
                continue

            kind = (row.get("kind") or "").strip()
            endpoint = (row.get("endpoint_url") or "").strip()
            
            # Parse params JSON
            params = {}
            try:
                params = json.loads(row.get("params") or "{}")
            except json.JSONDecodeError:
                params = {}

            print(f"[{company}] {kind} -> {endpoint[:50]}...")
            raw_payload = {}
            fetched = []
            india_only = bool(params.get("india_only", True))

            try:
                if kind == "greenhouse":
                    # Use Greenhouse API
                    fetched = fetch_greenhouse(
                        endpoint_url=endpoint,
                        max_pages=int(params.get("max_pages", 1))
                    )
                    # Filter for India jobs
                    fetched = _filter_india_jobs(fetched, india_only)
                
                elif kind == "workday_cxs":
                    fetched = fetch_workday(
                        endpoint_url=endpoint,
                        search_text=params.get("searchText"),
                        limit=int(params.get("limit", 50)),
                        max_pages=int(params.get("max_pages", 6)),
                        india_only=india_only,
                    )

                elif kind == "oracle_cx":
                    fetched = fetch_oracle(
                        endpoint_url=endpoint,
                        site_number=params.get("site_number"),
                        limit=int(params.get("limit", 200)),
                        max_pages=int(params.get("max_pages", 15)),
                        india_only=india_only,
                    )

                elif kind == "citi_custom":
                    fetched = fetch_citi(
                        india_base_url=endpoint,
                        max_pages=int(params.get("max_pages", 10))
                    )

                elif kind == "taleo_tgnewui":
                    fetched = fetch_taleo(
                        search_url=endpoint,
                        max_pages=int(params.get("max_pages", 4)),
                    )

                elif kind == "brassring_go":
                    fetched = fetch_brassring(
                        go_page_url=endpoint,
                        max_pages=int(params.get("max_pages", 6))
                    )

                elif kind == "barclays_search":
                    fetched = fetch_barclays(
                        endpoint_url=endpoint,
                        max_pages=int(params.get("max_pages", 5))
                    )

                elif kind == "bnpp_group":
                    fetched = fetch_bnpp(
                        india_landing_url=endpoint,
                        max_pages=int(params.get("max_pages", 3))
                    )

                else:
                    print(f"  (no handler yet for kind={kind})")
                    continue

                print(f"  fetched: {len(fetched)} jobs")
                raw_payload = {"count": len(fetched)}

                # Filter and normalize jobs
                jobs = []
                for d in fetched:
                    loc = (d.get("location") or "").strip() or None
                    
                    # For Greenhouse, location is already filtered
                    if kind != "greenhouse" and not india_location_ok(loc):
                        continue
                    
                    _, rec = normalize_job(
                        company_id=company_id,
                        title=d.get("title"),
                        apply_url=d.get("detail_url"),
                        location=loc,
                        description=d.get("description"),
                        req_id=d.get("req_id"),
                        posted_at=d.get("posted"),
                    )
                    jobs.append(rec)

                print(f"  normalized: {len(jobs)} jobs")

            except Exception as e:
                print(f"  ! error fetching {company}: {e}")
                traceback.print_exc()
                continue

            # Record raw fetch attempt
            try:
                upsert_jobs_raw(company_id, None, endpoint, raw_payload)
            except Exception as e:
                print(f"  ! jobs_raw upsert failed: {e}")

            # Upsert normalized jobs
            try:
                n = upsert_jobs(company_id, jobs)
                total_jobs += n
                print(f"  + upserted {n} jobs")
            except Exception as e:
                print(f"  ! jobs upsert failed: {e}")
                traceback.print_exc()

            time.sleep(0.5)

        print(f"\n{'='*50}")
        print(f"Done. Upserted total {total_jobs} jobs.")
        print(f"{'='*50}")


if __name__ == "__main__":
    os.environ["USE_LOCAL_DB"] = "true"
    run_from_sources_csv()
