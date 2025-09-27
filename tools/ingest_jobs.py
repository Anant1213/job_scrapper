# tools/ingest_jobs.py
import os, csv, json, time
from tools.supabase_client import fetch_companies, upsert_jobs_raw, upsert_jobs
from tools.normalize import normalize_job, city_passes

# connectors
from connectors.custom_barclays import fetch as fetch_barclays
from connectors.workday_cxs import fetch as fetch_workday
from connectors.greenhouse_board import fetch as fetch_greenhouse
from connectors.lever_postings import fetch as fetch_lever

def _company_map():
    by_id = {}
    for c in fetch_companies():
        if not c.get("active"): 
            continue
        by_id[c["name"]] = c["id"]
    return by_id

def run_from_sources_csv():
    with open("config/sources.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        companies = _company_map()
        total_jobs = 0
        for row in reader:
            if row.get("active","true").lower() != "true":
                continue

            company = row["company"].strip()
            company_id = companies.get(company)
            if not company_id:
                print(f"Skip source for unknown company: {company}")
                continue

            kind = row["kind"]
            endpoint = row["endpoint_url"].strip()
            params = json.loads(row.get("params") or "{}")
            print(f"[{company}] {kind} -> {endpoint}")

            jobs, raw_payload = [], {}
            try:
                if kind == "barclays_search":
                    data = fetch_barclays(endpoint, max_pages=3)
                elif kind == "workday_cxs":
                    data = fetch_workday(endpoint, search_text=params.get("searchText"), limit=params.get("limit", 50), max_pages=params.get("max_pages", 3))
                elif kind == "greenhouse_board":
                    data = fetch_greenhouse(endpoint)
                elif kind == "lever_postings":
                    data = fetch_lever(endpoint)
                else:
                    print(f"  (no handler yet for kind={kind})")
                    continue

                raw_payload = {"count": len(data)}
                for d in data:
                    loc = d.get("location")
                    if not city_passes(loc):
                        continue
                    key, rec = normalize_job(
                        company_id=company_id,
                        title=d.get("title"),
                        apply_url=d.get("detail_url"),
                        location=loc,
                        description=d.get("description"),
                        req_id=d.get("req_id"),
                        posted_at=d.get("posted"),
                    )
                    jobs.append(rec)

            except Exception as e:
                print(f"  ! error fetching {company}: {e}")
                continue

            try:
                upsert_jobs_raw(company_id, None, endpoint, raw_payload)
            except Exception as e:
                print(f"  ! jobs_raw upsert failed: {e}")

            try:
                n = upsert_jobs(company_id, jobs)
                total_jobs += n
                print(f"  + upserted {n} jobs")
            except Exception as e:
                print(f"  ! jobs upsert failed: {e}")

            time.sleep(1)

        print(f"Done. Upserted total {total_jobs} jobs.")

if __name__ == "__main__":
    run_from_sources_csv()
