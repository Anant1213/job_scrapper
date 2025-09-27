# tools/ingest_jobs.py
import os, csv, json, time
from tools.supabase_client import fetch_companies, upsert_jobs_raw, upsert_jobs
from tools.normalize import normalize_job, city_passes
from connectors.custom_barclays import fetch as fetch_barclays

def _company_map():
    # {name -> id}
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
            endpoint = row["endpoint_url"]
            params = json.loads(row.get("params") or "{}")

            print(f"[{company}] {kind} -> {endpoint}")
            jobs = []
            raw_payload = {}

            try:
                if kind == "barclays_search":
                    data = fetch_barclays(endpoint, max_pages=3)
                    raw_payload = {"count": len(data)}
                    for d in data:
                        if not city_passes(d.get("location")):
                            continue
                        key, rec = normalize_job(
                            company_id=company_id,
                            title=d["title"],
                            apply_url=d["detail_url"],
                            location=d.get("location"),
                            description=d.get("description"),
                            req_id=None,
                            posted_at=None
                        )
                        jobs.append(rec)
                else:
                    print(f"  (no handler yet for kind={kind})")
                    continue
            except Exception as e:
                print(f"  ! error fetching {company}: {e}")
                continue

            # store raw summary for auditing
            try:
                upsert_jobs_raw(company_id, None, endpoint, raw_payload)
            except Exception as e:
                print(f"  ! jobs_raw upsert failed: {e}")

            # upsert normalized
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
