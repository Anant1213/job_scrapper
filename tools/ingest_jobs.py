# tools/ingest_jobs.py
import csv, json, time
from tools.supabase_client import fetch_companies, upsert_jobs_raw, upsert_jobs
from tools.normalize import normalize_job, india_location_ok

from connectors.workday_cxs import fetch as fetch_workday
from connectors.oracle_cx import fetch as fetch_oracle
from connectors.citi_custom import fetch as fetch_citi
from connectors.taleo_tgnewui import fetch as fetch_taleo
from connectors.brassring_go import fetch as fetch_brassring
from connectors.custom_barclays import fetch as fetch_barclays  # keep for now
from connectors.bnpp_group import fetch as fetch_bnpp          # if you already had it

def _company_map():
    return {c["name"]: c["id"] for c in fetch_companies() if c.get("active")}

def _truthy(v, default=True):
    if v is None or v == "": return default
    return str(v).strip().lower() in ("true","1","yes","y")

def run_from_sources_csv():
    with open("config/sources.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        companies = _company_map()
        total_jobs = 0

        for row in reader:
            company = (row.get("company") or "").strip()
            if not company or company.startswith("#"): continue
            if not _truthy(row.get("active"), True):   continue

            company_id = companies.get(company)
            if not company_id:
                print(f"Skip source for unknown company: {company}")
                continue

            kind = (row.get("kind") or "").strip()
            endpoint = (row.get("endpoint_url") or "").strip()
            params = {}
            try:
                params = json.loads(row.get("params") or "{}")
            except Exception:
                params = {}

            print(f"[{company}] {kind} -> {endpoint}")
            raw_payload = {}
            jobs, fetched = [], []

            try:
                if kind == "workday_cxs":
                    fetched = fetch_workday(
                        endpoint_url=endpoint,
                        search_text=params.get("searchText"),
                        limit=int(params.get("limit", 50)),
                        max_pages=int(params.get("max_pages", 6)),
                        india_only=bool(params.get("india_only", True)),
                    )
                # tools/ingest_jobs.py  (inside run_from_sources_csv loop)
                elif kind == "oracle_cx":
                    data = fetch_oracle(
                        endpoint,
                        site_number=params.get("site_number"),
                        limit=int(params.get("limit", 200)),
                        max_pages=int(params.get("max_pages", 15)),
                        india_only=bool(params.get("india_only", True)),
                    )


                elif kind == "citi_custom":
                    fetched = fetch_citi(india_base_url=endpoint, max_pages=int(params.get("max_pages", 10)))
                elif kind == "taleo_tgnewui":
                    data = fetch_taleo(
                        search_url=endpoint,         # <— rename this arg
                        max_pages=int(params.get("max_pages", 4)),
                    )

                elif kind == "brassring_go":
                    fetched = fetch_brassring(go_page_url=endpoint, max_pages=int(params.get("max_pages", 6)))
                elif kind == "barclays_search":
                    fetched = fetch_barclays(endpoint, max_pages=int(params.get("max_pages", 5)))
                elif kind == "bnpp_group":
                    fetched = fetch_bnpp(india_landing_url=endpoint, max_pages=int(params.get("max_pages", 3)))
                else:
                    print(f"  (no handler yet for kind={kind})")
                    continue

                print(f"  fetched: {len(fetched)} rows (pre-filter)")
                raw_payload = {"count": len(fetched)}

                for d in fetched:
                    loc = (d.get("location") or "").strip() or None
                    if not india_location_ok(loc):
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

                print(f"  kept (India): {len(jobs)}")

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

            time.sleep(0.6)

        print(f"Done. Upserted total {total_jobs} jobs.")

if __name__ == "__main__":
    run_from_sources_csv()
