# tools/ingest_sites.py
import argparse, json, time, yaml
from tools.supabase_client import fetch_companies, upsert_jobs_raw, upsert_jobs
from tools.normalize import normalize_job, india_location_ok
from connectors.direct_site import fetch as fetch_static
from connectors.play_renderer import render_and_extract

def _company_map():
    return {c["name"]: c["id"] for c in fetch_companies() if c.get("active")}

def run(companies_filter=None):
    with open("config/sites.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    company_ids = _company_map()
    total = 0

    for entry in cfg.get("sites", []):
        if not entry.get("active", True):
            continue
        name = entry["company"]
        if companies_filter and name not in companies_filter:
            continue
        company_id = company_ids.get(name)
        if not company_id:
            print(f"Skip unknown company in sites.yaml: {name}")
            continue

        url = entry["url"]
        print(f"[{name}] site -> {url}")
        rows, raw_count = [], 0

        try:
            if entry.get("render", False):
                rows0 = render_and_extract(
                    url=url,
                    selectors=entry["selectors"],
                    max_pages=entry.get("max_pages", 1),
                    next_selector=entry.get("next_selector"),
                    wait_for=entry.get("wait_for"),
                    force_india=entry.get("force_india", False)
                )
            else:
                rows0 = fetch_static(
                    url=url,
                    selectors=entry["selectors"],
                    max_pages=entry.get("max_pages", 1),
                    page_param=entry.get("page_param"),
                    start_page=entry.get("start_page", 1),
                    force_india=entry.get("force_india", False),
                    delay_s=entry.get("delay_s", 0.4)
                )
            raw_count = len(rows0)

            jobs = []
            for d in rows0:
                loc = (d.get("location") or "").strip() or None
                # india filter powered by location and/or apply_url hints
                if not india_location_ok(loc, d.get("detail_url")):
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

            print(f"  fetched: {raw_count}, kept (India): {len(jobs)}")
            try:
                upsert_jobs_raw(company_id, None, url, {"count": raw_count})
            except Exception as e:
                print(f"  ! jobs_raw upsert failed: {e}")

            n = upsert_jobs(company_id, jobs)
            total += n
            print(f"  + upserted {n} jobs")

        except Exception as e:
            print(f"  ! error scraping {name}: {e}")

        time.sleep(0.5)

    print(f"Done. Upserted total {total} jobs.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", type=str, help="Comma-separated company names to run")
    args = ap.parse_args()
    companies = [s.strip() for s in args.only.split(",")] if args.only else None
    run(companies_filter=companies)
