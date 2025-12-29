# tools/ingest_sites.py
"""
Ingest jobs from career sites that require JavaScript rendering (Playwright).
Uses sites.yaml for configuration.
"""
import sys
import json
import time
import traceback
from typing import Optional, List, Iterable

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    print("Warning: PyYAML not installed. Run: pip install PyYAML")

from tools.supabase_client import fetch_companies, upsert_jobs_raw, upsert_jobs
from tools.normalize import normalize_job, india_location_ok
from connectors.play_renderer import render_and_extract


CFG_PATH = "config/sites.yaml"


def _load_config() -> List[dict]:
    """Load and parse sites.yaml configuration."""
    if not YAML_AVAILABLE:
        raise RuntimeError("PyYAML not installed. Run: pip install PyYAML")
    
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    
    # Accept either:
    #  1) {"sites": [...]}
    #  2) [...]
    if isinstance(cfg, dict) and "sites" in cfg and isinstance(cfg["sites"], list):
        return cfg["sites"]
    if isinstance(cfg, list):
        return cfg
    
    raise ValueError(
        f"Unsupported YAML structure in {CFG_PATH}. "
        "Use a top-level list or a dict with 'sites' key."
    )


def _company_map(active_only: bool = True) -> dict:
    """Build company name -> ID mapping."""
    return {
        c["name"]: c["id"]
        for c in fetch_companies()
        if (c.get("active") if active_only else True)
    }


def _filter_companies(entries: List[dict], want: Optional[Iterable[str]]) -> List[dict]:
    """Filter entries to only include wanted companies."""
    if not want:
        return entries
    
    wanted = {w.strip().lower() for w in want if w.strip()}
    out = []
    for e in entries:
        name = (e.get("company") or "").strip().lower()
        if name in wanted:
            out.append(e)
    return out


def run(companies_filter: Optional[List[str]] = None):
    """
    Main entry point for sites ingestion.
    
    Args:
        companies_filter: Optional list of company names to process (case-insensitive)
    """
    try:
        sites = _load_config()
    except Exception as e:
        print(f"ERROR loading config: {e}")
        return
    
    if companies_filter:
        sites = _filter_companies(sites, companies_filter)

    companies = _company_map(active_only=True)
    total_jobs = 0
    errors = 0

    for s in sites:
        company = (s.get("company") or "").strip()
        if not company:
            print("! skip entry with no company")
            continue

        company_id = companies.get(company)
        if not company_id:
            print(f"! skip unknown/inactive company: {company}")
            continue

        url = s.get("url")
        if not url:
            print(f"! {company}: no url")
            continue
        
        # Skip inactive sites
        if s.get("active") is False:
            print(f"! {company}: site marked inactive")
            continue

        wait_for = s.get("wait_for")
        next_selector = s.get("next_selector")
        page_param = s.get("page_param")
        step = int(s.get("step", 1))
        max_pages = int(s.get("max_pages", 1))
        force_india = bool(s.get("force_india", False))

        selectors = s.get("selectors") or {}
        if "card" not in selectors:
            print(f"! {company}: selectors.card missing")
            continue

        print(f"[{company}] site -> {url}")
        
        try:
            rows = render_and_extract(
                url=url,
                selectors=selectors,
                max_pages=max_pages,
                next_selector=next_selector,
                wait_for=wait_for,
                force_india=force_india,
                page_param=page_param,
                step=step,
                do_scroll=bool(s.get("do_scroll", True)),
            )
        except Exception as e:
            print(f"  ! error scraping {company}: {e}")
            traceback.print_exc()
            errors += 1
            
            # Record the error
            try:
                upsert_jobs_raw(company_id, None, url, {"error": str(e)})
            except Exception:
                pass
            continue

        pre = len(rows)
        kept_rows = []
        for r in rows:
            loc = (r.get("location") or "").strip() or None
            if india_location_ok(loc):
                kept_rows.append(r)

        print(f"  fetched: {pre}, kept (India): {len(kept_rows)}")

        # Normalize + de-dupe by canonical key
        dedup = {}
        for r in kept_rows:
            _, rec = normalize_job(
                company_id=company_id,
                title=r.get("title"),
                apply_url=r.get("detail_url"),
                location=r.get("location"),
                description=r.get("description"),
                req_id=r.get("req_id"),
                posted_at=r.get("posted"),
            )
            key = rec.get("canonical_key")
            if key and key not in dedup:
                dedup[key] = rec

        # Record raw fetch
        try:
            upsert_jobs_raw(company_id, None, url, {"count": pre})
        except Exception as e:
            print(f"  ! jobs_raw upsert failed: {e}")

        # Upsert normalized jobs
        try:
            n = upsert_jobs(company_id, list(dedup.values()))
            total_jobs += n
            print(f"  + upserted {n} jobs")
        except Exception as e:
            print(f"  ! jobs upsert failed: {e}")
            traceback.print_exc()

        time.sleep(1)

    print(f"Done. Upserted total {total_jobs} jobs (errors: {errors}).")


if __name__ == "__main__":
    # Optional: python -m tools.ingest_sites "Morgan Stanley,Citi"
    want = None
    if len(sys.argv) > 1:
        want = [p.strip() for p in sys.argv[1].split(",")]
    run(companies_filter=want)
