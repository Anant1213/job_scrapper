# database/__init__.py
"""Database module for job scraper."""
from database.local_db import (
    init_db,
    get_db_path,
    fetch_companies,
    upsert_company,
    upsert_jobs,
    upsert_jobs_raw,
    update_job_score,
    get_jobs,
    get_job_count,
    get_stats,
)

__all__ = [
    "init_db",
    "get_db_path",
    "fetch_companies",
    "upsert_company",
    "upsert_jobs",
    "upsert_jobs_raw",
    "update_job_score",
    "get_jobs",
    "get_job_count",
    "get_stats",
]
