# database/local_db.py
"""
Local SQLite database for job scraper.
Replaces Supabase for local development and testing.
"""
import os
import sqlite3
import json
from datetime import datetime
from typing import Optional, List
from contextlib import contextmanager

# Database file location
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "jobs.db")


def get_db_path() -> str:
    """Get the database file path."""
    return os.path.abspath(DB_PATH)


@contextmanager
def get_connection():
    """Get a database connection context manager."""
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    """Initialize the database with required tables."""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Companies table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                careers_url TEXT,
                ats_type TEXT,
                active BOOLEAN DEFAULT 1,
                comp_gate_status TEXT DEFAULT 'pass',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Jobs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                title TEXT,
                apply_url TEXT,
                team TEXT,
                location_city TEXT,
                location_country TEXT,
                description TEXT,
                req_id TEXT,
                posted_at TEXT,
                canonical_key TEXT,
                remote BOOLEAN DEFAULT 0,
                min_exp INTEGER,
                max_exp INTEGER,
                relevance_score INTEGER,
                ctc_predicted_pass BOOLEAN,
                first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (company_id) REFERENCES companies(id),
                UNIQUE(company_id, canonical_key)
            )
        """)
        
        # Jobs raw table (for debugging)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER,
                source_id INTEGER,
                page_url TEXT,
                payload TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (company_id) REFERENCES companies(id)
            )
        """)
        
        # Create indexes for faster queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_score ON jobs(relevance_score DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_posted ON jobs(posted_at DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_first_seen ON jobs(first_seen_at DESC)")
        
        conn.commit()
        print(f"Database initialized at: {get_db_path()}")


def fetch_companies() -> List[dict]:
    """Fetch all companies from database."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, name, ats_type, careers_url, active, comp_gate_status
            FROM companies
        """)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


def upsert_company(
    name: str,
    careers_url: Optional[str] = None,
    ats_type: Optional[str] = None,
    active: bool = True,
    comp_gate_status: str = "pass"
) -> int:
    """Insert or update a company."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO companies (name, careers_url, ats_type, active, comp_gate_status)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                careers_url = excluded.careers_url,
                ats_type = excluded.ats_type,
                active = excluded.active,
                comp_gate_status = excluded.comp_gate_status,
                updated_at = CURRENT_TIMESTAMP
        """, (name, careers_url, ats_type, active, comp_gate_status))
        conn.commit()
        
        cursor.execute("SELECT id FROM companies WHERE name = ?", (name,))
        return cursor.fetchone()[0]


def upsert_jobs_raw(
    company_id: int,
    source_id: Optional[int],
    page_url: str,
    payload: dict
) -> int:
    """Store raw job fetch data."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO jobs_raw (company_id, source_id, page_url, payload)
            VALUES (?, ?, ?, ?)
        """, (company_id, source_id, page_url, json.dumps(payload)))
        conn.commit()
        return cursor.lastrowid


def upsert_jobs(company_id: int, rows: List[dict]) -> int:
    """Upsert normalized job records."""
    if not rows:
        return 0
    
    # De-duplicate within batch
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
        r["canonical_key"] = rec.get("canonical_key")
        cleaned.append(r)
    
    if not cleaned:
        return 0
    
    count = 0
    with get_connection() as conn:
        cursor = conn.cursor()
        for rec in cleaned:
            try:
                cursor.execute("""
                    INSERT INTO jobs (
                        company_id, title, apply_url, team, location_city,
                        location_country, description, req_id, posted_at,
                        canonical_key, remote, min_exp, max_exp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(company_id, canonical_key) DO UPDATE SET
                        title = excluded.title,
                        apply_url = excluded.apply_url,
                        location_city = excluded.location_city,
                        location_country = excluded.location_country,
                        description = excluded.description,
                        posted_at = excluded.posted_at,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    rec.get("company_id"),
                    rec.get("title"),
                    rec.get("apply_url"),
                    rec.get("team"),
                    rec.get("location_city"),
                    rec.get("location_country"),
                    rec.get("description"),
                    rec.get("req_id"),
                    rec.get("posted_at"),
                    rec.get("canonical_key"),
                    rec.get("remote", False),
                    rec.get("min_exp"),
                    rec.get("max_exp"),
                ))
                count += 1
            except Exception as e:
                print(f"  Error inserting job: {e}")
        conn.commit()
    
    return count


def update_job_score(job_id: int, score: int, ctc_pass: bool):
    """Update job with relevance score."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE jobs 
            SET relevance_score = ?, ctc_predicted_pass = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (score, ctc_pass, job_id))
        conn.commit()


def get_jobs(
    limit: int = 100,
    offset: int = 0,
    company_id: Optional[int] = None,
    min_score: Optional[int] = None,
    search: Optional[str] = None,
    sort_by: str = "first_seen_at",
    sort_order: str = "DESC"
) -> List[dict]:
    """Get jobs with filtering and pagination."""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        query = """
            SELECT 
                j.*,
                c.name as company_name,
                c.comp_gate_status
            FROM jobs j
            JOIN companies c ON j.company_id = c.id
            WHERE 1=1
        """
        params = []
        
        if company_id:
            query += " AND j.company_id = ?"
            params.append(company_id)
        
        if min_score is not None:
            query += " AND j.relevance_score >= ?"
            params.append(min_score)
        
        if search:
            query += " AND (j.title LIKE ? OR j.location_city LIKE ? OR c.name LIKE ?)"
            search_term = f"%{search}%"
            params.extend([search_term, search_term, search_term])
        
        # Validate sort_by to prevent SQL injection
        valid_sorts = ["first_seen_at", "relevance_score", "posted_at", "title"]
        if sort_by not in valid_sorts:
            sort_by = "first_seen_at"
        
        sort_order = "DESC" if sort_order.upper() == "DESC" else "ASC"
        query += f" ORDER BY j.{sort_by} {sort_order}"
        query += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


def get_job_count(
    company_id: Optional[int] = None,
    min_score: Optional[int] = None,
    search: Optional[str] = None
) -> int:
    """Get total job count with filters."""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        query = """
            SELECT COUNT(*) as count
            FROM jobs j
            JOIN companies c ON j.company_id = c.id
            WHERE 1=1
        """
        params = []
        
        if company_id:
            query += " AND j.company_id = ?"
            params.append(company_id)
        
        if min_score is not None:
            query += " AND j.relevance_score >= ?"
            params.append(min_score)
        
        if search:
            query += " AND (j.title LIKE ? OR j.location_city LIKE ? OR c.name LIKE ?)"
            search_term = f"%{search}%"
            params.extend([search_term, search_term, search_term])
        
        cursor.execute(query, params)
        return cursor.fetchone()[0]


def get_stats() -> dict:
    """Get database statistics."""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM companies WHERE active = 1")
        company_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM jobs")
        job_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM jobs WHERE relevance_score >= 80")
        high_score_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT c.name, COUNT(j.id) as job_count
            FROM companies c
            LEFT JOIN jobs j ON c.id = j.company_id
            WHERE c.active = 1
            GROUP BY c.id
            ORDER BY job_count DESC
            LIMIT 10
        """)
        top_companies = [{"name": row[0], "job_count": row[1]} for row in cursor.fetchall()]
        
        return {
            "company_count": company_count,
            "job_count": job_count,
            "high_score_count": high_score_count,
            "top_companies": top_companies,
        }


if __name__ == "__main__":
    init_db()
    print("Database initialized successfully!")
