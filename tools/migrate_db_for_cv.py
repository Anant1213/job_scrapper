# tools/migrate_db_for_cv.py
"""
Database migration to add CV support and AI match scores.
Run this once to update the database schema.
"""
import os
import sys
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.local_db import get_db_path, get_connection

def migrate():
    """Add new tables and columns for CV matching."""
    
    print("Running database migration for CV support...")
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Create user_cv table
        print("  Creating user_cv table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_cv (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT,
                cv_text TEXT,
                extracted_skills TEXT,
                years_experience INTEGER,
                domain_expertise TEXT,
                preferred_roles TEXT,
                education TEXT,
                location_preference TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Add AI match score columns to jobs table
        print("  Adding AI match columns to jobs table...")
        
        # Check if columns exist first
        cursor.execute("PRAGMA table_info(jobs)")
        columns = {row[1] for row in cursor.fetchall()}
        
        if 'ai_match_score' not in columns:
            cursor.execute("ALTER TABLE jobs ADD COLUMN ai_match_score INTEGER DEFAULT 0")
            print("    ✓ Added ai_match_score column")
        else:
            print("    - ai_match_score column already exists")
        
        if 'match_reasoning' not in columns:
            cursor.execute("ALTER TABLE jobs ADD COLUMN match_reasoning TEXT")
            print("    ✓ Added match_reasoning column")
        else:
            print("    - match_reasoning column already exists")
        
        # Create index for AI match score
        print("  Creating indexes...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_ai_score ON jobs(ai_match_score DESC)")
        
        conn.commit()
        
    print(f"\n✓ Migration complete! Database at: {get_db_path()}")


if __name__ == "__main__":
    migrate()
