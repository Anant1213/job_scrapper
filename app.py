# app.py
"""
Flask web application for Job Scraper.
Provides a web interface to view and manage scraped jobs.
"""
import os
import sys

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, render_template, request, jsonify, redirect, url_for, send_from_directory
from werkzeug.utils import secure_filename
import threading
import time
import json
from database.local_db import init_db, get_jobs, get_job_count, get_stats, fetch_companies, get_connection
from tools.cv_parser import extract_cv_text, clean_cv_text
from tools.ollama_client import test_connection, extract_skills_from_cv, match_job_to_cv

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-change-in-production")
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(__file__), 'uploads')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
ALLOWED_EXTENSIONS = {'pdf', 'docx', 'doc', 'txt'}

# Ensure upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Global state for background tasks
background_tasks = {}


@app.before_request
def ensure_db():
    """Ensure database is initialized."""
    init_db()


@app.route("/")
def index():
    """Home page with job listings."""
    # Get filter parameters
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int)
    company_id = request.args.get("company", type=int)
    min_score = request.args.get("min_score", type=int)
    search = request.args.get("search", "").strip()
    sort_by = request.args.get("sort", "first_seen_at")
    sort_order = request.args.get("order", "DESC")
    
    # Calculate offset
    offset = (page - 1) * per_page
    
    # Get jobs
    jobs = get_jobs(
        limit=per_page,
        offset=offset,
        company_id=company_id,
        min_score=min_score,
        search=search if search else None,
        sort_by=sort_by,
        sort_order=sort_order,
    )
    
    # Get total count for pagination
    total = get_job_count(
        company_id=company_id,
        min_score=min_score,
        search=search if search else None,
    )
    
    # Calculate pagination
    total_pages = (total + per_page - 1) // per_page
    
    # Get companies for filter dropdown
    companies = fetch_companies()
    
    # Get stats
    stats = get_stats()
    
    return render_template(
        "index.html",
        jobs=jobs,
        companies=companies,
        stats=stats,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        company_id=company_id,
        min_score=min_score,
        search=search,
        sort_by=sort_by,
        sort_order=sort_order,
    )


@app.route("/api/jobs")
def api_jobs():
    """API endpoint for jobs."""
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int)
    company_id = request.args.get("company", type=int)
    min_score = request.args.get("min_score", type=int)
    search = request.args.get("search", "").strip()
    sort_by = request.args.get("sort", "first_seen_at")
    sort_order = request.args.get("order", "DESC")
    
    offset = (page - 1) * per_page
    
    jobs = get_jobs(
        limit=per_page,
        offset=offset,
        company_id=company_id,
        min_score=min_score,
        search=search if search else None,
        sort_by=sort_by,
        sort_order=sort_order,
    )
    
    total = get_job_count(
        company_id=company_id,
        min_score=min_score,
        search=search if search else None,
    )
    
    return jsonify({
        "jobs": jobs,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
    })


@app.route("/api/stats")
def api_stats():
    """API endpoint for statistics."""
    return jsonify(get_stats())


@app.route("/api/companies")
def api_companies():
    """API endpoint for companies."""
    return jsonify(fetch_companies())


def allowed_file(filename):
    """Check if file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route("/api/cv-status")
def cv_status():
    """Check if CV is uploaded."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT filename, extracted_skills, years_experience, uploaded_at FROM user_cv ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        
        if row:
            return jsonify({
                "has_cv": True,
                "filename": row[0],
                "skills": row[1],
                "experience": row[2],
                "uploaded_at": row[3]
            })
        else:
            return jsonify({"has_cv": False})


@app.route("/upload-cv", methods=["POST"])
def upload_cv():
    """Upload and analyze CV."""
    if 'cv_file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    
    file = request.files['cv_file']
    
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    
    if not allowed_file(file.filename):
        return jsonify({"error": "Invalid file type. Allowed: PDF, DOCX, TXT"}), 400
    
    try:
        # Save file
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Extract text
        cv_text = extract_cv_text(filepath)
        cv_text = clean_cv_text(cv_text)
        
        # Start background task for CV analysis
        task_id = f"cv_analysis_{int(time.time())}"
        background_tasks[task_id] = {
            "status": "running",
            "progress": "Analyzing CV with AI...",
            "result": None
        }
        
        def analyze_cv_async():
            try:
                # Extract skills with Ollama
                skills_data = extract_skills_from_cv(cv_text)
                
                # Save to database
                with get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM user_cv")  # Keep only latest
                    cursor.execute("""
                        INSERT INTO user_cv (
                            filename, cv_text, extracted_skills, years_experience,
                            domain_expertise, preferred_roles, education, location_preference
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        filename,
                        cv_text,
                        json.dumps(skills_data.get('technical_skills', [])),
                        skills_data.get('years_experience', 0),
                        json.dumps(skills_data.get('domain_expertise', [])),
                        json.dumps(skills_data.get('preferred_roles', [])),
                        skills_data.get('education', ''),
                        json.dumps(skills_data.get('location_preference', []))
                    ))
                    conn.commit()
                
                background_tasks[task_id] = {
                    "status": "complete",
                    "progress": "CV analysis complete!",
                    "result": skills_data
                }
            except Exception as e:
                background_tasks[task_id] = {
                    "status": "error",
                    "progress": f"Error: {str(e)}",
                    "result": None
                }
        
        thread = threading.Thread(target=analyze_cv_async)
        thread.start()
        
        return jsonify({
            "success": True,
            "message": "CV uploaded successfully",
            "task_id": task_id,
            "chars_extracted": len(cv_text)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/task-status/<task_id>")
def task_status(task_id):
    """Check status of background task."""
    if task_id in background_tasks:
        return jsonify(background_tasks[task_id])
    else:
        return jsonify({"error": "Task not found"}), 404


@app.route("/match-jobs", methods=["POST"])
def match_jobs():
    """Match all jobs against uploaded CV."""
    # Check if CV exists
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, extracted_skills, years_experience, domain_expertise, preferred_roles, education, location_preference FROM user_cv ORDER BY id DESC LIMIT 1")
        cv_row = cursor.fetchone()
        
        if not cv_row:
            return jsonify({"error": "No CV uploaded. Please upload your CV first."}), 400
        
        # Build skills summary
        skills_data = {
            'technical_skills': json.loads(cv_row[1]) if cv_row[1] else [],
            'years_experience': cv_row[2],
            'domain_expertise': json.loads(cv_row[3]) if cv_row[3] else [],
            'preferred_roles': json.loads(cv_row[4]) if cv_row[4] else [],
            'education': cv_row[5] or '',
            'location_preference': json.loads(cv_row[6]) if cv_row[6] else []
        }
    
    # Start background matching task
    task_id = f"job_matching_{int(time.time())}"
    background_tasks[task_id] = {
        "status": "running",
        "progress": "Starting job matching...",
        "matched": 0,
        "total": 0,
        "high_matches": 0
    }
    
    def match_jobs_async():
        try:
            jobs = get_jobs(limit=1000, offset=0)
            background_tasks[task_id]["total"] = len(jobs)
            
            matched = 0
            high_matches = 0
            
            for i, job in enumerate(jobs, 1):
                try:
                    score, reasoning = match_job_to_cv(
                        cv_summary=skills_data,
                        job_title=job.get('title', '')[:100],
                        job_description=job.get('description', '')[:1000],
                        job_location=job.get('location_city', '')
                    )
                    
                    # Update database
                    with get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE jobs 
                            SET ai_match_score = ?, match_reasoning = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        """, (score, reasoning, job['id']))
                        conn.commit()
                    
                    matched += 1
                    if score >= 70:
                        high_matches += 1
                    
                    # Update progress
                    background_tasks[task_id] = {
                        "status": "running",
                        "progress": f"Matched {i}/{len(jobs)} jobs...",
                        "matched": matched,
                        "total": len(jobs),
                        "high_matches": high_matches
                    }
                    
                    time.sleep(0.3)  # Rate limiting
                    
                except Exception as e:
                    print(f"Error matching job {job.get('id')}: {e}")
            
            background_tasks[task_id] = {
                "status": "complete",
                "progress": f"Complete! Matched {matched} jobs",
                "matched": matched,
                "total": len(jobs),
                "high_matches": high_matches
            }
            
        except Exception as e:
            background_tasks[task_id] = {
                "status": "error",
                "progress": f"Error: {str(e)}",
                "matched": 0,
                "total": 0,
                "high_matches": 0
            }
    
    thread = threading.Thread(target=match_jobs_async)
    thread.start()
    
    return jsonify({
        "success": True,
        "message": "Job matching started",
        "task_id": task_id
    })


@app.route("/run-scraper", methods=["POST"])
def run_scraper():
    """Trigger job scraping from official sites."""
    task_id = f"scraping_{int(time.time())}"
    background_tasks[task_id] = {
        "status": "running",
        "progress": "Starting scraper...",
        "jobs_found": 0
    }
    
    def scrape_async():
        import subprocess
        try:
            result = subprocess.run(
                [sys.executable, "tools/scrape_final.py"],
                capture_output=True,
                text=True,
                timeout=600,
                cwd=os.path.dirname(__file__),
                env={**os.environ, "USE_LOCAL_DB": "true", "PYTHONPATH": os.path.dirname(__file__)}
            )
            
            if result.returncode == 0:
                background_tasks[task_id] = {
                    "status": "complete",
                    "progress": "Scraping complete!",
                    "output": result.stdout[-500:]  # Last 500 chars
                }
            else:
                background_tasks[task_id] = {
                    "status": "error",
                    "progress": "Scraping failed",
                    "output": result.stderr[-500:]
                }
        except Exception as e:
            background_tasks[task_id] = {
                "status": "error",
                "progress": f"Error: {str(e)}",
                "output": ""
            }
    
    thread = threading.Thread(target=scrape_async)
    thread.start()
    
    return jsonify({
        "success": True,
        "message": "Scraping started",
        "task_id": task_id
    })


if __name__ == "__main__":
    # Set environment for local database
    os.environ["USE_LOCAL_DB"] = "true"
    
    # Initialize database
    init_db()
    
    # Start background scheduler for daily scraping
    try:
        from tools.scheduler import start_scheduler
        start_scheduler()
        print("✓ Background scheduler started (daily jobs at 2 AM)")
    except Exception as e:
        print(f"⚠️  Scheduler not started: {e}")
        print("  (You can still use manual scraping)")
    
    print("\nStarting Job Scraper Web App...")
    print("Open http://localhost:5001 in your browser")
    print("\nFeatures:")
    print("  • Upload CV once - stored permanently")
    print("  • Daily auto-scraping at 2 AM")
    print("  • New jobs auto-matched with your CV")
    print("  • Just open website to see best matches!")
    
    app.run(debug=True, host="0.0.0.0", port=5001)
