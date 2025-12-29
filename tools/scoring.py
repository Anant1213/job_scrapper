# tools/scoring.py
"""
Job relevance scoring engine.
Scores jobs 0-100 based on title match, skills, experience, location, and recency.
"""
import re
import math
import datetime as dt
from typing import Optional, Tuple

# Title patterns with weights (higher = more relevant)
TITLE_WEIGHTS = [
    (r"\bquant(itative)?\b", 25),
    (r"machine learning|ml engineer", 22),
    (r"\bdata scientist\b", 20),
    (r"\bdata engineer\b|data platform", 18),
    (r"risk (analytics|model|management)|model validation|model risk", 16),
    (r"analytics|business intelligence|bi ", 12),
]

# Skill tokens to look for in job descriptions
SKILL_TOKENS = [
    "python", "pandas", "numpy", "scikit", "sklearn", "pytorch", "tensorflow", "sql",
    "pyspark", "spark", "aws", "gcp", "azure", "airflow", "kafka", "hive", "time series",
    "feature engineering", "nlp", "llm", "xgboost", "catboost", "statistics",
    "probability", "regression", "classification", "clustering"
]


def _match_weight(title: str) -> int:
    """Calculate title match score based on keywords."""
    t = (title or "").lower()
    score = 0
    for pat, w in TITLE_WEIGHTS:
        if re.search(pat, t, re.I):
            score = max(score, w)
    return min(score, 25)


def _skills_score(desc: str) -> int:
    """Calculate skills score based on keywords in description."""
    d = (desc or "").lower()
    hits = sum(1 for s in SKILL_TOKENS if s in d)
    return min(30, int(8 * math.log2(1 + hits)))


def _exp_score(min_exp: Optional[int], max_exp: Optional[int]) -> int:
    """
    Calculate experience score.
    Best score for roles overlapping with 1-3 years experience.
    """
    if min_exp is None and max_exp is None:
        return 6  # Unknown experience, neutral score
    
    lo = min_exp if isinstance(min_exp, int) else 0
    hi = max_exp if isinstance(max_exp, int) else 99
    
    # Perfect match: role requires 1-3 years
    if lo <= 3 and hi >= 1:
        return 10
    # Acceptable: role requires 0-4 years with some overlap
    if lo <= 4 and hi >= 0:
        return 6
    return 0


def _geo_score(location: str, remote: Optional[bool]) -> int:
    """Calculate location score based on city preference."""
    if remote:
        return 20
    if not location:
        return 8
    
    s = location.lower()
    
    # Top tier cities (most preferred)
    top = ["mumbai", "bengaluru", "bangalore", "hyderabad"]
    if any(k in s for k in top):
        return 18
    
    # Second tier cities
    others = ["pune", "chennai", "gurugram", "gurgaon", "noida", "india"]
    if any(k in s for k in others):
        return 14
    
    return 4


def _recency_score(posted_at: Optional[str]) -> int:
    """Calculate recency score based on posting date."""
    if not posted_at:
        return 5
    
    try:
        # Parse date from ISO format
        y, m, d = [int(x) for x in posted_at[:10].split("-")]
        days = (dt.date.today() - dt.date(y, m, d)).days
    except (ValueError, AttributeError, IndexError):
        return 5
    
    if days <= 7:
        return 10
    if days <= 14:
        return 8
    if days <= 30:
        return 6
    return 2


def _seniority_penalty(title: str) -> int:
    """Calculate penalty for senior/manager roles or intern positions."""
    t = (title or "").lower()
    
    # Penalize senior roles
    senior_keywords = ["manager", "lead", "vp", "director", "principal", "head", "senior"]
    if any(x in t for x in senior_keywords):
        return 8
    
    # Penalize intern/trainee positions
    if "intern" in t or "trainee" in t:
        return 12
    
    return 0


def score_job(job: dict) -> Tuple[int, bool]:
    """
    Calculate overall relevance score for a job.
    
    Returns:
        Tuple of (score, ctc_predicted_pass)
        - score: 0-100 relevance score
        - ctc_predicted_pass: True if company likely pays well
    """
    title = job.get("title")
    desc = job.get("description") or ""
    location = job.get("location_city") or ""
    remote = job.get("remote")
    min_exp = job.get("min_exp")
    max_exp = job.get("max_exp")

    # Calculate component scores
    s = 0
    s += _match_weight(title)
    s += _skills_score(desc)
    s += _exp_score(min_exp, max_exp)
    s += _geo_score(location, remote)
    s += _recency_score(job.get("posted_at"))
    s -= _seniority_penalty(title)

    # Check compensation gate based on company status
    companies = job.get("companies") or {}
    comp_status = companies.get("comp_gate_status") or "probation"
    title_lower = (title or "").lower()
    ctc_predicted_pass = (comp_status == "pass") and ("intern" not in title_lower)

    return max(0, min(100, int(s))), ctc_predicted_pass
