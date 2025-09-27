# tools/scoring.py
import re, math, datetime as dt

# Title weights (max one applies; tune as you like)
TITLE_WEIGHTS = [
    (r"\bquant(itative)?\b", 25),
    (r"machine learning|ml engineer", 22),
    (r"\bdata scientist\b", 20),
    (r"\bdata engineer\b|data platform", 18),
    (r"risk (analytics|model|management)|model validation|model risk", 16),
    (r"analytics|business intelligence|bi ", 12),
]

# Skills to look for inside description (tune/add/remove)
SKILL_TOKENS = [
    "python","pandas","numpy","scikit","sklearn","pytorch","tensorflow","sql",
    "pyspark","spark","aws","gcp","azure","airflow","kafka","hive","time series",
    "feature engineering","nlp","llm","xgboost","catboost","statistics",
    "probability","regression","classification","clustering"
]

def _match_weight(title: str):
    t = (title or "").lower()
    score = 0
    for pat, w in TITLE_WEIGHTS:
        if re.search(pat, t, re.I):
            score = max(score, w)
    return min(score, 25)

def _skills_score(desc: str):
    d = (desc or "").lower()
    hits = sum(1 for s in SKILL_TOKENS if s in d)
    # 0..30 with diminishing returns
    return min(30, int(8 * math.log2(1 + hits)))

def _exp_score(min_exp, max_exp):
    # prefer 1–3 yrs (your band)
    if min_exp is None and max_exp is None:
        return 6
    lo = min_exp if isinstance(min_exp, int) else 0
    hi = max_exp if isinstance(max_exp, int) else 99
    if lo <= 3 and hi >= 1:
        return 10
    if lo <= 4 and hi >= 0:
        return 6
    return 0

def _geo_score(location: str, remote: bool | None):
    if remote:
        return 20
    if not location:
        return 8
    s = location.lower()
    top = ["mumbai","bengaluru","bangalore","hyderabad"]
    others = ["pune","chennai","gurugram","gurgaon","noida","india"]
    if any(k in s for k in top):
        return 18
    if any(k in s for k in others):
        return 14
    return 4

def _recency_score(posted_at: str | None):
    if not posted_at:
        return 5
    try:
        y, m, d = [int(x) for x in posted_at[:10].split("-")]
        days = (dt.date.today() - dt.date(y, m, d)).days
    except Exception:
        return 5
    if days <= 7: return 10
    if days <= 14: return 8
    if days <= 30: return 6
    return 2

def _seniority_penalty(title: str):
    t = (title or "").lower()
    if any(x in t for x in ["manager","lead","vp","director","principal","head"]):
        return 8
    if "intern" in t or "trainee" in t:
        return 12
    return 0

def score_job(job):
    """
    job has: title, description, location_city, remote, posted_at, min_exp, max_exp,
             companies(name, comp_gate_status)
    """
    title = job.get("title")
    desc = job.get("description") or ""
    location = job.get("location_city") or ""
    remote = job.get("remote")
    min_exp = job.get("min_exp")
    max_exp = job.get("max_exp")

    s = 0
    s += _match_weight(title)                 # 0..25
    s += _skills_score(desc)                  # 0..30
    s += _exp_score(min_exp, max_exp)         # 0..10
    s += _geo_score(location, remote)         # 0..20
    s += _recency_score(job.get("posted_at")) # 0..10
    s -= _seniority_penalty(title)            # 0..12 penalty

    # simple comp gate based on firm bucket (pass/probation)
    comp_status = (job.get("companies") or {}).get("comp_gate_status") or "probation"
    ctc_predicted_pass = (comp_status == "pass") and ("intern" not in (title or "").lower())

    return max(0, min(100, int(s))), ctc_predicted_pass
