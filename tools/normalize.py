import hashlib, re

ALLOWED_CITIES = {
    "mumbai","bengaluru","bangalore","hyderabad","pune","chennai",
    "gurugram","gurgaon","noida","india","remote"
}
EXP_RE = re.compile(r'(\d+)\s*(?:\+|\-)?\s*(?:years?|yrs?)', re.I)

def _hash_key(*parts):
    return hashlib.md5("|".join([p or "" for p in parts]).encode("utf-8")).hexdigest()

def infer_exp_range(text: str | None):
    if not text: return (None, None)
    years = [int(m.group(1)) for m in EXP_RE.finditer(text)]
    if not years: return (None, None)
    return (min(years), max(years))

def city_passes(loc: str | None):
    if not loc: return True
    s = loc.lower()
    return any(c in s for c in ALLOWED_CITIES)

def normalize_job(company_id: int, title: str, apply_url: str, location: str | None, description: str | None, req_id: str | None = None, posted_at: str | None = None):
    remote = None
    if location and ("remote" in location.lower() or "work from home" in location.lower()):
        remote = True
    min_exp, max_exp = infer_exp_range(f"{title or ''} {description or ''}")
    canonical_key = req_id or _hash_key(title, apply_url, location)
    return canonical_key, {
        "company_id": company_id,
        "req_id": req_id,
        "title": title,
        "team": None,
        "location_city": location,
        "location_country": "India" if location and "india" in location.lower() else None,
        "remote": remote,
        "posted_at": posted_at,
        "apply_url": apply_url,
        "description": description,
        "seniority": None,
        "min_exp": min_exp,
        "max_exp": max_exp,
        "ctc_predicted_pass": None
    }
