# tools/normalize.py
import hashlib, re, datetime as dt

ALLOWED_CITIES = {"mumbai","bengaluru","bangalore","hyderabad","pune","chennai","gurugram","india","remote"}
EXP_RE = re.compile(r'(\d+)\s*(?:\+|\-)?\s*(?:years?|yrs?)', re.I)

def _hash_key(*parts):
    plain = "|".join([p or "" for p in parts])
    return hashlib.md5(plain.encode("utf-8")).hexdigest()

def infer_exp_range(text: str | None):
    if not text: return (None, None)
    years = [int(m.group(1)) for m in EXP_RE.finditer(text)]
    if not years: return (None, None)
    return (min(years), max(years))

def city_passes(loc: str | None):
    if not loc: return True  # unknown -> keep for now
    s = loc.lower()
    return any(c in s for c in ALLOWED_CITIES)

def normalize_job(company_id: int, title: str, apply_url: str, location: str | None, description: str | None, req_id: str | None = None, posted_at: str | None = None):
    # infer remote flag
    remote = None
    if location:
        ls = location.lower()
        if "remote" in ls or "work from home" in ls:
            remote = True
    min_exp, max_exp = infer_exp_range((title or "") + " " + (description or ""))
    canonical_key = req_id or _hash_key(title, apply_url, location)
    rec = {
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
        "ctc_predicted_pass": None,  # set in Phase 2
        "last_seen_at": dt.datetime.utcnow().isoformat()
    }
    return canonical_key, rec
