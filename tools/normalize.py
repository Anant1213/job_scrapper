# tools/normalize.py
import hashlib, re

# target: India + core cities; remote accepted only if it says India or a target city
TARGET_CITIES = {
    "mumbai","bengaluru","bangalore","hyderabad",
    "pune","chennai","gurugram","gurgaon","noida"
}
EXP_RE = re.compile(r'(\d+)\s*(?:\+|\-)?\s*(?:years?|yrs?)', re.I)

def _hash_key(*parts):
    return hashlib.md5("|".join([p or "" for p in parts]).encode("utf-8")).hexdigest()

def infer_exp_range(text: str | None):
    if not text: return (None, None)
    years = [int(m.group(1)) for m in EXP_RE.finditer(text)]
    if not years: return (None, None)
    return (min(years), max(years))

def india_location_ok(loc: str | None) -> bool:
    """pass only if we can see India or one of the target cities explicitly"""
    if not loc:
        return False
    s = loc.lower()
    if "india" in s:
        return True
    return any(c in s for c in TARGET_CITIES)

def normalize_job(
    company_id: int,
    title: str,
    apply_url: str,
    location: str | None,
    description: str | None,
    req_id: str | None = None,
    posted_at: str | None = None
):
    remote = None
    if location and ("remote" in location.lower() or "work from home" in location.lower()):
        # accept remote only if text still mentions India/city somewhere
        remote = True if india_location_ok(location) else None

    min_exp, max_exp = infer_exp_range(f"{title or ''} {description or ''}")
    canonical_key = req_id or _hash_key(title, apply_url, location or "")

    return canonical_key, {
        "company_id": company_id,
        "req_id": req_id,
        "title": title,
        "team": None,
        "location_city": location,
        "location_country": "India" if india_location_ok(location) else None,
        "remote": remote,
        "posted_at": posted_at,
        "apply_url": apply_url,
        "description": description,
        "seniority": None,
        "min_exp": min_exp,
        "max_exp": max_exp,
        "ctc_predicted_pass": None
    }
