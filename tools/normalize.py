# tools/normalize.py
import re
from datetime import datetime
from urllib.parse import urlparse, parse_qs

CITIES = [
    # big metros + common spellings
    "Mumbai","Bombay","Navi Mumbai","Thane",
    "Bengaluru","Bangalore",
    "Pune","Hyderabad","Chennai","Kolkata","New Delhi","Delhi","NCR","Gurugram","Gurgaon","Noida","Greater Noida",
    "Ahmedabad","Vadodara","Surat","Indore","Jaipur","Nagpur","Mysuru","Mysore","Kochi","Cochin","Coimbatore",
    "Trivandrum","Thiruvananthapuram","Visakhapatnam","Vizag","Gandhinagar","Mohali","Gurgaon"
]
STATES = [
    "Maharashtra","Karnataka","Telangana","Tamil Nadu","Uttar Pradesh",
    "Haryana","West Bengal","Delhi","Gujarat","Kerala","Rajasthan",
    "Andhra Pradesh","Madhya Pradesh","Punjab","Odisha","Bihar"
]
INDIA_PHRASES = [
    "india","pan india","remote - india","remote—india","remote/india","remote in india","across india",
    "multiple locations - india","multiple locations in india","anywhere in india"
]

# quick helpers
def _contains_word(hay, word):
    return re.search(rf"\b{re.escape(word)}\b", hay, flags=re.I) is not None

def _apply_url_implies_india(apply_url: str | None) -> bool:
    if not apply_url:
        return False
    try:
        u = urlparse(apply_url)
        qs = parse_qs(u.query)
        # explicit query filters
        for k, vals in qs.items():
            for v in vals:
                if "india" in str(v).lower():
                    return True
        # path hints
        if _contains_word(u.path, "India"):
            return True
    except Exception:
        pass
    return False

def india_location_ok(loc: str | None, apply_url: str | None = None) -> bool:
    if loc:
        s = loc.strip()
        sl = s.lower()

        # explicit India markers / phrases
        for phrase in INDIA_PHRASES:
            if phrase in sl:
                return True

        # ISO-ish / country codes
        # e.g., "IN-Mumbai", "Mumbai, IN", "Bengaluru, IND"
        if sl.startswith("in-") or _contains_word(s, "IN") or _contains_word(s, "IND"):
            return True

        # cities and states
        for w in CITIES + STATES:
            if _contains_word(s, w):
                return True

    # fallback: apply_url reveals India filter (e.g., ?location=India or /India/)
    if _apply_url_implies_india(apply_url):
        return True

    return False

def normalize_job(company_id, title, apply_url, location, description, req_id, posted_at):
    title = (title or "").strip()
    location = (location or "").strip() or None
    req_id = (req_id or "").strip() or None
    canonical = f"{title}::{location or ''}::{req_id or ''}".lower().strip()

    # parse posted date (loose)
    posted = None
    if posted_at:
        for fmt in (
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%d %b %Y",
            "%b %d, %Y",
        ):
            try:
                posted = datetime.strptime(str(posted_at), fmt)
                break
            except Exception:
                continue

    # decide India country flag using both location text and apply_url hints
    is_india = india_location_ok(location, apply_url)

    record = {
        "company_id": company_id,
        "title": title[:255] if title else None,
        "apply_url": apply_url,
        "team": None,
        "location_city": location,                  # keep whatever the site shows (may be city/state/India)
        "location_country": "India" if is_india else None,
        "description": (description or None),
        "req_id": req_id,
        "posted_at": posted.isoformat() if posted else None,
        "canonical_key": canonical[:255] if canonical else None,
    }
    return canonical, record
