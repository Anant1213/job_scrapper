# tools/normalize.py
import re
from datetime import datetime

CITIES = [
    "Mumbai","Navi Mumbai","Bengaluru","Bangalore","Pune","Hyderabad",
    "Chennai","Noida","Gurugram","Gurgaon","Kolkata","New Delhi","Delhi",
    "Ahmedabad","Cochin","Kochi","Coimbatore","Jaipur"
]
STATES = [
    "Maharashtra","Karnataka","Telangana","Tamil Nadu","Uttar Pradesh",
    "Haryana","West Bengal","Delhi","Gujarat","Kerala"
]

def india_location_ok(loc: str | None) -> bool:
    if not loc:
        return False
    s = loc.strip()
    s_low = s.lower()
    # obvious markers
    if "india" in s_low or s_low.startswith("in-"):
        return True
    # cities and states
    for w in CITIES + STATES:
        if re.search(rf"\b{re.escape(w)}\b", s, flags=re.I):
            return True
    # common ISO-ish patterns: "IN, Mumbai", "Mumbai, IN"
    if re.search(r"\bIN\b", s):
        return True
    return False

def normalize_job(company_id, title, apply_url, location, description, req_id, posted_at):
    # canonical key combines title + location + req_id when present
    title = (title or "").strip()
    location = (location or "").strip() or None
    req_id = (req_id or "").strip() or None
    canonical = f"{title}::{location or ''}::{req_id or ''}".lower().strip()

    posted = None
    if posted_at:
        # accept common formats, but keep loose
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%d %b %Y", "%b %d, %Y"):
            try:
                posted = datetime.strptime(str(posted_at), fmt)
                break
            except Exception:
                continue

    record = {
        "company_id": company_id,
        "title": title[:255] if title else None,
        "apply_url": apply_url,
        "team": None,
        "location_city": location,
        "location_country": "India" if india_location_ok(location) else None,
        "description": (description or None),
        "req_id": req_id,
        "posted_at": posted.isoformat() if posted else None,
        "canonical_key": canonical[:255] if canonical else None,
    }
    return canonical, record
