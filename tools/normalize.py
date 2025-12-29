# tools/normalize.py
"""
Job data normalization utilities.
Handles location validation, date parsing, and canonical key generation.
"""
import re
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from typing import Optional, Tuple

# Indian cities (no duplicates)
CITIES = [
    # Major metros
    "Mumbai", "Bombay", "Navi Mumbai", "Thane",
    "Bengaluru", "Bangalore",
    "Pune", "Hyderabad", "Chennai", "Kolkata",
    # NCR region
    "New Delhi", "Delhi", "NCR", "Gurugram", "Gurgaon", "Noida", "Greater Noida",
    # Other major cities
    "Ahmedabad", "Vadodara", "Surat", "Indore", "Jaipur", "Nagpur",
    "Mysuru", "Mysore", "Kochi", "Cochin", "Coimbatore",
    "Trivandrum", "Thiruvananthapuram", "Visakhapatnam", "Vizag",
    "Gandhinagar", "Mohali",
]

# Indian states
STATES = [
    "Maharashtra", "Karnataka", "Telangana", "Tamil Nadu", "Uttar Pradesh",
    "Haryana", "West Bengal", "Delhi", "Gujarat", "Kerala", "Rajasthan",
    "Andhra Pradesh", "Madhya Pradesh", "Punjab", "Odisha", "Bihar"
]

# Common phrases indicating India location
INDIA_PHRASES = [
    "india", "pan india", "remote - india", "remote—india", "remote/india",
    "remote in india", "across india", "multiple locations - india",
    "multiple locations in india", "anywhere in india"
]


def _contains_word(hay: str, word: str) -> bool:
    """Check if a word exists in text as a whole word."""
    return re.search(rf"\b{re.escape(word)}\b", hay, flags=re.I) is not None


def _apply_url_implies_india(apply_url: Optional[str]) -> bool:
    """Check if apply URL contains India-related filters."""
    if not apply_url:
        return False
    
    try:
        u = urlparse(apply_url)
        qs = parse_qs(u.query)
        
        # Check query parameters for India
        for k, vals in qs.items():
            for v in vals:
                if "india" in str(v).lower():
                    return True
        
        # Check path for India
        if _contains_word(u.path, "India"):
            return True
    except Exception:
        pass
    
    return False


def india_location_ok(loc: Optional[str], apply_url: Optional[str] = None) -> bool:
    """
    Check if a location string indicates an India-based job.
    
    Args:
        loc: Location string from job posting
        apply_url: Optional apply URL to check for India filters
        
    Returns:
        True if location appears to be in India
    """
    if loc:
        s = loc.strip()
        sl = s.lower()

        # Check explicit India markers/phrases
        for phrase in INDIA_PHRASES:
            if phrase in sl:
                return True

        # Check ISO-ish / country codes
        # e.g., "IN-Mumbai", "Mumbai, IN", "Bengaluru, IND"
        if sl.startswith("in-") or _contains_word(s, "IN") or _contains_word(s, "IND"):
            return True

        # Check cities and states
        for w in CITIES + STATES:
            if _contains_word(s, w):
                return True

    # Fallback: check if apply_url reveals India filter
    if _apply_url_implies_india(apply_url):
        return True

    return False


def normalize_job(
    company_id: int,
    title: Optional[str],
    apply_url: Optional[str],
    location: Optional[str],
    description: Optional[str],
    req_id: Optional[str],
    posted_at: Optional[str]
) -> Tuple[str, dict]:
    """
    Normalize job data for database insertion.
    
    Returns:
        Tuple of (canonical_key, record_dict)
    """
    title = (title or "").strip()
    location = (location or "").strip() or None
    req_id = (req_id or "").strip() or None
    canonical = f"{title}::{location or ''}::{req_id or ''}".lower().strip()

    # Parse posted date (try multiple formats)
    posted = None
    if posted_at:
        date_formats = (
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%d %b %Y",
            "%b %d, %Y",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%S.%f%z",
        )
        for fmt in date_formats:
            try:
                posted = datetime.strptime(str(posted_at), fmt)
                break
            except (ValueError, TypeError):
                continue

    # Decide India country flag using both location text and apply_url hints
    is_india = india_location_ok(location, apply_url)

    record = {
        "company_id": company_id,
        "title": title[:255] if title else None,
        "apply_url": apply_url,
        "team": None,
        "location_city": location,  # Keep whatever the site shows
        "location_country": "India" if is_india else None,
        "description": description or None,
        "req_id": req_id,
        "posted_at": posted.isoformat() if posted else None,
        "canonical_key": canonical[:255] if canonical else None,
    }
    
    return canonical, record
