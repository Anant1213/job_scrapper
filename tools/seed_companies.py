# tools/seed_companies.py
import csv, os, sys, json, time
import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_ROLE = os.environ["SUPABASE_SERVICE_ROLE"]

CSV_PATH = "config/firms.csv"

HEADERS = {
    "apikey": SERVICE_ROLE,
    "Authorization": f"Bearer {SERVICE_ROLE}",
    "Content-Type": "application/json",
    "Accept-Profile": "public",
}

def upsert_company(row):
    url = f"{SUPABASE_URL}/rest/v1/companies?on_conflict=name"
    payload = {
        "name": row["company"].strip(),
        "careers_url": row.get("careers_url") or None,
        "ats_type": (row.get("ats_type") or None),
        "active": row.get("active", "true").lower() == "true",
        "comp_gate_status": (row.get("comp_gate_status") or "pass"),
    }
    r = requests.post(url, headers=HEADERS, data=json.dumps([payload]), params={"select":"id,name"})
    if r.status_code not in (200, 201, 204):
        raise RuntimeError(f"Upsert failed for {payload['name']}: {r.status_code} {r.text}")
    return r.json()[0]["name"] if r.content else payload["name"]

def main():
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        names = []
        for row in reader:
            if (row.get("active","true").lower() != "true"):
                continue
            name = upsert_company(row)
            names.append(name)
            time.sleep(0.2)  # polite
    print(f"Seeded/updated {len(names)} companies:\n - " + "\n - ".join(names))

if __name__ == "__main__":
    try:
        main()
    except KeyError as e:
        print(f"Missing env var: {e}. Did you set GitHub secrets SUPABASE_URL and SUPABASE_SERVICE_ROLE?")
        sys.exit(1)
