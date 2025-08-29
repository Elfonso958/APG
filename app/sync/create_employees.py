#!/usr/bin/env python3
r"""
Create/Update APG crew from an Excel file.

Default input:
  C:\Users\Jayden\Downloads\APG_Crew.xlsx

Env required (put these in a .env or set as environment variables):
  APG_BASE=https://fly.rocketroute.com/api
  APG_APP_KEY=<YOUR_APP_KEY>
  APG_API_VERSION=1.17
  APG_EMAIL=<api-user@email>
  APG_PASSWORD=<password>

Run:
  python create_employees.py
  python create_employees.py --dry-run
  python create_employees.py --file "C:\Users\Jayden\Downloads\APG_Crew.xlsx" --sheet "Sheet1"
"""

import os
import sys
import argparse
from typing import Dict, Any, Optional, Tuple, List

import pandas as pd
import requests

# --- Load .env if present ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---- APG config ----
APG_BASE = os.getenv("APG_BASE", "https://fly.rocketroute.com/api").rstrip("/")
APG_APP_KEY = os.getenv("APG_APP_KEY", "")
APG_API_VERSION = os.getenv("APG_API_VERSION", "1.17")
APG_EMAIL = os.getenv("APG_EMAIL", "")
APG_PASSWORD = os.getenv("APG_PASSWORD", "")

REPORT_FILE = "apg_crew_import_report.csv"

def die(msg: str, code: int = 2):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)

def apg_headers(bearer: str) -> Dict[str, str]:
    return {
        "Authorization": bearer,
        "X-API-Version": APG_API_VERSION,
        "Content-Type": "application/json",
        "User-Agent": "APG-BulkCrew/1.0",
    }

def apg_login(email: str, password: str) -> Dict[str, str]:
    if not APG_BASE or not APG_APP_KEY or not email or not password:
        die("Missing APG_BASE / APG_APP_KEY / APG_EMAIL / APG_PASSWORD")
    url = f"{APG_BASE}/login"
    headers = {
        "Authorization": f"AppKey {APG_APP_KEY}",
        "X-API-Version": APG_API_VERSION,
        "Content-Type": "application/json",
    }
    payload = {"email": email.strip(), "password": password.strip()}
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if not r.ok:
        die(f"APG login failed HTTP {r.status_code}: {r.text[:400]}")
    data = r.json()
    status = data.get("status", {})
    if not status.get("success"):
        die(f"APG login error: {status.get('message')}")
    bearer = r.headers.get("Authorization")
    if bearer and bearer.startswith("Bearer "):
        tok = bearer
    else:
        tok_val = (data.get("data") or {}).get("access_token")
        if not tok_val:
            die("APG login success but no access_token or Authorization header")
        tok = f"Bearer {tok_val}"
    refresh = (data.get("data") or {}).get("refresh_token", "")
    return {"authorization": tok, "refresh_token": refresh}

def try_api_then_legacy(url_api: str, url_legacy: str, method: str, headers: dict, json_payload: dict):
    if method == "POST":
        r = requests.post(url_api, headers=headers, json=json_payload, timeout=60)
        if not r.ok:
            r2 = requests.post(url_legacy, headers=headers, json=json_payload, timeout=60)
            r2.raise_for_status()
            return r2
        r.raise_for_status()
        return r
    elif method == "GET":
        r = requests.get(url_api, headers=headers, timeout=60)
        if not r.ok:
            r2 = requests.get(url_legacy, headers=headers, timeout=60)
            r2.raise_for_status()
            return r2
        r.raise_for_status()
        return r
    else:
        raise ValueError("Unsupported method")

def apg_get_crew_list(bearer: str) -> List[dict]:
    headers = apg_headers(bearer)
    url_api = f"{APG_BASE}/api/crew/list"
    url_legacy = f"{APG_BASE}/crew/list"
    try:
        r = try_api_then_legacy(url_api, url_legacy, "POST", headers, json_payload={})
    except requests.HTTPError:
        r = try_api_then_legacy(url_api, url_legacy, "GET", headers, json_payload={})
    data = r.json()
    if isinstance(data, dict) and "data" in data:
        return data.get("data") or []
    if isinstance(data, list):
        return data
    die(f"Unexpected crew/list response: {str(data)[:300]}")
    return []

def apg_crew_edit(bearer: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    headers = apg_headers(bearer)
    url_api = f"{APG_BASE}/api/crew/list"
    url_legacy = f"{APG_BASE}/crew/list"
    r = try_api_then_legacy(url_api, url_legacy, "POST", headers, json_payload=payload)

    # Defensive parsing: some tenants return a list instead of dict
    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"/crew/list returned non-JSON: {r.text[:400]}")

    # Normalise list → dict if possible
    if isinstance(data, list):
        # Common variants: [ { "status": {...}, "data": {...} } ] or [ {..crew..} ]
        if data and isinstance(data[0], dict):
            # If there's a 'status' key inside first element, treat that as the envelope
            if "status" in data[0]:
                data = data[0]
            else:
                # Wrap as success with data
                data = {"status": {"success": True, "message": "OK"}, "data": data[0]}
        else:
            raise RuntimeError(f"Unexpected /crew/edit response list shape: {str(data)[:300]}")

    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected /crew/edit response type: {type(data).__name__} -> {str(data)[:300]}")

    status = data.get("status", {})
    if not status.get("success", False):
        raise RuntimeError(f"APG crew/edit error: {status.get('message', 'unknown')} | {str(data)[:300]}")
    return data


def pick(df: pd.DataFrame, opts: list[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for o in opts:
        if o.lower() in cols:
            return cols[o.lower()]
    return None

def norm_str(x) -> str:
    if pd.isna(x) or x is None:
        return ""
    return str(x).strip()

def parse_cert_list(val) -> Optional[List[int]]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    parts = [p.strip() for p in s.split(",")]
    out: List[int] = []
    for p in parts:
        if not p:
            continue
        try:
            out.append(int(p))
        except Exception:
            pass
    return out or None

def build_payload_from_row(row: pd.Series, cols: dict, existing_by_code: dict) -> Tuple[Dict[str, Any], str]:
    crew_code = norm_str(row[cols["crew_code"]])
    code_key = crew_code.upper()  # normalize
    fname = norm_str(row[cols["first_name"]])
    lname = norm_str(row[cols["last_name"]])
    email = norm_str(row[cols["email"]]) if cols.get("email") else ""
    mobile = norm_str(row[cols["phone"]]) if cols.get("phone") else ""
    role = norm_str(row[cols["role"]]) if cols.get("role") else "crew"
    certs = parse_cert_list(row[cols["certified_for"]]) if cols.get("certified_for") else None

    payload: Dict[str, Any] = {
        "crew_code": crew_code,
        "fname": fname,
        "lname": lname,
        "role": role or "crew",
    }
    if email:  payload["email"]  = email
    if mobile: payload["mobile"] = mobile
    if certs:  payload["certified_for"] = certs

    # FIX: look up using uppercased key (matches how we store the map)
    if code_key in existing_by_code and existing_by_code[code_key].get("id"):
        try:
            payload["id"] = int(existing_by_code[code_key]["id"])
        except Exception:
            pass

    return payload, crew_code

def main():
    p = argparse.ArgumentParser(description="Create/Update APG crew from an Excel file")
    # FIX: correct quoting + raw string
    p.add_argument("--file", default=r"C:\Users\Jayden\Downloads\APG_Crew.xlsx", help="Path to Excel")
    p.add_argument("--sheet", default=None, help="Sheet name (optional)")
    p.add_argument("--dry-run", action="store_true", help="Do not call APG; preview only")
    args = p.parse_args()

    xfile = args.file
    if not os.path.exists(xfile):
        die(f"Input file not found: {xfile}")

    df = pd.read_excel(xfile, sheet_name=args.sheet) if args.sheet else pd.read_excel(xfile)
    if df.empty:
        die("Excel file is empty")

    cols = {
        "crew_code":     pick(df, ["EmployeeNo", "employeeNo", "Employee No", "Emp No", "CrewCode", "crew_code", "Code"]),
        "first_name":    pick(df, ["FirstName", "First Name", "GivenName", "Given Name", "fname", "first_name"]),
        "last_name":     pick(df, ["Surname", "LastName", "Last Name", "FamilyName", "lname", "last_name"]),
        "email":         pick(df, ["Email", "E-mail", "email"]),
        "phone":         pick(df, ["Phone", "MobilePhoneNumber", "Mobile", "mobile", "phone"]),
        "role":          pick(df, ["Role", "role", "Rank", "JobTitle", "Position"]),
        "certified_for": pick(df, ["certified_for", "CertifiedFor", "Certified For", "MasterAircraftIDs", "AircraftIDs"]),
    }

    missing = [k for k in ("crew_code","first_name","last_name") if not cols.get(k)]
    if missing:
        die(f"Missing required columns in Excel: {', '.join(missing)}")

    bearer = None
    existing_by_code: Dict[str, dict] = {}
    if not args.dry_run:
        tokens = apg_login(APG_EMAIL, APG_PASSWORD)
        bearer = tokens["authorization"]
        existing = apg_get_crew_list(bearer)
        for c in existing:
            code = norm_str(c.get("crew_code")).upper()
            if code:
                existing_by_code[code] = {"id": c.get("id"), "crew_code": code}

    created = updated = skipped = failed = 0
    out_rows: List[dict] = []

    for i, r in df.iterrows():
        try:
            payload, crew_code = build_payload_from_row(r, cols, existing_by_code)
            action = "update" if "id" in payload else "create"

            # live progress
            print(f"[{i+1}/{len(df)}] {payload.get('crew_code','')}: {payload.get('fname','')} {payload.get('lname','')} → {action}")

            if args.dry_run:
                out_rows.append({
                    "row": int(i)+1,
                    "crew_code": payload.get("crew_code"),
                    "fname": payload.get("fname"),
                    "lname": payload.get("lname"),
                    "email": payload.get("email",""),
                    "mobile": payload.get("mobile",""),
                    "role": payload.get("role",""),
                    "certified_for": ",".join(map(str, payload.get("certified_for", []))) if isinstance(payload.get("certified_for"), list) else "",
                    "action": action,
                    "status": "DRY-RUN",
                    "apg_id": existing_by_code.get((crew_code or "").upper(),{}).get("id","") if action=="update" else "",
                    "message": "",
                })
                skipped += 1
                continue

            res = apg_crew_edit(bearer, payload)
            ret = (res.get("data") or {})
            apg_id = ret.get("id") or existing_by_code.get((crew_code or "").upper(),{}).get("id")

            if action == "create":
                created += 1
            else:
                updated += 1

            out_rows.append({
                "row": int(i)+1,
                "crew_code": payload.get("crew_code"),
                "fname": payload.get("fname"),
                "lname": payload.get("lname"),
                "email": payload.get("email",""),
                "mobile": payload.get("mobile",""),
                "role": payload.get("role",""),
                "certified_for": ",".join(map(str, payload.get("certified_for", []))) if isinstance(payload.get("certified_for"), list) else "",
                "action": action,
                "status": "OK",
                "apg_id": apg_id,
                "message": "",
            })

            if crew_code:
                existing_by_code[(crew_code or "").upper()] = {"id": apg_id, "crew_code": (crew_code or "").upper()}

        except Exception as e:
            failed += 1
            out_rows.append({
                "row": int(i)+1,
                "crew_code": norm_str(r.get(cols["crew_code"], "")),
                "fname": norm_str(r.get(cols["first_name"], "")),
                "lname": norm_str(r.get(cols["last_name"], "")),
                "email": norm_str(r.get(cols["email"], "")) if cols.get("email") else "",
                "mobile": norm_str(r.get(cols["phone"], "")) if cols.get("phone") else "",
                "role": norm_str(r.get(cols["role"], "")) if cols.get("role") else "",
                "certified_for": norm_str(r.get(cols["certified_for"], "")) if cols.get("certified_for") else "",
                "action": "",
                "status": "ERROR",
                "apg_id": "",
                "message": str(e)[:300],
            })

    pd.DataFrame(out_rows, columns=[
        "row","crew_code","fname","lname","email","mobile","role","certified_for",
        "action","status","apg_id","message"
    ]).to_csv(REPORT_FILE, index=False, encoding="utf-8-sig")

    print(f"\nDone. Created={created} Updated={updated} Skipped(DRY)={skipped} Failed={failed}")
    print(f"Report written: {REPORT_FILE}")
    if args.dry_run:
        print("This was a DRY RUN. Remove --dry-run to perform the import.")

if __name__ == "__main__":
    main()
