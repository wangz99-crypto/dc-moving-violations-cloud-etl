import os
import math
import requests
import pandas as pd
from sqlalchemy import create_engine, text

# Optional: load .env locally
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


def make_engine():
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "3306")
    db   = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    pwd  = os.getenv("DB_PASSWORD")

    missing = [k for k, v in {
        "DB_HOST": host, "DB_NAME": db, "DB_USER": user, "DB_PASSWORD": pwd
    }.items() if not v]
    if missing:
        raise ValueError(f"Missing env vars: {', '.join(missing)}")

    url = f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url)


ENGINE = make_engine()

URL_2024 = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2024/MapServer"
URL_2025 = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2025/MapServer"

LAYER_2024 = {"2024-09": 8, "2024-10": 9, "2024-11": 10, "2024-12": 11}
LAYER_2025 = {"2025-01": 0, "2025-02": 1, "2025-03": 2, "2025-04": 3, "2025-05": 4, "2025-06": 5,
              "2025-07": 6, "2025-08": 7, "2025-09": 8, "2025-10": 9, "2025-11": 10, "2025-12": 11}

CHUNK = 2000


def ensure_violations_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS violations (
        violation_id   VARCHAR(50) PRIMARY KEY,
        issue_date     DATETIME,
        violation_date DATE,
        issuing_agency_name VARCHAR(200),
        accident_indicator VARCHAR(10),
        location       TEXT,
        violation_code VARCHAR(50),
        violation_desc TEXT,
        fine_amount    DOUBLE,
        total_paid     DOUBLE,
        latitude       DOUBLE,
        longitude      DOUBLE,
        month          VARCHAR(7)
    );
    """
    with ENGINE.begin() as conn:
        conn.execute(text(ddl))
    print("Table `violations` is ready.")


def get_layer_url(month_key: str):
    if month_key.startswith("2024"):
        return URL_2024, LAYER_2024[month_key]
    return URL_2025, LAYER_2025[month_key]


def fetch_month(month_key: str) -> pd.DataFrame:
    base_url, layer_id = get_layer_url(month_key)
    url = f"{base_url}/{layer_id}/query"

    # First call: get count
    params_count = {
        "where": "1=1",
        "returnCountOnly": "true",
        "f": "json",
    }
    r = requests.get(url, params=params_count, timeout=60)
    r.raise_for_status()
    total = r.json().get("count", 0)

    if total == 0:
        return pd.DataFrame()

    pages = math.ceil(total / CHUNK)
    rows = []

    for i in range(pages):
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "json",
            "resultOffset": i * CHUNK,
            "resultRecordCount": CHUNK,
        }
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        feats = resp.json().get("features", [])

        for f in feats:
            a = f.get("attributes", {})
            rows.append({
                "violation_id": str(a.get("violation_id") or a.get("VIOLATION_ID") or ""),
                "issue_date": a.get("issue_date") or a.get("ISSUE_DATE"),
                "violation_date": a.get("violation_date") or a.get("VIOLATION_DATE"),
                "issuing_agency_name": a.get("issuing_agency_name") or a.get("ISSUING_AGENCY_NAME"),
                "accident_indicator": a.get("accident_indicator") or a.get("ACCIDENT_INDICATOR"),
                "location": a.get("location") or a.get("LOCATION"),
                "violation_code": a.get("violation_code") or a.get("VIOLATION_CODE"),
                "violation_desc": a.get("violation_desc") or a.get("VIOLATION_DESC"),
                "fine_amount": a.get("fine_amount") or a.get("FINE_AMOUNT"),
                "total_paid": a.get("total_paid") or a.get("TOTAL_PAID"),
                "latitude": a.get("latitude") or a.get("LATITUDE"),
                "longitude": a.get("longitude") or a.get("LONGITUDE"),
                "month": month_key,
            })

    df = pd.DataFrame(rows)
    return df[df["violation_id"].astype(str).str.len() > 0].copy()


def upsert_violations(df: pd.DataFrame):
    if df.empty:
        return 0

    insert_sql = """
    INSERT INTO violations
    (violation_id, issue_date, violation_date, issuing_agency_name, accident_indicator,
     location, violation_code, violation_desc, fine_amount, total_paid, latitude, longitude, month)
    VALUES
    (:violation_id, :issue_date, :violation_date, :issuing_agency_name, :accident_indicator,
     :location, :violation_code, :violation_desc, :fine_amount, :total_paid, :latitude, :longitude, :month)
    ON DUPLICATE KEY UPDATE
      issue_date=VALUES(issue_date),
      violation_date=VALUES(violation_date),
      issuing_agency_name=VALUES(issuing_agency_name),
      accident_indicator=VALUES(accident_indicator),
      location=VALUES(location),
      violation_code=VALUES(violation_code),
      violation_desc=VALUES(violation_desc),
      fine_amount=VALUES(fine_amount),
      total_paid=VALUES(total_paid),
      latitude=VALUES(latitude),
      longitude=VALUES(longitude),
      month=VALUES(month);
    """
    with ENGINE.begin() as conn:
        conn.execute(text(insert_sql), df.to_dict(orient="records"))
    return len(df)


def main():
    ensure_violations_table()

    months = list(LAYER_2024.keys()) + list(LAYER_2025.keys())
    total = 0

    for m in months:
        df = fetch_month(m)
        processed = upsert_violations(df)
        total += processed
        print(f"[{m}] rows processed: {len(df)}")

    print(f"Done. Total rows processed: {total}")


if __name__ == "__main__":
    main()