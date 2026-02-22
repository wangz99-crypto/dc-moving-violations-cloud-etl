import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# Optional: load .env locally (recommended)
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


def ensure_weather_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS weather_daily (
        weather_date DATE PRIMARY KEY,
        tempmax      DOUBLE,
        tempmin      DOUBLE,
        temp         DOUBLE,
        precip       DOUBLE,
        humidity     DOUBLE,
        windspeed    DOUBLE,
        conditions   TEXT,
        is_rain      TINYINT
    );
    """
    with ENGINE.begin() as conn:
        conn.execute(text(ddl))
    print("Table `weather_daily` is ready.")


def create_date_ranges(start_date, end_date, chunk_days=15):
    ranges = []
    cur = start_date
    while cur <= end_date:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end_date)
        ranges.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return ranges


def fetch_weather_chunk(location, start_date, end_date, api_key):
    base_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
    url = f"{base_url}/{location}/{start_date}/{end_date}"
    params = {"unitGroup": "us", "include": "days", "key": api_key, "contentType": "json"}

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    days = data.get("days", [])
    rows = []
    for d in days:
        rows.append({
            "weather_date": d.get("datetime"),
            "tempmax": d.get("tempmax"),
            "tempmin": d.get("tempmin"),
            "temp": d.get("temp"),
            "precip": d.get("precip"),
            "humidity": d.get("humidity"),
            "windspeed": d.get("windspeed"),
            "conditions": d.get("conditions"),
            "is_rain": 1 if (d.get("precip") or 0) > 0 else 0
        })
    return pd.DataFrame(rows)


def upsert_weather(df: pd.DataFrame):
    if df.empty:
        return 0

    df["weather_date"] = pd.to_datetime(df["weather_date"]).dt.date

    # idempotent: insert-ignore by primary key
    insert_sql = """
    INSERT INTO weather_daily
    (weather_date, tempmax, tempmin, temp, precip, humidity, windspeed, conditions, is_rain)
    VALUES (:weather_date, :tempmax, :tempmin, :temp, :precip, :humidity, :windspeed, :conditions, :is_rain)
    ON DUPLICATE KEY UPDATE
      tempmax=VALUES(tempmax),
      tempmin=VALUES(tempmin),
      temp=VALUES(temp),
      precip=VALUES(precip),
      humidity=VALUES(humidity),
      windspeed=VALUES(windspeed),
      conditions=VALUES(conditions),
      is_rain=VALUES(is_rain);
    """

    with ENGINE.begin() as conn:
        conn.execute(text(insert_sql), df.to_dict(orient="records"))
    return len(df)


def main():
    ensure_weather_table()

    api_key = os.getenv("WEATHER_API_KEY")
    if not api_key:
        raise ValueError("Missing WEATHER_API_KEY in environment (.env)")

    location = "Washington,DC"
    start_date = datetime(2024, 9, 1).date()
    end_date = datetime(2025, 12, 31).date()

    total = 0
    for s, e in create_date_ranges(start_date, end_date, chunk_days=15):
        df = fetch_weather_chunk(location, s, e, api_key)
        inserted = upsert_weather(df)
        total += inserted
        print(f"[{s} → {e}] rows processed: {len(df)}")

    print(f"Done. Total rows processed: {total}")


if __name__ == "__main__":
    main()