import os
import json
import logging
import datetime as dt

import boto3
import pymysql
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# 1. RDS connetion (Secrets Manager)


def get_db_config():
    secret_name = os.environ["DB_SECRET_NAME"]   # mis664_project_db_secret
    region_name = os.environ["AWS_REGION"]       # us-east-2

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    resp = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(resp["SecretString"])

    # Try to get the database name from the Secret; if it's not available, use an environment variable or a default value.
    db_name = (
        secret.get("dbname")
        or secret.get("database")
        or os.environ.get("DB_NAME")
        or "mis664_project"
    )

    return {
        "host": secret["host"],
        "user": secret["username"],
        "password": secret["password"],
        "db": db_name,
        "port": int(secret.get("port", 3306)),
    }



def get_connection():
    cfg = get_db_config()
    return pymysql.connect(
        host=cfg["host"],
        user=cfg["user"],
        password=cfg["password"],
        db=cfg["db"],
        port=cfg["port"],
        cursorclass=pymysql.cursors.DictCursor,
    )


# 2. Calculate the date range that requires increments (weather_daily)


def get_date_range(conn):
    """
    Use weather_daily.weather_date as the incremental benchmark
    """
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(weather_date) AS max_date FROM weather_daily;")
        row = cur.fetchone()
        max_date = row["max_date"]

    if max_date is None:
        # If the weather table is empty: start from the first day after historical data
        start_date = dt.date(2024, 12, 1)
    else:
        if isinstance(max_date, dt.datetime):
            max_date = max_date.date()
        start_date = max_date + dt.timedelta(days=1)

    # Capture up to yesterday to avoid incomplete data for the current day
    today = dt.date.today()
    end_date = today - dt.timedelta(days=1)

    if start_date > end_date:
        return None, None

    return start_date, end_date




# 3. Call the Visual Crossing API to get the weather for the current day


def fetch_weather_for_date(d: dt.date):
    api_key = os.environ["WEATHER_API_KEY"]
    location = os.environ["WEATHER_LOCATION"]

    base_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
    url = f"{base_url}/{location}/{d.isoformat()}"

    params = {
        "unitGroup": "metric",
        "include": "days",
        "key": api_key,
        "contentType": "json",
    }

    logger.info(f"[DEBUG] Calling VisualCrossing for {d} ...")

    resp = requests.get(url, params=params, timeout=5)  # First, use a small timeout of 5 seconds
    logger.info(f"[DEBUG] VisualCrossing responded for {d} with status {resp.status_code}")

    resp.raise_for_status()
    js = resp.json()
    return js


# 4. Map JSON to a row in weather_daily


def transform_weather_row(d: dt.date, js: dict):
    """
    weather_daily ：

    weather_date DATE PRIMARY KEY,
    tempmax      DOUBLE,
    tempmin      DOUBLE,
    temp         DOUBLE,
    precip       DOUBLE,
    humidity     DOUBLE,
    windspeed    DOUBLE,
    conditions   TEXT,
    is_rain      TINYINT
    """

    days = js.get("days", [])
    if not days:
        # If there is no data on that day, use all NULL + label text
        return (
            d,
            None,
            None,
            None,
            None,
            None,
            None,
            "missing_from_api",
            0,
        )

    day = days[0]

    tempmax = day.get("tempmax")
    tempmin = day.get("tempmin")
    temp = day.get("temp")
    precip = day.get("precip")
    humidity = day.get("humidity")
    windspeed = day.get("windspeed")
    conditions = day.get("conditions") or ""

    # Simple rule: If there is precipitation or the text mentions 'rain', it is considered rainy.
    cond_lower = conditions.lower()
    is_rain = 1 if (precip and precip > 0) or ("rain" in cond_lower) else 0

    return (
        d,
        tempmax,
        tempmin,
        temp,
        precip,
        humidity,
        windspeed,
        conditions,
        is_rain,
    )


def insert_weather_daily(conn, rows):
    """
    rows: list[tuple]，The order must correspond to the fields in the INSERT statement.
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO weather_daily (
            weather_date,
            tempmax,
            tempmin,
            temp,
            precip,
            humidity,
            windspeed,
            conditions,
            is_rain
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON DUPLICATE KEY UPDATE
            tempmax = VALUES(tempmax),
            tempmin = VALUES(tempmin),
            temp    = VALUES(temp),
            precip  = VALUES(precip),
            humidity= VALUES(humidity),
            windspeed = VALUES(windspeed),
            conditions = VALUES(conditions),
            is_rain    = VALUES(is_rain);
    """

    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    return len(rows)



# 5. Lambda Main Entry (Weather Increment)


def lambda_handler(event, context):
    conn = get_connection()
    try:
        start_date, end_date = get_date_range(conn)
        if start_date is None:
            msg = "No new weather dates to fetch. DB is up to date."
            logger.info(msg)
            return {"statusCode": 200, "body": msg}

        logger.info(f"Fetching weather from {start_date} to {end_date}")

        cur_date = start_date
        total_inserted = 0

        while cur_date <= end_date:
            try:
                logger.info(f"==== {cur_date} ====")

                js = fetch_weather_for_date(cur_date)
                row = transform_weather_row(cur_date, js)
                inserted = insert_weather_daily(conn, [row])
                total_inserted += inserted

                logger.info(
                    f"{cur_date}: fetched 1 day, inserted {inserted} row into weather_daily"
                )

            except Exception as e:
                logger.error(f"Error processing weather for {cur_date}: {e}")

            cur_date += dt.timedelta(days=1)

        result_msg = (
            f"Inserted/updated {total_inserted} rows into weather_daily "
            f"from {start_date} to {end_date}"
        )
        logger.info(result_msg)

        return {
            "statusCode": 200,
            "body": result_msg,
        }

    finally:
        conn.close()
