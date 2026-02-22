import os
import json
import math
import logging
import datetime as dt

import boto3
import pymysql
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# 0. ArcGIS configuration


URL_2024 = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2024/MapServer"
URL_2025 = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2025/MapServer"

LAYER_2024 = {
    "2024-09": 8,
    "2024-10": 9,
    "2024-11": 10,
    "2024-12": 11,
}

LAYER_2025 = {
    "2025-01": 0,
    "2025-02": 1,
    "2025-03": 2,
    "2025-04": 3,
    "2025-05": 4,
    "2025-06": 5,
    "2025-07": 6,
    "2025-08": 7,
    "2025-09": 8,
    "2025-10": 9,
    "2025-11": 10,
    "2025-12": 11,
}

CHUNK = 2000  


def get_layer_url(month_key: str):
    """Select the corresponding MapServer+layer ID based on the monthly key"""
    if month_key.startswith("2024"):
        layer_id = LAYER_2024.get(month_key)
        base_url = URL_2024
    else:
        layer_id = LAYER_2025.get(month_key)
        base_url = URL_2025

    if layer_id is None:
        raise ValueError(f"No layer mapping for month_key={month_key}")

    return base_url, layer_id


def date_to_month_key(d: dt.date) -> str:
    return d.strftime("%Y-%m")


def date_to_ms_range(d: dt.date):
    """Convert a certain day to the millisecond interval of ArcGIS ISSUE-DATE [start, end)"""
    start = dt.datetime(d.year, d.month, d.day)
    end = start + dt.timedelta(days=1)
    epoch = dt.datetime(1970, 1, 1)
    start_ms = int((start - epoch).total_seconds() * 1000)
    end_ms = int((end - epoch).total_seconds() * 1000)
    return start_ms, end_ms



# 1. RDS connetion (Secrets Manager)


def get_db_config():
    secret_name = os.environ["DB_SECRET_NAME"]  # mis664_project_db_secret
    region_name = os.environ["AWS_REGION"]      # us-east-2

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    resp = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(resp["SecretString"])

    # Some are called dbname, some are called dbInstanceIdentifier, and some are called database.
    db_name = (
        secret.get("dbname")
        or secret.get("database")
        or secret.get("dbInstanceIdentifier")
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



# 2. Calculate the date range that requires increment


def get_date_range(conn):
    """
    Using violations.violation_date as the incremental benchmark
    """
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(violation_date) AS max_date FROM violations;")
        row = cur.fetchone()
        max_date = row["max_date"]

    if max_date is None:
        # If the table is really empty, you can change it to the desired starting point as needed
        # For example, starting from September 1, 2024 (corresponding to your existing ArcGIS data starting point)
        start_date = dt.date(2024, 9, 1)
    else:
        # Starting from the day following the maximum date
        if isinstance(max_date, dt.datetime):
            max_date = max_date.date()
        start_date = max_date + dt.timedelta(days=1)

    # To avoid incomplete data for that day, we will keep track of 'yesterday' until it is captured here
    today = dt.date.today()
    end_date = today - dt.timedelta(days=1)

    if start_date > end_date:
        return None, None

    return start_date, end_date



# 3. Capture violation data by day from ArcGIS


def fetch_violations_for_date(d: dt.date):
    """
    Click 'Day' to pull the record of ISSUE-DATE in ArcGIS for that day
    """
    month_key = date_to_month_key(d)
    base_url, layer_id = get_layer_url(month_key)
    url = f"{base_url}/{layer_id}/query"

    start_ms, end_ms = date_to_ms_range(d)

    offset = 0
    all_rows = []

    while True:
        params = {
            "where": f"ISSUE_DATE >= {start_ms} AND ISSUE_DATE < {end_ms}",
            "outFields": "*",
            "returnGeometry": "false",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": CHUNK,
        }

        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        js = resp.json()

        features = js.get("features", [])
        if not features:
            break

        rows = [f["attributes"] for f in features]
        all_rows.extend(rows)

        logger.info(f"{d} ({month_key}): fetched {len(features)} rows at offset {offset}")

        if len(features) < CHUNK:
            break

        offset += CHUNK

    return all_rows, month_key



# 4. Field cleaning+mapping to the behaviors table


def to_float_safe(x):
    if x is None:
        return None
    try:
        f = float(x)
        if math.isnan(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def ms_to_datetime(ms_value):
    """
    ArcGIS ISSUE_DATE: Millisecond timestamp -> Python datetime + date
    """
    if ms_value is None:
        return None, None
    try:
        sec = float(ms_value) / 1000.0
        dt_val = dt.datetime.utcfromtimestamp(sec)
        return dt_val, dt_val.date()
    except Exception:
        return None, None


def transform_row(r: dict, month_key: str):
    """
    Put ArcGIS attributes on one line ->violations table on one line tuple
    
    - OBJECTID
    - ISSUE_DATE (ms)
    - ISSUING_AGENCY_NAME
    - ACCIDENT_INDICATOR
    - LOCATION
    - VIOLATION_CODE
    - VIOLATION_PROCESS_DESC
    - FINE_AMOUNT
    - TOTAL_PAID
    - LATITUDE
    - LONGITUDE
    """

    issue_ms = r.get("ISSUE_DATE")
    issue_date, violation_date = ms_to_datetime(issue_ms)

    obj_id = r.get("OBJECTID")
    violation_id = f"{month_key}_{obj_id}"

    issuing_agency_name = r.get("ISSUING_AGENCY_NAME")
    accident_indicator = r.get("ACCIDENT_INDICATOR")
    location = r.get("LOCATION")
    violation_code = r.get("VIOLATION_CODE")
    violation_desc = r.get("VIOLATION_PROCESS_DESC")

    fine_amount = to_float_safe(r.get("FINE_AMOUNT"))
    total_paid = to_float_safe(r.get("TOTAL_PAID"))
    latitude = to_float_safe(r.get("LATITUDE"))
    longitude = to_float_safe(r.get("LONGITUDE"))

    # The 'month' field is' YYYY-MM '
    month = month_key

    return (
        violation_id,
        issue_date,
        violation_date,
        issuing_agency_name,
        accident_indicator,
        location,
        violation_code,
        violation_desc,
        fine_amount,
        total_paid,
        latitude,
        longitude,
        month,
    )


def insert_violations(conn, rows):
    """
    rows: list[tuple]， The order must correspond one-to-one with the fields in the INSERT
    """
    if not rows:
        return 0

    sql = """
        INSERT IGNORE INTO violations (
            violation_id,
            issue_date,
            violation_date,
            issuing_agency_name,
            accident_indicator,
            location,
            violation_code,
            violation_desc,
            fine_amount,
            total_paid,
            latitude,
            longitude,
            month
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
    """

    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    return len(rows)



# 5. Lambda main entrance


def lambda_handler(event, context):
    conn = get_connection()
    try:
        start_date, end_date = get_date_range(conn)
        if start_date is None:
            msg = "No new dates to fetch. DB is up to date."
            logger.info(msg)
            return {"statusCode": 200, "body": msg}

        logger.info(f"Fetching violations from {start_date} to {end_date}")

        cur_date = start_date
        total_inserted = 0

        while cur_date <= end_date:
            try:
                logger.info(f"==== {cur_date} ====")
                raw_rows, month_key = fetch_violations_for_date(cur_date)

                # Convert to a tuple list that conforms to the table structure
                rows = [transform_row(r, month_key) for r in raw_rows]

                inserted = insert_violations(conn, rows)
                total_inserted += inserted

                logger.info(
                    f"{cur_date}: fetched {len(raw_rows)} rows, "
                    f"inserted {inserted} into violations"
                )

            except Exception as e:
                logger.error(f"Error processing date {cur_date}: {e}")

            cur_date += dt.timedelta(days=1)

        result_msg = (
            f"Inserted {total_inserted} rows into violations "
            f"from {start_date} to {end_date}"
        )
        logger.info(result_msg)

        return {
            "statusCode": 200,
            "body": result_msg,
        }

    finally:
        conn.close()
