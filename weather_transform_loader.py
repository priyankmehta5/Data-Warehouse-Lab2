import json
import pandas as pd
from datetime import datetime, timezone
from .snowflake_utils import get_snowflake_connection


def load_latest_raw_json(conn):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT RESPONSE_JSON, LOCATION_NAME, LAT, LON
            FROM OPEN_METEO_RAW
            ORDER BY INGEST_TS_UTC DESC
            LIMIT 1
        """)
        row = cur.fetchone()

        if not row:
            raise ValueError("No data found in OPEN_METEO_RAW")

        response_json = row[0]
        location_name = row[1]
        lat = row[2]
        lon = row[3]

        if isinstance(response_json, str):
            response_json = json.loads(response_json)

        return response_json, location_name, lat, lon

    finally:
        cur.close()


def transform_hourly_data(response_json, location_name, lat, lon):
    if "hourly" not in response_json:
        raise ValueError("Expected 'hourly' key not found in API response JSON")

    hourly = response_json["hourly"]

    df = pd.DataFrame({
        "obs_ts_utc": hourly["time"],
        "temp_c": hourly["temperature_2m"],
        "rel_humidity_pct": hourly["relative_humidity_2m"],
        "wind_speed_ms": hourly["wind_speed_10m"],
        "precip_mm": hourly["precipitation"]
    })

    df["obs_ts_utc"] = pd.to_datetime(df["obs_ts_utc"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["location_name"] = location_name
    df["lat"] = lat
    df["lon"] = lon
    df["load_ts_utc"] = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

    return df


def transform_daily_data(hourly_df):
    daily_df = hourly_df.copy()
    daily_df["date"] = pd.to_datetime(daily_df["obs_ts_utc"]).dt.strftime("%Y-%m-%d")

    result = daily_df.groupby(["location_name", "date", "lat", "lon"], as_index=False).agg(
        temp_max=("temp_c", "max"),
        temp_min=("temp_c", "min"),
        temp_mean=("temp_c", "mean"),
        precip_total=("precip_mm", "sum"),
        wind_speed_avg=("wind_speed_ms", "mean")
    )

    result["load_ts_utc"] = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    return result


def refresh_table(conn, target_table, insert_sql, rows, table_label):
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")

        # Full refresh pattern makes reruns idempotent
        cur.execute(f"DELETE FROM {target_table}")

        if not rows:
            raise ValueError(f"No rows generated for {table_label}; load stopped.")

        cur.executemany(insert_sql, rows)

        cur.execute("COMMIT")
        print(f"{table_label} loaded successfully with {len(rows)} rows.")

    except Exception as e:
        cur.execute("ROLLBACK")
        print(f"{table_label} load failed. Transaction rolled back. Error: {e}")
        raise

    finally:
        cur.close()


def load_hourly_table(conn, hourly_df):
    rows = [
        (
            row["location_name"],
            row["obs_ts_utc"],
            row["lat"],
            row["lon"],
            row["temp_c"],
            row["rel_humidity_pct"],
            row["wind_speed_ms"],
            row["precip_mm"],
            row["load_ts_utc"]
        )
        for _, row in hourly_df.iterrows()
    ]

    insert_sql = """
        INSERT INTO WEATHER_OBSERVATION_HOURLY (
            LOCATION_NAME,
            OBS_TS_UTC,
            LAT,
            LON,
            TEMP_C,
            REL_HUMIDITY_PCT,
            WIND_SPEED_MS,
            PRECIP_MM,
            LOAD_TS_UTC
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    refresh_table(
        conn=conn,
        target_table="WEATHER_OBSERVATION_HOURLY",
        insert_sql=insert_sql,
        rows=rows,
        table_label="WEATHER_OBSERVATION_HOURLY"
    )


def load_daily_table(conn, daily_df):
    rows = [
        (
            row["location_name"],
            row["date"],
            row["temp_max"],
            row["temp_min"],
            row["temp_mean"],
            row["precip_total"],
            row["wind_speed_avg"],
            row["load_ts_utc"]
        )
        for _, row in daily_df.iterrows()
    ]

    insert_sql = """
        INSERT INTO WEATHER_DAILY (
            LOCATION_NAME,
            DATE,
            TEMP_MAX,
            TEMP_MIN,
            TEMP_MEAN,
            PRECIP_TOTAL,
            WIND_SPEED_AVG,
            LOAD_TS_UTC
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    refresh_table(
        conn=conn,
        target_table="WEATHER_DAILY",
        insert_sql=insert_sql,
        rows=rows,
        table_label="WEATHER_DAILY"
    )


def main():
    conn = None
    try:
        conn = get_snowflake_connection()

        response_json, location_name, lat, lon = load_latest_raw_json(conn)
        hourly_df = transform_hourly_data(response_json, location_name, lat, lon)
        daily_df = transform_daily_data(hourly_df)

        load_hourly_table(conn, hourly_df)
        load_daily_table(conn, daily_df)

        print("Hourly and daily weather tables loaded successfully.")

    except Exception as e:
        print(f"Transformation load failed: {e}")
        raise

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()