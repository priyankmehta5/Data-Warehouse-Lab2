import json
import requests
from datetime import datetime, timezone
from contextlib import closing

from ETL.config import (
    OPEN_METEO_LATITUDE,
    OPEN_METEO_LONGITUDE,
    OPEN_METEO_URL,
    LOCATION_NAME,
    TIMEZONE,
)
from ETL.snowflake_utils import get_snowflake_connection


def fetch_open_meteo_data():
    source_url = OPEN_METEO_URL.format(
        lat=OPEN_METEO_LATITUDE,
        lon=OPEN_METEO_LONGITUDE,
        tz=TIMEZONE
    )

    response = requests.get(source_url, timeout=60)
    response.raise_for_status()
    return response.json(), source_url


def build_raw_row(api_json):
    ingest_ts_utc = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

    return (
        json.dumps(api_json),                 # RESPONSE_JSON as string, parsed in SQL
        LOCATION_NAME,                       # LOCATION_NAME
        float(OPEN_METEO_LATITUDE),          # LAT
        float(OPEN_METEO_LONGITUDE),         # LON
        ingest_ts_utc                        # INGEST_TS_UTC
    )


def load_staging_and_merge(row):
    insert_stage_sql = """
        INSERT INTO OPEN_METEO_RAW_STG (
            RESPONSE_JSON,
            LOCATION_NAME,
            LAT,
            LON,
            INGEST_TS_UTC
        )
        SELECT
            PARSE_JSON(%s),
            %s,
            %s,
            %s,
            %s
    """

    count_stage_sql = "SELECT COUNT(*) FROM OPEN_METEO_RAW_STG"

    merge_sql = """
        MERGE INTO OPEN_METEO_RAW AS tgt
        USING OPEN_METEO_RAW_STG AS stg
        ON tgt.LOCATION_NAME = stg.LOCATION_NAME
           AND tgt.INGEST_TS_UTC = stg.INGEST_TS_UTC
        WHEN MATCHED THEN UPDATE SET
            tgt.RESPONSE_JSON = stg.RESPONSE_JSON,
            tgt.LAT = stg.LAT,
            tgt.LON = stg.LON
        WHEN NOT MATCHED THEN INSERT (
            RESPONSE_JSON,
            LOCATION_NAME,
            LAT,
            LON,
            INGEST_TS_UTC
        )
        VALUES (
            stg.RESPONSE_JSON,
            stg.LOCATION_NAME,
            stg.LAT,
            stg.LON,
            stg.INGEST_TS_UTC
        )
    """

    with closing(get_snowflake_connection()) as conn:
        with closing(conn.cursor()) as cur:
            try:
                cur.execute("BEGIN")

                cur.execute("DELETE FROM OPEN_METEO_RAW_STG")
                cur.execute(insert_stage_sql, row)

                cur.execute(count_stage_sql)
                stage_count = cur.fetchone()[0]

                if stage_count == 0:
                    raise ValueError("Stage validation failed: OPEN_METEO_RAW_STG has 0 rows.")

                cur.execute(merge_sql)

                cur.execute("COMMIT")
                print(f"Raw API data loaded successfully. Staged rows: {stage_count}")

            except Exception as e:
                cur.execute("ROLLBACK")
                print(f"Load failed. Rolled back transaction. Error: {e}")
                raise


def main():
    api_json, source_url = fetch_open_meteo_data()
    row = build_raw_row(api_json)
    load_staging_and_merge(row)


if __name__ == "__main__":
    main()