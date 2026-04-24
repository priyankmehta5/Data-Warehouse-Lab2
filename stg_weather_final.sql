select
    event_ts_utc,
    cast(event_ts_utc as date) as event_date,
    lat,
    lon,
    temp_c,
    pred_temp_c,
    record_type,
    load_ts_utc
from USER_DB_GROUNDHOG.SCHEMA_WEATHER.WEATHER_FINAL