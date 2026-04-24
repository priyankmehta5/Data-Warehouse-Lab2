{% snapshot weather_final_snapshot %}

{{
    config(
      target_schema='SNAPSHOTS',
      unique_key="cast(event_ts_utc as string) || '-' || cast(lat as string) || '-' || cast(lon as string) || '-' || record_type",
      strategy='check',
      check_cols=['temp_c', 'pred_temp_c', 'record_type', 'load_ts_utc']
    )
}}

select
    event_ts_utc,
    lat,
    lon,
    temp_c,
    pred_temp_c,
    record_type,
    load_ts_utc
from USER_DB_GROUNDHOG.SCHEMA_WEATHER.WEATHER_FINAL

{% endsnapshot %}
