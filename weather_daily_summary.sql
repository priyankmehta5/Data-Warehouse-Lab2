select
    event_date,
    lat,
    lon,
    avg(temp_c) as avg_actual_temp,
    avg(pred_temp_c) as avg_forecast_temp,
    min(temp_c) as min_actual_temp,
    max(temp_c) as max_actual_temp,
    count_if(record_type = 'OBS') as obs_count,
    count_if(record_type = 'FCST') as forecast_count
from {{ ref('stg_weather_final') }}
group by event_date, lat, lon