select
    lat,
    lon,
    count(*) as total_records,
    avg(temp_c) as overall_actual_temp,
    avg(pred_temp_c) as overall_forecast_temp,
    min(temp_c) as min_actual_temp,
    max(temp_c) as max_actual_temp,
    count_if(record_type = 'OBS') as total_obs_records,
    count_if(record_type = 'FCST') as total_forecast_records
from {{ ref('stg_weather_final') }}
group by lat, lon
