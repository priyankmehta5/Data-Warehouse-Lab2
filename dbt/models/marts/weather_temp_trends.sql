with daily_base as (

    select
        event_date,
        lat,
        lon,
        avg(temp_c) as avg_actual_temp,
        avg(pred_temp_c) as avg_forecast_temp
    from {{ ref('stg_weather_final') }}
    group by event_date, lat, lon

)

select
    event_date,
    lat,
    lon,
    avg_actual_temp,
    avg_forecast_temp,
    avg_forecast_temp - avg_actual_temp as forecast_error,
    avg(avg_actual_temp) over (
        partition by lat, lon
        order by event_date
        rows between 6 preceding and current row
    ) as moving_avg_7d_actual
from daily_base
