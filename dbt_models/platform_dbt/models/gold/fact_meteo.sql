{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select
    time,
    avg(temperature_2m) as temperature_2m_moy,
    avg(elevation) as elevation_moy,
    sum(generationtime_ms) as generationtime_ms_totale
from {{ ref('meteo_quotidien') }}
group by 1
order by 1
