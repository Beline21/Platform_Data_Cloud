{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select distinct
    time as time_id,
    time as time
from {{ ref('meteo_quotidien') }}
where time is not null
