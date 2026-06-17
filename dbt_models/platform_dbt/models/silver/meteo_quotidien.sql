{{
  config(
    materialized='table',
    schema='silver'
  )
}}

select
    row_number() over () as cle_primaire,
    TO_TIMESTAMP(time, 'YYYY-MM-DD"T"HH24:MI') AS time,
    COALESCE(temperature_2m::FLOAT, 0) AS temperature_2m,
    COALESCE(latitude::FLOAT, 0) AS latitude,
    COALESCE(longitude::FLOAT, 0) AS longitude,
    COALESCE(elevation::FLOAT, 0) AS elevation,
    COALESCE(generationtime_ms::FLOAT, 0) AS generationtime_ms,
    COALESCE(timezone::TEXT, '') AS timezone
from {{ source('bronze', 'meteo_quotidien') }}
