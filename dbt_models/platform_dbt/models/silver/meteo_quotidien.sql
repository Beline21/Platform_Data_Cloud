{{
  config(
    materialized='table',
    schema='silver'
  )
}}

select
    time::TIMESTAMP AS time,
    COALESCE(temperature_2m::FLOAT, 0) AS temperature_2m,
    COALESCE(latitude::FLOAT, 0) AS latitude,
    COALESCE(longitude::FLOAT, 0) AS longitude,
    COALESCE(elevation::FLOAT, 0) AS elevation,
    COALESCE(generationtime_ms::FLOAT, 0) AS generationtime_ms,
    COALESCE(utc_offset_seconds::INT, 0) AS utc_offset_seconds,
    COALESCE(timezone::TEXT, '') AS timezone,
    COALESCE(timezone_abbreviation::TEXT, '') AS timezone_abbreviation
from {{ source('bronze', 'meteo_quotidien') }}
