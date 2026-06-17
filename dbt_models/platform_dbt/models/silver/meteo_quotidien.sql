{{
  config(
    materialized='table',
    schema='silver'
  )
}}

WITH source AS (

    SELECT
        latitude,
        longitude,
        elevation,
        generationtime_ms,
        timezone,
        hourly
    FROM {{ source('bronze', 'meteo_quotidien') }}

),

exploded AS (

    SELECT
        f.index AS idx,
        f.value AS time_value,

        hourly,
        latitude,
        longitude,
        elevation,
        generationtime_ms,
        timezone

    FROM source,
    LATERAL FLATTEN(input => hourly:time) f

)

SELECT

    ROW_NUMBER() OVER (ORDER BY idx) AS cle_primaire,

    time_value::TIMESTAMP AS time,
    hourly:temperature_2m::FLOAT AS temperature_2m,

    latitude::FLOAT AS latitude,
    longitude::FLOAT AS longitude,
    elevation::FLOAT AS elevation,
    generationtime_ms::FLOAT AS generationtime_ms,
    timezone::TEXT AS timezone

FROM exploded
