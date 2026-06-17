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

meteo AS (

    SELECT
        ROW_NUMBER() OVER () AS cle_primaire,

        hourly.value:time::TIMESTAMP AS time,

        hourly.value:temperature_2m::FLOAT AS temperature_2m,

        latitude::FLOAT AS latitude,
        longitude::FLOAT AS longitude,
        elevation::FLOAT AS elevation,
        generationtime_ms::FLOAT AS generationtime_ms,
        timezone::TEXT AS timezone

    FROM source,
    LATERAL FLATTEN(input => hourly:time) hourly

)

SELECT *
FROM meteo
