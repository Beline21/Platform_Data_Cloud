{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select distinct
    code_commune as commune_id,
    code_commune as code_commune
from {{ ref('dvf_mutations') }}
where code_commune is not null
