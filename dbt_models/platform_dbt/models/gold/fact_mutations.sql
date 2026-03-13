{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select
    date_mutation,
    valeur_fonciere,
    surface_bati,
    code_commune,
    type_local,
    nature_mutation
from {{ ref('dvf_mutations') }}
