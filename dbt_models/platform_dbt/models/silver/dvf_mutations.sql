{{
  config(
    materialized='table',
    schema='silver'
  )
}}

select

    row_number() over () as cle_primaire,

    COALESCE("No disposition", 0)::INT AS disposition_id,

    COALESCE(
        "Date mutation"::DATE,
        DATE '1900-01-01'
    ) AS date_mutation,

    COALESCE("Nature mutation", '')::TEXT AS nature_mutation,

    COALESCE(
        REPLACE("Valeur fonciere", ',', '.')::NUMERIC,
        0
    ) AS valeur_fonciere,

    COALESCE("No voie", '0')::INT AS numero_voie,

    COALESCE("Voie", '')::TEXT AS voie,

    COALESCE("Code postal", '')::TEXT AS code_postal,

    COALESCE("Commune", '')::TEXT AS commune,

    COALESCE("Code departement", '')::TEXT AS departement,

    COALESCE("Code commune", '')::INT AS code_commune,

    COALESCE("Section", '')::TEXT AS section,

    COALESCE("No plan", '0)::INT AS numero_plan,

    COALESCE("Code type local", '')::TEXT AS code_type_local,

    COALESCE("Type local", '')::TEXT AS type_local,

    COALESCE(
        NULLIF("Surface reelle bati", '')::FLOAT,
        0
    ) AS surface_bati,

    COALESCE(
        NULLIF("Nombre pieces principales", '')::INT,
        0
    ) AS nb_pieces,

    COALESCE("Nature culture", '')::TEXT AS nature_culture,

    COALESCE(
        NULLIF("Surface terrain", '')::FLOAT,
        0
    ) AS surface_terrain

from {{ source('bronze', 'dvf_mutations') }}
where "Valeur fonciere" is not null
  and REPLACE("Valeur fonciere", ',', '.')::NUMERIC > 0
