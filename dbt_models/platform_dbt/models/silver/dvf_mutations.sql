{{
  config(
    materialized='table',
    schema='silver'
  )
}}

select

    "Identifiant de document"::TEXT AS document_id,
    COALESCE("Reference document", '')::TEXT AS reference_document,

    COALESCE(NULLIF("No disposition", ''), '0')::INT AS disposition_id,

    COALESCE(
        "Date mutation"::DATE,
        DATE '1900-01-01'
    ) AS date_mutation,

    COALESCE("Nature mutation", '')::TEXT AS nature_mutation,

    COALESCE(NULLIF("Valeur fonciere", '')::NUMERIC
    ) AS valeur_fonciere,

    COALESCE(NULLIF("No voie", ''), '0')::INT AS numero_voie,

    COALESCE("Type de voie", '')::TEXT AS type_voie,

    COALESCE("Voie", '')::TEXT AS voie,

    COALESCE("Code postal", '')::TEXT AS code_postal,

    COALESCE("Commune", '')::TEXT AS commune,

    COALESCE("Code departement", '')::TEXT AS departement,

    COALESCE("Code commune", '')::INT AS code_commune,

    COALESCE("Type local", '')::TEXT AS type_local,

    COALESCE(
        NULLIF("Surface reelle bati", '')::FLOAT,
        0
    ) AS surface_bati,

    COALESCE(
        NULLIF("Nombre pieces principales", '')::INT,
        0
    ) AS nb_pieces,

    COALESCE(
        NULLIF("Surface terrain", '')::FLOAT,
        0
    ) AS surface_terrain

from {{ source('bronze', 'dvf_data') }}
where "Valeur fonciere" is not null
  and REPLACE(NULLIF("Valeur fonciere", ''), ',', '.')::NUMERIC > 0
