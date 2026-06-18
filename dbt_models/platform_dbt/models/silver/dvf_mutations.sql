{{
  config(
    materialized='table',
    schema='silver'
  )
}}

select

    row_number() over (ORDER BY "Date mutation") as cle_primaire,

    COALESCE(
        TRY_TO_NUMBER("No disposition"),
        0
    ) AS disposition_id,

    COALESCE(
        TRY_TO_DATE("Date mutation"),
        TRY_TO_DATE('1900-01-01')
    ) AS date_mutation,

    COALESCE("Nature mutation", '')::TEXT AS nature_mutation,

    COALESCE(
        TRY_TO_DECIMAL(
            REPLACE("Valeur fonciere", ',', '.')
        ),
        0
    ) AS valeur_fonciere,

    COALESCE(
        TRY_TO_NUMBER("No voie"),
        0
    ) AS numero_voie,

    COALESCE("Voie", '')::TEXT AS voie,

    COALESCE("Code postal", '')::TEXT AS code_postal,

    COALESCE("Commune", '')::TEXT AS commune,

    COALESCE("Code departement", '')::TEXT AS departement,

    COALESCE(
        TRY_TO_NUMBER("Code commune"),
        0
   )  AS code_commune,

    COALESCE("Section", '')::TEXT AS section,

    COALESCE(
        TRY_TO_NUMBER("No plan"),
        0
    ) AS numero_plan,

    COALESCE("Code type local", '')::TEXT AS code_type_local,

    COALESCE("Type local", '')::TEXT AS type_local,

    COALESCE(
        TRY_TO_DOUBLE(
            REPLACE("Surface reelle bati", ',', '.')
        ),
        0
    ) AS surface_bati,

    COALESCE(
        TRY_TO_NUMBER("Nombre pieces principales"),
        0
    ) AS nb_pieces,

    COALESCE("Nature culture", '')::TEXT AS nature_culture,

    COALESCE(
        TRY_TO_DOUBLE(
            REPLACE("Surface terrain", ',', '.')
        ),
        0
    ) AS surface_terrain

from {{ source('bronze', 'dvf_mutations') }}
where COALESCE(
          TRY_TO_DECIMAL(
              REPLACE("Valeur fonciere", ',', '.')
          ),
          0
      ) > 0
