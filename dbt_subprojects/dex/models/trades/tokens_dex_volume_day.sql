{{ config(
    schema = 'tokens'
    , alias = 'dex_volume_day'
    , materialized = 'view'
    )
}}

SELECT *
FROM {{ ref('dex_token_volumes_daily') }}

