{{ config(
    schema='prices_v2'
    , alias = 'dex_minute'
    , materialized = 'view'
    )
}}

SELECT
*
FROM {{ ref('prices_v2_dex_minute_raw') }}
INNER JOIN {{ ref('prices_v2_dex_filter') }} using (blockchain, contract_address)
