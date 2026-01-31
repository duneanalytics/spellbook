{{ config(
        schema = 'dex_evm'
        , alias = 'trades'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ ref('dex_trades') }}

