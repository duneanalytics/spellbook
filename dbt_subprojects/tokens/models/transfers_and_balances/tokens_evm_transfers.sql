{{ config(
        schema = 'tokens_evm'
        , alias = 'transfers'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ ref('tokens_transfers') }}
