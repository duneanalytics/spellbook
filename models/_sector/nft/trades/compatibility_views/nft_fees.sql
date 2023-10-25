{{ config(
        schema = 'nft',
        alias = 'fees',
        tags = ['dunesql'],
        materialized = 'view'
        )
}}

-- kept for backward compatibility
SELECT *
FROM {{ ref('nft_trades') }}
