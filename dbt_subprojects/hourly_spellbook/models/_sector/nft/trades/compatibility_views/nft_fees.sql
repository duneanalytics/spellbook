{{ config(
        schema = 'nft',
        alias = 'fees',
        materialized = 'view'
        )
}}

-- kept for backward compatibility
SELECT *
FROM {{ ref('nft_trades') }}
