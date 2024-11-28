{{ config(
        schema = 'nft',
        alias = 'events',
        materialized = 'view'
        )
}}

-- kept for backward compatibility
SELECT *
FROM {{ ref('nft_trades') }}
