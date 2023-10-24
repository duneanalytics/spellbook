{{ config(
        schema = 'nft',
        alias = alias('events'),
        tags = ['dunesql'],
        materialized = 'view',
}}

-- kept for backward compatibility
SELECT *
FROM {{ ref('nft_trades') }}
