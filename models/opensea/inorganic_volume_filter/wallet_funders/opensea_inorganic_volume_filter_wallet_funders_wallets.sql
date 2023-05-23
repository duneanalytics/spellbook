{{ config(
    alias = 'inorganic_volume_filter_wallet_funders_wallet',
    materialized = 'view'
)
}}

SELECT DISTINCT(buyer) as wallet FROM {{ ref('nft_events') }}
WHERE project in ('looksrare','x2y2','blur')
UNION
SELECT DISTINCT(seller) as wallet FROM {{ ref('nft_events') }}
WHERE project in ('looksrare','x2y2','blur')
