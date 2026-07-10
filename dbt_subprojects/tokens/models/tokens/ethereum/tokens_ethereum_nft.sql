{{ config(
        alias = 'nft'
        , materialized = 'table'
        , post_hook='{{ hide_spells() }}'
        )
}}

-- reservoir name lookup is now a one-time snapshot of the deprecated reservoir community
-- dataset (see tokens_ethereum_nft_reservoir_names) instead of an inline ~31 GB re-scan
SELECT
    c.contract_address
  , coalesce(curated.name, reservoir.name) as name
  , curated.symbol
  , c.standard
FROM {{ ref('tokens_ethereum_nft_standards')}} c
LEFT JOIN  {{ref('tokens_ethereum_nft_curated')}} curated
    ON c.contract_address = curated.contract_address
LEFT JOIN {{ ref('tokens_ethereum_nft_reservoir_names') }} reservoir
    ON c.contract_address = reservoir.contract_address

