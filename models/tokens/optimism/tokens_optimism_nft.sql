{{ config(
        alias = 'nft'
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "tokens",
                                \'["0xRob"]\') }}'
        )
}}

SELECT
    c.contract_address
  , coalesce(t.name,b.name, g.name) as name
  , coalesce(t.symbol,b.symbol, g.symbol) as symbol
  , c.standard

FROM {{ ref('tokens_optimism_nft_standards')}} c
LEFT JOIN {{ref('tokens_optimism_nft_curated')}} t
    ON c.contract_address = t.contract_address
LEFT JOIN {{ ref('tokens_optimism_nft_bridged_mapping')}} b
    ON c.contract_address = b.contract_address
LEFT JOIN {{ ref('tokens_optimism_nft_generated')}} g
    ON c.contract_address = g.contract_address
