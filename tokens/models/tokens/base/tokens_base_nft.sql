{{ config(
        alias = 'nft'
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["base"]\',
                                "sector",
                                "tokens",
                                \'["msilb7"]\') }}'
        )
}}

SELECT
    c.contract_address
  , name--coalesce(t.name,b.name) as name
  , symbol--coalesce(t.symbol,b.symbol) as symbol
  , c.standard

FROM {{ ref('tokens_base_nft_standards')}} c
LEFT JOIN {{ref('tokens_base_nft_curated')}} t
    ON c.contract_address = t.contract_address
-- LEFT JOIN 'tokens_base_nft_bridged_mapping b
--     ON c.contract_address = b.contract_address
