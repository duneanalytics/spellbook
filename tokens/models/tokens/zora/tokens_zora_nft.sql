{{ config(
        schema = 'tokens_zora'
        , alias = 'nft'
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["zora"]\',
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

FROM {{ ref('tokens_zora_nft_standards')}} c
LEFT JOIN {{ref('tokens_zora_nft_curated')}} t
    ON c.contract_address = t.contract_address
-- LEFT JOIN 'tokens_zora_nft_bridged_mapping b
--     ON c.contract_address = b.contract_address
