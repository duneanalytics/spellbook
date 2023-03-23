{{ config(
        alias ='nft'
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "tokens",
                                \'["0xRob"]\') }}'
        )
}}

SELECT
    c.nft_contract_address as contract_address
  , coalesce(t.name,b.name) as name
  , coalesce(t.symbol,b.symbol) as symbol
  , c.standard
FROM {{ ref('nft_optimism_contract_standards')}} c
LEFT JOIN  {{ref('tokens_optimism_nft_curated')}} t
    ON c.nft_contract_address = t.contract_address
LEFT JOIN {{ ref('tokens_optimism_nft_bridged_mapping')}} b
    ON c.nft_contract_address = b.contract_address
