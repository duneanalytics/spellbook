{{ config(
        alias = alias('nft', legacy_model=True)
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "tokens",
                                \'["0xRob"]\') }}'
        )
}}

SELECT
    c.contract_address
  , coalesce(t.name,b.name) as name
  , coalesce(t.symbol,b.symbol) as symbol
  , c.standard
FROM {{ ref('tokens_optimism_nft_standards_legacy')}} c
LEFT JOIN  {{ref('tokens_optimism_nft_curated_legacy')}} t
    ON c.contract_address = t.contract_address
LEFT JOIN {{ ref('tokens_optimism_nft_bridged_mapping_legacy')}} b
    ON c.contract_address = b.contract_address
