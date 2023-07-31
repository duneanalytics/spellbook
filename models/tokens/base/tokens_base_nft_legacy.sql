{{ config(
	tags=['legacy'],
        alias = alias('nft', legacy_model=True)
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["base"]\',
                                "sector",
                                "tokens",
                                \'["0xRob"]\') }}'
        )
}}

SELECT
    c.contract_address
  , name--coalesce(t.name,b.name) as name
  , symbol--coalesce(t.symbol,b.symbol) as symbol
  , c.standard
FROM {{ ref('tokens_base_nft_standards_legacy')}} c
LEFT JOIN  {{ref('tokens_base_nft_curated_legacy')}} t
    ON c.contract_address = t.contract_address
-- LEFT JOIN 'tokens_base_nft_bridged_mapping_legacy' b
--     ON c.contract_address = b.contract_address
