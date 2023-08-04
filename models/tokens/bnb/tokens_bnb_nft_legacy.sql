{{ config(
	tags=['legacy'],
        alias = alias('nft', legacy_model=True)
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["bnb"]\',
                                "sector",
                                "tokens",
                                \'["0xRob"]\') }}'
        )
}}

SELECT
    c.contract_address
  , t.name
  , t.symbol
  , c.standard
  FROM {{ ref('tokens_bnb_nft_standards_legacy')}} c
LEFT JOIN  {{ref('tokens_bnb_nft_curated_legacy')}} t
ON c.contract_address = t.contract_address
