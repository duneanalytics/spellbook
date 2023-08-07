{{ config(
	      tags=['legacy'],
        alias = alias('nft_standards', legacy_model=True)
)
}}

 SELECT
  1 as contract_address
, 1 AS standard
