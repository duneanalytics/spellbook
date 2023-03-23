{{ config(
        alias ='nft'
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "sector",
                                "tokens",
                                \'["0xRob"]\') }}'
        )
}}

SELECT c.nft_contract_address as contract_address
  , t.name
  , t.symbol
  , c.standard
  FROM {{ ref('nft_avalanche_c_contract_standards')}} c
LEFT JOIN  {{ref('tokens_avalanche_c_nft_curated')}} t
ON s.nft_contract_address = t.contract_address
