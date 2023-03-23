{{ config(
        alias ='nft'
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "sector",
                                "tokens_arbitrum",
                                \'["0xRob"]\') }}'
        )
}}

SELECT c.nft_contract_address as contract_address
  , t.name
  , t.symbol
  , c.standard
  FROM {{ ref('nft_arbitrum_contract_standards')}} c
LEFT JOIN  {{ref('tokens_arbitrum_nft_curated')}} t
ON s.nft_contract_address = t.contract_address
