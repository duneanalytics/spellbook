{{ config(
        alias = 'nft'
        , schema = 'tokens_goerli'
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["goerli"]\',
                                "sector",
                                "tokens",
                                \'["0xRob"]\') }}'
        )
}}

SELECT c.contract_address
  , t.name
  , t.symbol
  , c.standard
  FROM {{ ref('tokens_goerli_nft_standards')}} c
LEFT JOIN  {{ref('tokens_goerli_nft_curated')}} t
ON c.contract_address = t.contract_address
