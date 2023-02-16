{{ config(
        alias ='nft'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['contract_address']
        , post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "tokens",
                                \'["hildobby"]\') }}'
        )
}}

WITH wizards_curated_collections AS (
    SELECT contract_address
  , name
  , symbol
  , standard
  , category
  FROM {{ref('tokens_ethereum_nft_wizards_curated')}}
  )

SELECT *
FROM (
  SELECT *
  FROM wizards_curated_collections

  UNION ALL

  SELECT c.contract AS contract_address
  , MIN(c.name) AS name
  , NULL AS symbol
  , MIN(t.token_standard) AS standard
  , NULL AS category
  FROM {{source('reservoir','collections')}} c
  INNER JOIN {{ref('nft_ethereum_transfers')}} t ON c.contract=t.contract_address
  LEFT ANTI JOIN wizards_curated_collections w ON w.contract_address=c.contract_address
  GROUP BY c.contract, c.name
  ) r
LEFT ANTI JOIN {{this}} f ON f.contract_address=r.contract_address