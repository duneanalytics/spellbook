{{ config(
        alias ='nft'
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "tokens",
                                \'["hildobby"]\') }}'
        )
}}

SELECT contract_address
  , name
  , symbol
  , standard
  , category
  FROM {{ref('tokens_ethereum_nft_wizards_curated')}}
UNION
SELECT c.contract AS contract_address
, MIN(c.name) AS name
, NULL AS symbol
, MIN(t.token_standard) AS standard
, NULL AS category
FROM {{source('reservoir','collections')}} c
INNER JOIN {{ref('nft_ethereum_transfers')}} t ON c.contract=t.contract_address
LEFT ANTI JOIN {{ref('tokens_ethereum_nft_wizards_curated')}} w ON w.contract_address=c.contract
GROUP BY c.contract
