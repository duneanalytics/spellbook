{{ config(
     schema = 'tokens_zksync'
    , alias = 'nft'
    , materialized = 'table'
    , post_hook='{{ expose_spells(\'["zksync"]\',
                                "sector",
                                "tokens",
                                \'["lgingerich"]\') }}'
    )
}}

SELECT
    c.contract_address
  , t.name
  , t.symbol
  , c.standard
  FROM {{ ref('tokens_zksync_nft_standards')}} c
LEFT JOIN  {{ref('tokens_zksync_nft_curated')}} t
ON c.contract_address = t.contract_address
