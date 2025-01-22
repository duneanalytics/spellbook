{{ config(
        alias = 'nft'
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["polygon"]\',
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
FROM {{ ref('tokens_polygon_nft_standards')}} c
LEFT JOIN  {{ref('tokens_polygon_nft_curated')}} t
    ON c.contract_address = t.contract_address
