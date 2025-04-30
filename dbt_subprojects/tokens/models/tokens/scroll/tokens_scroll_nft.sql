{{ config(
        alias = 'nft'
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["scroll"]\',
                                "sector",
                                "tokens",
                                \'["msilb7"]\') }}'
        )
}}

SELECT
    c.contract_address
  , name
  , symbol
  , c.standard

FROM {{ ref('tokens_scroll_nft_standards')}} c
LEFT JOIN {{ref('tokens_scroll_nft_curated')}} t
    ON c.contract_address = t.contract_address 
