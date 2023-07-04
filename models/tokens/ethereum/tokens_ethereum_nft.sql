{{ config(tags=['dunesql'],
        alias = alias('nft')
        , materialized = 'table'
        , post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "tokens",
                                \'["hildobby"]\') }}'
        )
}}

-- reservoir has multiple collection names for single contracts, we just take the first record
WITH reservoir_names as (
    select
    contract as contract_address
    ,min_by(name,created_at) as name
    FROM {{source('reservoir','collections')}} c
    group by 1
)

SELECT
    c.contract_address
  , coalesce(curated.name, reservoir.name) as name
  , curated.symbol
  , c.standard
FROM {{ ref('tokens_ethereum_nft_standards')}} c
LEFT JOIN  {{ref('tokens_ethereum_nft_curated')}} curated
    ON c.contract_address = curated.contract_address
LEFT JOIN reservoir_names reservoir
    ON c.contract_address = reservoir.contract_address

