{{ config( alias='erc20', materialized = 'table',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "tokens",
                                    \'["msilb7"]\') }}')}}


SELECT
    c.contract_address
  , coalesce(t.symbol,b.symbol) as symbol
  , coalesce(t.decimals,b.decimals) as decimals
  , t.token_type
  , t.token_mapping_source
  , t.is_counted_in_tvl

FROM {{ ref('tokens_optimism_erc20_transfer_source')}} c
LEFT JOIN  {{ref('tokens_optimism_erc20_curated')}} t
    ON c.contract_address = t.contract_address
LEFT JOIN {{ ref('tokens_optimism_erc20_generated')}} b
    ON c.contract_address = b.contract_address
-- Eventually we can also try to map sectors here (i.e. stablecoin, liquid staking)