{{
    config(
        alias='tx_hash_labels_stable_to_stable_ethereum',
    )
}}

with
 stable_to_stable_trades as (
    select
        distinct tx_hash
    from (
        select tx_hash
        from {{ ref('dex_aggregator_trades') }}
        where blockchain = 'ethereum'
        and token_bought_address in (select contract_address from {{ ref('tokens_ethereum_erc20_stablecoins') }})
        and token_sold_address in (select contract_address from {{ ref('tokens_ethereum_erc20_stablecoins') }})
        UNION ALL
        select tx_hash
        from {{ ref('dex_trades') }}
        where blockchain = 'ethereum'
        and token_bought_address in (select contract_address from {{ ref('tokens_ethereum_erc20_stablecoins') }})
        and token_sold_address in (select contract_address from {{ ref('tokens_ethereum_erc20_stablecoins') }})
    )
 )

select
  array("ethereum") as blockchain,
  tx_hash,
  "Stable to stable" AS name,
  "stable_to_stable" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  timestamp('2022-11-16') as created_at,
  now() as updated_at
from
  stable_to_stable_trades
