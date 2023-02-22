{{
    config(
        alias='tx_hash_labels_offramp_ethereum',
    )
}}

with
 offramp_trades as (
    select
        tx_hash
    from (
        select tx_hash
        from {{ ref('dex_aggregator_trades') }}
        where blockchain = 'ethereum'
        and token_bought_address in (select contract_address from {{ ref('tokens_ethereum_erc20_stablecoins') }})
        and token_sold_address not in (select contract_address from {{ ref('tokens_ethereum_erc20_stablecoins') }})
        UNION ALL
        select tx_hash
        from {{ ref('dex_trades') }}
        where blockchain = 'ethereum'
        and token_bought_address in (select contract_address from {{ ref('tokens_ethereum_erc20_stablecoins') }})
        and token_sold_address not in (select contract_address from {{ ref('tokens_ethereum_erc20_stablecoins') }})
    )
 )

select
  "ethereum" as blockchain,
  tx_hash,
  "Offramp to stable" AS name,
  "tx_hash" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  timestamp('2023-02-21') as created_at,
  now() as updated_at,
  "offramp" as model_name,
  "usage" as label_type
from
  offramp_trades
