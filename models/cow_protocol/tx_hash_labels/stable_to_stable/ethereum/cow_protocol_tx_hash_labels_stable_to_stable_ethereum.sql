{{
    config(
        alias = alias('tx_hash_labels_stable_to_stable_ethereum'),
        tags=['dunesql']
    )
}}

with
 stable_to_stable_trades as (
    select
        *
    from (
        select tx_hash, evt_index, project, version
        from {{ ref('dex_aggregator_trades') }}
        where blockchain = 'ethereum'
        and token_bought_address in (select contract_address from {{ ref('tokens_ethereum_erc20_stablecoins') }})
        and token_sold_address in (select contract_address from {{ ref('tokens_ethereum_erc20_stablecoins') }})
        UNION ALL
        select tx_hash, evt_index, project, version
        from {{ ref('dex_trades') }}
        where blockchain = 'ethereum'
        and token_bought_address in (select contract_address from {{ ref('tokens_ethereum_erc20_stablecoins') }})
        and token_sold_address in (select contract_address from {{ ref('tokens_ethereum_erc20_stablecoins') }})
    )
 )

select
  'ethereum' as blockchain,
  concat(CAST(tx_hash AS VARCHAR), CAST(evt_index AS VARCHAR), project, version) as tx_hash_key,
  'Stable to stable' AS name,
  'tx_hash' AS category,
  'gentrexha' AS contributor,
  'query' AS source,
  TIMESTAMP '2022-11-16' as created_at,
  now() as updated_at,
  'stable_to_stable' as model_name,
  'usage' as label_type
from
  stable_to_stable_trades
