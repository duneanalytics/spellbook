{{
    config(
        alias = 'tx_hash_labels_stable_to_stable_ethereum',

    )
}}

with
 stable_to_stable_trades as (
    select
        *
    from (
        select tx_hash, evt_index, project, version
        from {{ source('dex_aggregator', 'trades') }}
        where blockchain = 'ethereum'
        and token_bought_address in (select contract_address from {{ source('tokens_ethereum', 'stablecoins') }})
        and token_sold_address in (select contract_address from {{ source('tokens_ethereum', 'stablecoins') }})
        UNION ALL
        select tx_hash, evt_index, project, version
        from {{ source('dex', 'trades') }}
        where blockchain = 'ethereum'
        and token_bought_address in (select contract_address from {{ source('tokens_ethereum', 'stablecoins') }})
        and token_sold_address in (select contract_address from {{ source('tokens_ethereum', 'stablecoins') }})
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
