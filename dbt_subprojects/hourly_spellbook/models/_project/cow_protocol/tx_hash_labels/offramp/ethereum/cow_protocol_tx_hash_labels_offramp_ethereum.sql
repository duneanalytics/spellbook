{{
    config(
        alias = 'tx_hash_labels_offramp_ethereum',

    )
}}

with
 offramp_trades as (
    select
        *
    from (
        select tx_hash, evt_index, project, version
        from {{ source('dex_aggregator', 'trades') }}
        where blockchain = 'ethereum'
        and token_bought_address in (select contract_address from {{ source('tokens_ethereum', 'stablecoins') }})
        and token_sold_address not in (select contract_address from {{ source('tokens_ethereum', 'stablecoins') }})
        UNION ALL
        select tx_hash, evt_index, project, version
        from {{ source('dex', 'trades') }}
        where blockchain = 'ethereum'
        and token_bought_address in (select contract_address from {{ source('tokens_ethereum', 'stablecoins') }})
        and token_sold_address not in (select contract_address from {{ source('tokens_ethereum', 'stablecoins') }})
    )
 )

select
  'ethereum' as blockchain,
  concat(CAST(tx_hash AS VARCHAR), CAST(evt_index AS VARCHAR), project, version) as tx_hash_key,
  'Offramp to stable' AS name,
  'tx_hash' AS category,
  'gentrexha' AS contributor,
  'query' AS source,
  TIMESTAMP '2023-02-21' as created_at,
  now() as updated_at,
  'offramp' as model_name,
  'usage' as label_type
from
  offramp_trades
