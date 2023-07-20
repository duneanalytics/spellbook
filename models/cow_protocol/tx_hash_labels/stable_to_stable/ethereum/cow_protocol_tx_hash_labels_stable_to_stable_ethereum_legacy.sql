{{
    config(
	tags=['legacy'],
	
        alias = alias('tx_hash_labels_stable_to_stable_ethereum', legacy_model=True),
    )
}}

with
 stable_to_stable_trades as (
    select
        *
    from (
        select tx_hash, evt_index, project, version
        from {{ ref('dex_aggregator_trades_legacy') }}
        where blockchain = 'ethereum'
        and token_bought_address in (select contract_address from {{ ref('tokens_ethereum_erc20_stablecoins_legacy') }})
        and token_sold_address in (select contract_address from {{ ref('tokens_ethereum_erc20_stablecoins_legacy') }})
        UNION ALL
        select tx_hash, evt_index, project, version
        from {{ ref('dex_trades_legacy') }}
        where blockchain = 'ethereum'
        and token_bought_address in (select contract_address from {{ ref('tokens_ethereum_erc20_stablecoins_legacy') }})
        and token_sold_address in (select contract_address from {{ ref('tokens_ethereum_erc20_stablecoins_legacy') }})
    )
 )

select
  "ethereum" as blockchain,
  concat(tx_hash, CAST(evt_index AS VARCHAR(100)), project, version) as tx_hash_key,
  "Stable to stable" AS name,
  "tx_hash" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  CAST('2022-11-16' AS TIMESTAMP) as created_at,
  now() as updated_at,
  "stable_to_stable" as model_name,
  "usage" as label_type
from
  stable_to_stable_trades
