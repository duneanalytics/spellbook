{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc721_latest'),
        post_hook='{{ expose_spells_hide_trino(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["tomfutago"]\') }}'
    )
}}

with

recent_balances as (
  select
    blockchain,
    wallet_address,
    token_address,
    token_id,
    collection,
    block_hour,
    row_number() over (partition by wallet_address, token_address order by block_hour desc) as recency_index
  from {{ ref('balances_celo_erc721_hour') }}
  where block_hour >= date_trunc('day', now() - interval '1' day) -- safety net to allow 1 day delay in balances refresh
)

select
  blockchain,
  wallet_address,
  token_address,
  token_id,
  collection, 
  block_hour as last_updated
from recent_balances
where recency_index = 1
