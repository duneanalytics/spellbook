{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc1155_rolling_day'),
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "sector",
                                    "transfers",
                                    \'["tomfutago"]\') }}'
    )
}}

select
  'avalanche_c' as blockchain,
  block_month,
  block_day,
  wallet_address,
  token_address,
  token_id,
  sum(amount) over (partition by token_address, wallet_address, token_id order by block_day) as amount,
  row_number() over (partition by token_address, wallet_address, token_id order by block_day desc) as recency_index,
  now() as last_updated
from {{ ref('transfers_avalanche_c_erc1155_agg_day') }}
