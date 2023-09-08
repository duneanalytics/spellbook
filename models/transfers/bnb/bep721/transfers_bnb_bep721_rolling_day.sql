{{ 
    config(
        tags = ['dunesql'],
        alias = alias('bep721_rolling_day'),
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "transfers",
                                    \'["tomfutago"]\') }}'
    )
}}

select
  'bnb' as blockchain,
  block_month,
  block_day,
  wallet_address,
  token_address,
  token_id,
  sum(amount) over (partition by token_address, wallet_address, token_id order by block_day) as amount,
  row_number() over (partition by token_address, wallet_address, token_id order by block_day desc) as recency_index,
  now() as last_updated
from {{ ref('transfers_bnb_bep721_agg_day') }}
