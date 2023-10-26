{{ 
    config(
        
        alias = 'erc20_rolling_day',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['wallet_address', 'token_address', 'block_day'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "transfers",
                                    \'["tomfutago"]\') }}'
    )
}}

select
  'celo' as blockchain,
  block_month,
  block_day,
  wallet_address,
  token_address,
  symbol,
  sum(amount_raw) over (partition by token_address, wallet_address order by block_day) as amount_raw,
  sum(amount) over (partition by token_address, wallet_address order by block_day) as amount,
  row_number() over (partition by token_address, wallet_address order by block_day desc) as recency_index,
  now() as last_updated
from {{ ref('transfers_celo_erc20_agg_day') }}
