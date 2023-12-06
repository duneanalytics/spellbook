{{ 
    config(
        
        alias = 'erc20_rolling_hour',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['wallet_address', 'token_address', 'block_hour'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "transfers",
                                    \'["tomfutago"]\') }}'
    )
}}

select
  'celo' as blockchain,
  block_month,
  block_hour,
  wallet_address,
  token_address,
  symbol,
  sum(amount_raw) over (partition by token_address, wallet_address order by block_hour) as amount_raw,
  sum(amount) over (partition by token_address, wallet_address order by block_hour) as amount,
  row_number() over (partition by token_address, wallet_address order by block_hour desc) as recency_index,
  now() as last_updated
from {{ ref('transfers_celo_erc20_agg_hour') }}
