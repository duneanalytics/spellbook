{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc20_latest'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['wallet_address', 'token_address'],
        post_hook='{{ expose_spells_hide_trino(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["tomfutago"]\') }}'
    )
}}

-- placeholder until hourly balance fully built
select 1

/*
select
  blockchain,
  block_month,
  wallet_address,
  token_address,
  symbol,
  amount_raw,
  amount,
  amount_usd,
  now() as last_updated
from {{ ref('balances_celo_erc20_hour') }}
where recency_index = 1
*/
