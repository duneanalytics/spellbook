{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc20_latest'),
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
select
  'celo' as blockchain,
  0x0000000000000000000000000000000000000000 as wallet_address,
  0x0000000000000000000000000000000000000000 as token_address,
  'XXX' as symbol,
  0 as amount_raw,
  0 as amount,
  0 as amount_usd,
  now() as last_updated

/*
select
  blockchain,
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
