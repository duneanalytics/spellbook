{{ 
    config(
        tags = ['dunesql'],
        schema = 'balances_celo',
        alias = alias('erc20_latest'),
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["tomfutago"]\') }}'
    )
}}

select
  blockchain,
  wallet_address,
  token_address,
  symbol,
  max_by(amount_raw, block_hour) as amount_raw,
  max_by(amount, block_hour) as amount,
  max_by(amount_usd, block_hour) as amount_usd,
  max(block_hour) as last_updated
from {{ ref('balances_celo_erc20_hour') }}
group by 1,2,3,4
