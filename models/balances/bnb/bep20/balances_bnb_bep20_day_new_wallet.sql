{{ config(
        tags = ['dunesql'],
        alias = alias('bep20_day_new_wallet'),
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "balances",
                                    \'["longnhbkhn"]\') }}'
        )
}}
with
  balances_token as (
    select
      token_address,
      wallet_address,
      block_day,
      amount
    from
       {{ ref('balances_bnb_bep20_day') }}
    where
      block_day >= date_trunc('day', now() - interval '30' Day)
  ),
  first_day_wallet as (
    select
      wallet_address,
      token_address,
      min(block_day) as first_day
    from
      {{ ref('balances_bnb_bep20_day') }}
    where
      block_day >= date_trunc('day', now() - interval '45' Day)
    group by
      wallet_address,
      token_address
  )

select
    b.wallet_address,
    b.token_address,
    block_day,
    amount
from
    balances_token b
    inner join first_day_wallet f on b.wallet_address = f.wallet_address
    and b.token_address = f.token_address
    and f.first_day >= b.block_day - interval '14' Day