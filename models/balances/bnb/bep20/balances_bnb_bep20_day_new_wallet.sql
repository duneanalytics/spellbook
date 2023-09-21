{{ config(
        tags = ['dunesql'],
        materialized='table',
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
  ),
  balances_new_wallet as (
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
  ),
  volumn_new_wallets as (
    select
      block_day,
      token_address,
      sum(amount) as volumn
    from
      balances_new_wallet
    group by
      block_day,
      token_address
  ),
   avg_volume_new_wallets as (
    select
      token_address,
      avg(volumn) FILTER(
        where
          block_day < date_trunc('day', now() - interval '7' Day)
      ) as avg_volumn_before,
      avg(volumn) FILTER(
        where
          block_day >= date_trunc('day', now() - interval '7' Day)
      ) as avg_volumn_now
    from
      volumn_new_wallets
    group by
      token_address
  )

select
    token_address,
    avg_volumn_now / avg_volumn_before as volume_fluctuation_rate
from
    avg_volume_new_wallets

