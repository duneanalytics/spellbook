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
  balances_filter as (
    select wallet_address, token_address, block_day, amount_raw from {{ ref('balances_bnb_bep20_day') }}
    where block_day > date('2023-01-01')
  ), 
  first_day_wallet as (
    select
      wallet_address,
      token_address,
      min(block_day) as first_day
    from
      balances_filter
    group by
      wallet_address,
      token_address
  ),
  balances_new_wallet as (
    select
        b.wallet_address,
        b.token_address,
        block_day,
        amount_raw
    from
        balances_filter b
        inner join first_day_wallet f on b.wallet_address = f.wallet_address
        and b.token_address = f.token_address
        and f.first_day >= b.block_day - interval '14' Day
  ),
  agg_new_wallets as (
    select
      block_day,
      token_address,
      sum(amount_raw) as volumn_new_wallets,
      count(*) as number_new_wallets
    from
      balances_new_wallet
    group by
      block_day,
      token_address
  ),
  price_filter as (
    select contract_address, price, minute
    from  {{ source('prices', 'usd') }}
    where minute > date('2023-01-01')
    and blockchain = 'bnb'
  )

select
  d.block_day,
  d.token_address,
  d.number_new_wallets,
  d.volumn_new_wallets / power(10, 18) as volumn_new_wallets,
  p.price
from
  agg_new_wallets d
  left join price_filter p on d.block_day = p.minute
  and p.contract_address = d.token_address

