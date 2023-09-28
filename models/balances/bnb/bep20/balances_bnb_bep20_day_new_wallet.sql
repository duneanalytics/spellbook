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
  first_day_wallet as (
    select
      wallet_address,
      token_address,
      min(block_day) as first_day
    from
      {{ ref('balances_bnb_bep20_day') }
    group by
      wallet_address,
      token_address
  ),
  balances_new_wallet as (
    select
        b.wallet_address,
        b.token_address,
        block_day,
        amount_raw ra
    from
        {{ ref('balances_bnb_bep20_day') }} b
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
   token_holder as (
    select block_day,
      token_address,
      count(*) as number_holder
      from {{ ref('balances_bnb_bep20_day') }}
      where amount_raw > 0
      group by token_address, block_day

   ),
   agg_token as (
    select n.block_day, n.token_address
      n.volumn_new_wallets, n.number_new_wallets
      h.number_holder
      from token_holder h
      left join agg_new_wallets n
      on h.token_address = n.token_address
      and h.block_day = n.block_day
   )

select
  d.block_day,
  d.token_address,
  d.number_holder,
  d.number_new_wallets,
  d.volumn_new_wallets / power(10, 18) as volumn_new_wallets,
  p.price
from
  agg_token d
  left join prices.usd p on d.block_day = p.minute
  and p.contract_address = d.token_address
  and p.blockchain = 'bnb'

