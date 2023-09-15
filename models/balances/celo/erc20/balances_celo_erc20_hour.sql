{{ 
    config(
        tags = ['dunesql'],
        schema = 'balances_celo',
        alias = alias('erc20_hour'),
        --unique_key = ['block_hour', 'wallet_address', 'token_address'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["tomfutago"]\') }}'
    )
}}

with

years as (
    select year
    from (
          values (
            sequence(timestamp '2020-01-01', cast(date_trunc('year', now()) as timestamp), interval '1' year)
          )
        ) s(year_array)
      cross join unnest(year_array) as h(year)
),

hours as (
    select date_add('hour', s.n, y.year) as block_hour
    from years y
      cross join unnest(sequence(1, 9000)) s(n)
    where s.n <= date_diff('hour', y.year, y.year + interval '1' year)
),

daily_balances as (
    select
      blockchain,
      wallet_address,
      token_address,
      symbol,
      amount_raw,
      amount,
      block_month,
      block_hour,
      lead(block_hour, 1, now() + interval '1' hour) over ( -- now + 1 hour so that last hour..
        partition by token_address, wallet_address order by block_hour
      ) - interval '1' hour as next_hour -- .. becomes hour-1 so it covers 'between' hours excatly in next cte
    from {{ ref('transfers_celo_erc20_rolling_hour') }}
)

select
  b.blockchain,
  cast(date_trunc('month', h.block_hour) as date) as block_month,
  h.block_hour,
  b.wallet_address,
  b.token_address,
  b.symbol,
  b.amount_raw,
  b.amount,
  b.amount * p.price as amount_usd
from daily_balances b
  join hours h on h.block_hour between b.block_hour and b.next_hour
  left join {{ source('prices', 'usd') }} p
    on b.token_address = p.contract_address
    and b.block_hour = p.minute
    and p.blockchain = 'celo'
  -- Removes likely non-compliant tokens due to negative balances
  left join {{ ref('balances_celo_erc20_noncompliant') }} nc on b.token_address = nc.token_address
where nc.token_address is null
