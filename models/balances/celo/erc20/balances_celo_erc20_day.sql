{{ 
    config(
        tags = ['dunesql'],
        schema = 'balances_celo',
        alias = alias('erc20_day'),
        unique_key = ['block_day', 'wallet_address', 'token_address'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["tomfutago"]\') }}'
    )
}}

with

days as (
    select day as block_day
    from (
          values (
            sequence(timestamp '2020-04-22', cast(date_trunc('day', now()) as timestamp), interval '1' day)
          )
        ) s(date_array)
      cross join unnest(date_array) as d(day)
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
      block_day,
      lead(block_day, 1, now() + interval '1' day) over ( -- now + 1 day so that last day..
        partition by token_address, wallet_address order by block_day
      ) - interval '1' day as next_day -- .. becomes today's and -1 so it covers 'between' days excatly in next cte
    from {{ ref('transfers_celo_erc20_rolling_day') }}
    where 1=1
)

select
  b.blockchain,
  cast(date_trunc('month', d.block_day) as date) as block_month,
  d.block_day,
  b.wallet_address,
  b.token_address,
  b.symbol,
  b.amount_raw,
  b.amount,
  b.amount * p.price as amount_usd,
  row_number() over (partition by b.token_address, b.wallet_address order by b.block_day desc) as recency_index
from daily_balances b
  join days d on d.block_day between b.block_day and b.next_day
  left join {{ source('prices', 'usd') }} p
    on b.token_address = p.contract_address
    and b.block_day = p.minute
    and p.blockchain = 'celo'
  -- Removes likely non-compliant tokens due to negative balances
  left join {{ ref('balances_celo_erc20_noncompliant') }} nc on b.token_address = nc.token_address
where nc.token_address is null
