{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc20_hour'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_hour', 'wallet_address', 'token_address'],
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
      cross join unnest(year_array) as d(year)
),

hours as (
    select date_add('hour', s.n, y.year) as hour
    from years y
      cross join unnest(sequence(0, 9000)) s(n)
    where s.n <= date_diff('hour', y.year, y.year + interval '1' year)
),

token_first_acquired as (
    select
      wallet_address,
      token_address,
      min(block_hour) as first_block_hour
    from {{ ref('transfers_celo_erc20_rolling_hour') }}
    group by 1, 2
),

token_fill_hours as (
    select
      tfa.wallet_address,
      tfa.token_address,
      cast(date_trunc('month', h.hour) as date) as block_month,
      h.hour as block_hour
    from token_first_acquired tfa
      join hours h on tfa.first_block_hour <= h.hour
),

daily_balances as (
    select
      wallet_address,
      token_address,
      symbol,
      amount_raw,
      amount,
      block_month,
      block_hour,
      lead(block_hour, 1, now() + interval '1' day) over ( -- now + 1 day so that last day..
        partition by token_address, wallet_address order by block_hour
      ) - interval '1' day as next_hour -- .. becomes today's and -1 so it covers 'between' days excatly in next cte
    from {{ ref('transfers_celo_erc20_rolling_hour') }}
    where 1=1
      {% if is_incremental() %} -- this filter will only be applied on an incremental run
      and block_hour >= date_trunc('day', now() - interval '7' day)
      {% endif %}
)

select
  'celo' as blockchain,
  fd.block_month,
  fd.block_hour,
  fd.wallet_address,
  fd.token_address,
  db.symbol,
  db.amount_raw,
  db.amount,
  db.amount * p.price as amount_usd,
  row_number() over (partition by fd.token_address, fd.wallet_address order by fd.block_hour desc) as recency_index
from token_fill_hours fd
  join daily_balances db on fd.wallet_address = db.wallet_address and fd.token_address = db.token_address
    and fd.block_hour between db.block_hour and db.next_hour
  left join {{ source('prices', 'usd') }} p
    on db.token_address = p.contract_address
    and fd.block_hour = p.minute
    and p.blockchain = 'celo'
  -- Removes likely non-compliant tokens due to negative balances
  left join {{ ref('balances_celo_erc20_noncompliant') }} nc on fd.token_address = nc.token_address
where nc.token_address is null
