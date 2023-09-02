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
                                    \'["soispoke", "dot2dotseurat", "tomfutago"]\') }}'
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

daily_balances as (
    select
      wallet_address,
      token_address,
      symbol,
      amount_raw,
      amount,
      block_month,
      block_hour,
      lead(block_hour, 1, now()) over (partition by token_address, wallet_address order by block_hour) AS next_hour
    from {{ ref('transfers_celo_erc20_rolling_hour') }}
    where 1=1
      {% if is_incremental() %} -- this filter will only be applied on an incremental run
      and block_hour >= date_trunc('day', now() - interval '3' day)
      {% endif %}
)

select
  'celo' as blockchain,
  b.block_month,
  h.hour as block_hour,
  b.wallet_address,
  b.token_address,
  b.symbol,
  b.amount_raw,
  b.amount,
  b.amount * p.price as amount_usd,
  row_number() over (partition by b.token_address, b.wallet_address order by b.block_hour desc) as recency_index
from hours h
  join daily_balances b on b.block_hour <= h.hour and h.hour < b.next_hour
  left join {{ source('prices', 'usd') }} p
    on p.contract_address = b.token_address
    and h.hour = p.minute
    and p.blockchain = 'celo'
  -- Removes likely non-compliant tokens due to negative balances
  left join {{ ref('balances_celo_erc20_noncompliant') }} nc on b.token_address = nc.token_address
where nc.token_address is null
