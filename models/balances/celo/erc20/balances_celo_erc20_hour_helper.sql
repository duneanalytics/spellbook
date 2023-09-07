{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc20_hour_helper'),
        post_hook='{{ expose_spells_hide_trino(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["tomfutago"]\') }}'
    )
}}

-- placeholder to build balances in stages (dayily first)
select
  0x0000000000000000000000000000000000000000 as wallet_address,
  0x0000000000000000000000000000000000000000 as token_address,
  now() as block_month,
  now() as block_hour

/*
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
    where 1=1
      {% if is_incremental() %} -- this filter will only be applied on an incremental run
      and block_hour >= date_trunc('day', now() - interval '7' day)
      {% endif %}
    group by 1, 2
)

select
  tfa.wallet_address,
  tfa.token_address,
  cast(date_trunc('month', h.hour) as date) as block_month,
  h.hour as block_hour
from token_first_acquired tfa
  join hours h on tfa.first_block_hour <= h.hour
*/
