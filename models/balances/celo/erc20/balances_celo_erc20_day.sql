{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc20_day'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_day', 'wallet_address', 'token_address'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["soispoke", "dot2dotseurat", "tomfutago"]\') }}'
    )
}}

with

days as (
    select *
    from (select sequence(timestamp '2020-04-22', cast(date_trunc('day', now()) as timestamp), interval '1' day) as day) s
      cross join unnest(day) as s(day)
),

daily_balances as (
    select
      wallet_address,
      token_address,
      symbol,
      amount_raw,
      amount,
      block_month,
      block_day,
      lead(day, 1, now()) over (partition by token_address, wallet_address order by block_day) AS next_day
    from {{ ref('transfers_celo_erc20_rolling_day') }}
)

select
  'celo' as blockchain,
  b.block_month,
  b.block_day,
  b.wallet_address,
  b.token_address,
  b.symbol,
  b.amount_raw,
  b.amount,
  b.amount * p.price as amount_usd,
  row_number() over (partition by b.token_address, b.wallet_address order by b.block_day desc) as recency_index
from days d
  join daily_balances b on b.block_day <= d.day and d.day < b.next_day
  left join {{ source('prices', 'usd') }} p
    on p.contract_address = b.token_address
    and d.day = p.minute
    and p.blockchain = 'celo'
  -- Removes rebase tokens from balances
  --left join ref('tokens_celo_rebase') r on b.token_address = r.contract_address
  -- Removes likely non-compliant tokens due to negative balances
  left join {{ ref('balances_celo_erc20_noncompliant') }} nc on b.token_address = nc.token_address
where nc.token_address is null
  --and r.contract_address is null
