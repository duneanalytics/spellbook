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

daily_balances as (
    select
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
    where 1=1
      {% if is_incremental() %} -- this filter will only be applied on an incremental run
      and block_hour >= date_trunc('day', now() - interval '7' day)
      {% endif %}
)

select
  'celo' as blockchain,
  hh.block_month,
  hh.block_hour,
  hh.wallet_address,
  hh.token_address,
  db.symbol,
  db.amount_raw,
  db.amount,
  db.amount * p.price as amount_usd,
  row_number() over (partition by hh.token_address, hh.wallet_address order by hh.block_hour desc) as recency_index
from {{ ref('balances_celo_erc20_hour_helper') }} hh
  join daily_balances db on hh.wallet_address = db.wallet_address and hh.token_address = db.token_address
    and hh.block_hour between db.block_hour and db.next_hour
  left join {{ source('prices', 'usd') }} p
    on db.token_address = p.contract_address
    and hh.block_hour = p.minute
    and p.blockchain = 'celo'
  -- Removes likely non-compliant tokens due to negative balances
  left join {{ ref('balances_celo_erc20_noncompliant') }} nc on hh.token_address = nc.token_address
where nc.token_address is null
