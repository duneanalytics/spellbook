{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc721_day'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_day', 'wallet_address', 'token_address'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["tomfutago"]\') }}'
    )
}}

with

days as (
    select day
    from (
          values (
            sequence(timestamp '2020-04-22', cast(date_trunc('day', now()) as timestamp), interval '1' day)
          )
        ) s(date_array)
      cross join unnest(date_array) as d(day)
),

token_first_acquired as (
    select
      wallet_address,
      token_address,
      min(block_day) as first_block_day
    from {{ ref('transfers_celo_erc721_rolling_day') }}
    group by 1, 2
),

token_fill_days as (
    select
      tfa.wallet_address,
      tfa.token_address,
      cast(date_trunc('month', d.day) as date) as block_month,
      d.day as block_day
    from token_first_acquired tfa
      join days d on tfa.first_block_day <= d.day
),

daily_balances as (
    select
      wallet_address,
      token_address,
      token_id,
      amount,
      block_month,
      block_day,
      lead(block_day, 1, now() + interval '1' day) over ( -- now + 1 day so that last day..
        partition by token_address, wallet_address order by block_day
      ) - interval '1' day as next_day -- .. becomes today's and -1 so it covers 'between' days excatly in next cte
    from {{ ref('transfers_celo_erc721_rolling_day') }} t
      left join {{ ref('balances_celo_erc721_noncompliant') }} nc on t.token_address = nc.token_address
    where 1=1
      and nc.token_address is null
      {% if is_incremental() %} -- this filter will only be applied on an incremental run
      and block_day >= date_trunc('day', now() - interval '7' day)
      {% endif %}
)

select
  'celo' as blockchain,
  fd.block_month,
  fd.block_day,
  fd.wallet_address,
  fd.token_address,
  db.token_id,
  nft_tokens.name as collection,
  row_number() over (partition by fd.token_address, fd.wallet_address order by fd.block_day desc) as recency_index
from token_fill_days fd
  join daily_balances db on fd.wallet_address = db.wallet_address and fd.token_address = db.token_address
    and fd.block_day between db.block_day and db.next_day
  left join {{ ref('tokens_nft') }} nft_tokens on nft_tokens.contract_address = b.token_address
    and nft_tokens.blockchain = 'celo'
where b.amount = 1
