{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc721_day'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_day', 'wallet_address', 'token_address', 'token_id'],
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
      blockchain,
      wallet_address,
      token_address,
      token_id,
      min(block_day) as first_block_day
    from {{ ref('transfers_celo_erc721_rolling_day') }}
    group by 1, 2, 3, 4
),

token_fill_days as (
    select
      tfa.blockchain,
      tfa.wallet_address,
      tfa.token_address,
      tfa.token_id,
      cast(date_trunc('month', d.day) as date) as block_month,
      d.day as block_day
    from token_first_acquired tfa
      join days d on tfa.first_block_day <= d.day
),

daily_balances as (
    select
      t.wallet_address,
      t.token_address,
      t.token_id,
      t.amount,
      t.block_day,
      lead(t.block_day, 1, now() + interval '1' day) over ( -- now + 1 day so that last day..
        partition by t.token_address, t.wallet_address order by t.block_day
      ) - interval '1' day as next_day -- .. becomes today's and -1 so it covers 'between' days excatly in the next query
    from {{ ref('transfers_celo_erc721_rolling_day') }} t
      left join {{ ref('balances_celo_erc721_noncompliant') }} nc on t.token_address = nc.token_address
    where 1=1
      and nc.token_address is null
      {% if is_incremental() %} -- this filter will only be applied on an incremental run
      and t.block_day >= date_trunc('day', now() - interval '7' day)
      {% endif %}
)

select
  fd.blockchain,
  fd.block_month,
  fd.block_day,
  fd.wallet_address,
  fd.token_address,
  fd.token_id,
  nft_tokens.name as collection
from token_fill_days fd
  join daily_balances db on fd.wallet_address = db.wallet_address
    and fd.token_address = db.token_address
    and fd.token_id = db.token_id
    and fd.block_day between db.block_day and db.next_day
  left join {{ ref('tokens_nft') }} nft_tokens on db.token_address = nft_tokens.contract_address
    and fd.blockchain = nft_tokens.blockchain
where db.amount = 1
