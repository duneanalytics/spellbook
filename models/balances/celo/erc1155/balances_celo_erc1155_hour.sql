{{ 
    config(
        tags = ['dunesql'],
        alias = alias('erc1155_hour'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_hour', 'wallet_address', 'token_address'],
        post_hook='{{ expose_spells_hide_trino(\'["celo"]\',
                                    "sector",
                                    "balances",
                                    \'["tomfutago"]\') }}'
    )
}}

with

daily_balances as (
    select
      t.wallet_address,
      t.token_address,
      t.token_id,
      t.amount,
      t.block_month,
      t.block_hour,
      lead(t.block_hour, 1, now() + interval '1' hour) over ( -- now + 1 hour so that last hour..
        partition by t.token_address, t.wallet_address order by t.block_hour
      ) - interval '1' hour as next_hour -- .. becomes hour-1 so it covers 'between' hours excatly in next query
    from {{ ref('transfers_celo_erc1155_rolling_day') }} t
    where 1=1
      {% if is_incremental() %} -- this filter will only be applied on an incremental run
      and t.block_hour >= date_trunc('day', now() - interval '7' day)
      {% endif %}
)

select
  'celo' as blockchain,
  hh.block_month,
  hh.block_hour,
  hh.wallet_address,
  hh.token_address,
  db.token_id,
  db.amount,
  nft_tokens.name as collection
from {{ ref('balances_celo_erc1155_hour_helper') }} hh
  join daily_balances db on hh.wallet_address = db.wallet_address and hh.token_address = db.token_address
    and hh.block_hour between db.block_hour and db.next_hour
  left join {{ ref('tokens_nft') }} nft_tokens on db.token_address = nft_tokens.contract_address
    and nft_tokens.blockchain = 'celo'
where db.amount = 1
