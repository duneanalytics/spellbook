{% macro balances_erc1155_hour(transfers_erc1155_rolling_hour, init_date) %}

with

years as (
    select year
    from (
          values (
            sequence(timestamp '{{init_date}}', cast(date_trunc('year', now()) as timestamp), interval '1' year)
          )
        ) s(year_array)
      cross join unnest(year_array) as d(year)
),

hours as (
    select date_add('hour', s.n, y.year) as block_hour
    from years y
      cross join unnest(sequence(1, 9000)) s(n)
    where s.n <= date_diff('hour', y.year, y.year + interval '1' year)
),

token_first_acquired as (
    select
      blockchain,
      wallet_address,
      token_address,
      token_id,
      min(block_hour) as first_block_hour
    from {{ transfers_erc1155_rolling_hour }}
    where 1=1
      {% if is_incremental() %} -- this filter will only be applied on an incremental run
      and block_hour >= date_trunc('day', now() - interval '7' day)
      {% endif %}
    group by 1, 2, 3, 4
),

token_fill_hours as (
    select
      tfa.blockchain,
      tfa.wallet_address,
      tfa.token_address,
      tfa.token_id,
      cast(date_trunc('month', h.block_hour) as date) as block_month,
      h.block_hour
    from token_first_acquired tfa
      join hours h on tfa.first_block_hour <= h.hour
),

daily_balances as (
    select
      t.wallet_address,
      t.token_address,
      t.token_id,
      t.amount,
      t.block_hour,
      lead(t.block_hour, 1, now() + interval '1' hour) over ( -- now + 1 hour so that last hour..
        partition by t.token_address, t.wallet_address order by t.block_hour
      ) - interval '1' hour as next_hour -- .. becomes hour-1 so it covers 'between' hours excatly in the next query
    from {{ transfers_erc1155_rolling_hour }} t
    where 1=1
      {% if is_incremental() %} -- this filter will only be applied on an incremental run
      and t.block_hour >= date_trunc('day', now() - interval '7' day)
      {% endif %}
)

select
  fh.blockchain,
  fh.block_month,
  fh.block_hour,
  fh.wallet_address,
  fh.token_address,
  db.token_id,
  db.amount,
  nft_tokens.name as collection
from token_fill_hours fh
  join daily_balances db on fh.wallet_address = db.wallet_address
    and fh.token_address = db.token_address
    and fh.token_id = db.token_id
    and fh.block_hour between db.block_hour and db.next_hour
  left join {{ ref('tokens_nft') }} nft_tokens on db.token_address = nft_tokens.contract_address
    and fh.blockchain = nft_tokens.blockchain

{% endmacro %}
