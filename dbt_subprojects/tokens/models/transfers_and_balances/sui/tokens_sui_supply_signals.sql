{{
  config(
    schema = 'tokens_sui',
    alias = 'supply_signals',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    merge_skip_unchanged = true,
    tags = ['sui', 'tokens', 'transfers'],
  )
}}

{% set sui_transfer_start_date = '2023-04-12' %}

with

events_filtered as (
  select
    e.date as block_date,
    e.transaction_digest as tx_digest,
    lower(e.event_type) as event_type_lower,
    lower(regexp_extract(e.event_type, '^(0x[0-9a-f]+)::', 1)) as package_address,
    lower(regexp_extract(e.event_type, '^0x[0-9a-f]+::([^:]+)::', 1)) as module_name,
    lower(regexp_extract(e.event_type, '^0x[0-9a-f]+::[^:]+::([^<]+)', 1)) as event_name,
    regexp_extract(lower(e.event_type), 'treasury::(?:mint|burn)<([^>]+)>', 1) as generic_coin_type
  from {{ source('sui', 'events') }} e
  where e.date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() -%}
    and {{ incremental_predicate('e.date') }}
    {% endif -%}
),

package_coin_types as (
  select
    lower(split_part(m.coin_type, '::', 1)) as package_address,
    lower(m.coin_type) as coin_type
  from {{ source('dex_sui', 'coin_info') }} m
),

unambiguous_package_coin_types as (
  select
    p.package_address,
    max_by(p.coin_type, p.coin_type) as coin_type
  from package_coin_types p
  group by 1
  having count(distinct p.coin_type) = 1
),

generic_treasury_signals as (
  select
    e.block_date,
    e.tx_digest,
    e.generic_coin_type as coin_type,
    case
      when e.event_type_lower like '%treasury::mint<%' then true
      else false
    end as has_treasury_mint,
    case
      when e.event_type_lower like '%treasury::burn<%' then true
      else false
    end as has_treasury_burn,
    false as has_cctp_message_received,
    false as has_cctp_deposit_for_burn
  from events_filtered e
  where e.generic_coin_type is not null
),

package_treasury_signals as (
  select
    e.block_date,
    e.tx_digest,
    p.coin_type,
    case
      when e.module_name = 'treasury' and e.event_name = 'mint' then true
      else false
    end as has_treasury_mint,
    case
      when e.module_name = 'treasury' and e.event_name = 'burn' then true
      else false
    end as has_treasury_burn,
    false as has_cctp_message_received,
    false as has_cctp_deposit_for_burn
  from events_filtered e
  inner join unambiguous_package_coin_types p
    on e.package_address = p.package_address
  where e.module_name = 'treasury'
    and e.event_name in ('mint', 'burn')
),

cctp_usdc_signals as (
  select
    e.block_date,
    e.tx_digest,
    lower('0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::usdc') as coin_type,
    false as has_treasury_mint,
    false as has_treasury_burn,
    case
      when e.module_name = 'message_transmitter' and e.event_name = 'messagereceived' then true
      else false
    end as has_cctp_message_received,
    case
      when e.module_name = 'deposit_for_burn' and e.event_name = 'depositforburn' then true
      else false
    end as has_cctp_deposit_for_burn
  from events_filtered e
  where (e.module_name, e.event_name) in (
    ('message_transmitter', 'messagereceived'),
    ('deposit_for_burn', 'depositforburn')
  )
),

unioned as (
  select
    g.block_date,
    g.tx_digest,
    g.coin_type,
    g.has_treasury_mint,
    g.has_treasury_burn,
    g.has_cctp_message_received,
    g.has_cctp_deposit_for_burn
  from generic_treasury_signals g
  union all
  select
    p.block_date,
    p.tx_digest,
    p.coin_type,
    p.has_treasury_mint,
    p.has_treasury_burn,
    p.has_cctp_message_received,
    p.has_cctp_deposit_for_burn
  from package_treasury_signals p
  union all
  select
    c.block_date,
    c.tx_digest,
    c.coin_type,
    c.has_treasury_mint,
    c.has_treasury_burn,
    c.has_cctp_message_received,
    c.has_cctp_deposit_for_burn
  from cctp_usdc_signals c
)

select
  {{ dbt_utils.generate_surrogate_key(['u.tx_digest', 'u.coin_type']) }} as unique_key,
  u.block_date,
  u.tx_digest,
  u.coin_type,
  bool_or(u.has_treasury_mint) as has_treasury_mint,
  bool_or(u.has_treasury_burn) as has_treasury_burn,
  bool_or(u.has_cctp_message_received) as has_cctp_message_received,
  bool_or(u.has_cctp_deposit_for_burn) as has_cctp_deposit_for_burn,
  current_timestamp as _updated_at
from unioned u
group by 1, 2, 3, 4
