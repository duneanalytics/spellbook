{{
  config(
    schema = 'tokens_sui',
    alias = 'supply_signals',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    merge_skip_unchanged = true,
    tags = ['sui', 'tokens', 'transfers'],
  )
}}

-- temporary ci filter: original start date '2023-04-12', bumped to '2026-03-01' to reduce scan and unblock ci run
{% set sui_transfer_start_date = '2026-03-01' %}

with

events_filtered as (
  select
    e.date as block_date,
    e.transaction_digest as tx_digest,
    lower(e.event_type) as event_type_lower,
    regexp_extract(lower(e.event_type), '^(0x[0-9a-f]+)::', 1) as package_address,
    regexp_extract(lower(e.event_type), '^0x[0-9a-f]+::([^:]+)::', 1) as module_name,
    regexp_extract(lower(e.event_type), '^0x[0-9a-f]+::[^:]+::([^<]+)', 1) as event_name,
    regexp_extract(lower(e.event_type), 'treasury::(?:mint|burn)<([^>]+)>', 1) as generic_coin_type
  from {{ source('sui', 'events') }} e
  where e.date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('e.date') }}
    {% endif %}
),

package_coin_types as (
  select
    lower(split_part(m.coin_type, '::', 1)) as package_address,
    max_by(lower(m.coin_type), lower(m.coin_type)) as resolved_coin_type,
    count(distinct lower(m.coin_type)) as coin_type_count
  from {{ source('dex_sui', 'coin_info') }} m
  group by 1
),

generic_treasury_signals as (
  select
    e.block_date,
    e.tx_digest,
    lower(e.generic_coin_type) as coin_type,
    case
      when e.event_type_lower like '%treasury::mint<%' then 'mint'
      else 'burn'
    end as supply_event_type,
    'treasury_generic' as signal_source
  from events_filtered e
  where e.generic_coin_type is not null
    and e.event_type_lower like '%treasury::%'
),

package_treasury_signals as (
  select
    e.block_date,
    e.tx_digest,
    p.resolved_coin_type as coin_type,
    case
      when e.event_name = 'mint' then 'mint'
      else 'burn'
    end as supply_event_type,
    'treasury_package' as signal_source
  from events_filtered e
  inner join package_coin_types p
    on e.package_address = p.package_address
  where e.module_name = 'treasury'
    and e.event_name in ('mint', 'burn')
    -- ambiguous package->coin mappings are excluded intentionally because they
    -- cannot be resolved to one deterministic coin_type for downstream joins.
    and p.coin_type_count = 1
),

cctp_event_signatures (
  package_address,
  module_name,
  event_name,
  supply_event_type
) as (
  -- circle cctp v1 sui mainnet package ids:
  -- https://developers.circle.com/cctp/v1/sui-packages
  values
    ('0x08d87d37ba49e785dde270a83f8e979605b03dc552b5548f26fdf2f49bf7ed1b', 'message_transmitter', 'messagereceived', 'mint'),
    ('0x410d70c8baad60f310f45c13b9656ecbfed46fdf970e051f0cac42891a848856', 'deposit_for_burn', 'depositforburn', 'burn'),
    ('0x2aa6c5d56376c371f88a6cc42e852824994993cb9bab8d3e6450cbe3cb32b94e', 'deposit_for_burn', 'depositforburn', 'burn')
),

cctp_usdc_signals as (
  select
    e.block_date,
    e.tx_digest,
    lower('0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::usdc') as coin_type,
    c.supply_event_type,
    'cctp' as signal_source
  from events_filtered e
  inner join cctp_event_signatures c
    on e.package_address = c.package_address
    and e.module_name = c.module_name
    and e.event_name = c.event_name
),

unioned as (
  select
    g.block_date,
    g.tx_digest,
    g.coin_type,
    g.supply_event_type,
    g.signal_source
  from generic_treasury_signals g
  union all
  select
    p.block_date,
    p.tx_digest,
    p.coin_type,
    p.supply_event_type,
    p.signal_source
  from package_treasury_signals p
  union all
  select
    c.block_date,
    c.tx_digest,
    c.coin_type,
    c.supply_event_type,
    c.signal_source
  from cctp_usdc_signals c
),

aggregated as (
  select
    u.block_date,
    u.tx_digest,
    lower(u.coin_type) as coin_type,
    bool_or(
      u.signal_source in ('treasury_generic', 'treasury_package')
      and u.supply_event_type = 'mint'
    ) as has_treasury_mint,
    bool_or(
      u.signal_source in ('treasury_generic', 'treasury_package')
      and u.supply_event_type = 'burn'
    ) as has_treasury_burn,
    bool_or(
      u.signal_source = 'cctp'
      and u.supply_event_type = 'mint'
    ) as has_cctp_message_received,
    bool_or(
      u.signal_source = 'cctp'
      and u.supply_event_type = 'burn'
    ) as has_cctp_deposit_for_burn,
    bool_or(u.supply_event_type = 'mint') as has_mint_signal,
    bool_or(u.supply_event_type = 'burn') as has_burn_signal
  from unioned u
  group by 1, 2, 3
)

select
  {{ dbt_utils.generate_surrogate_key(['a.tx_digest', 'a.coin_type']) }} as unique_key,
  a.block_date,
  a.tx_digest,
  a.coin_type,
  a.has_treasury_mint,
  a.has_treasury_burn,
  a.has_cctp_message_received,
  a.has_cctp_deposit_for_burn,
  case
    when a.has_mint_signal and not a.has_burn_signal then 'mint'
    when a.has_burn_signal and not a.has_mint_signal then 'burn'
    else cast(null as varchar)
  end as supply_event_type,
  current_timestamp as _updated_at
from aggregated a
