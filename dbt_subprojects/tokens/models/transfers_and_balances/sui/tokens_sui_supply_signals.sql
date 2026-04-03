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
    tags = ['prod_exclude'],
  )
}}

{% set sui_transfer_start_date = '2023-04-12' %}

with

events_filtered as (
  select
    e.date as block_date,
    e.transaction_digest as tx_digest,
    regexp_extract(lower(e.event_type), '^(0x[0-9a-f]+)::', 1) as package_address,
    regexp_replace(
      regexp_extract(lower(e.event_type), '^(0x[0-9a-f]+)::', 1),
      '^0x0*([0-9a-f]+)$',
      '0x$1'
    ) as package_address_normalized,
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
    min(lower(m.coin_type)) as resolved_coin_type,
    count(distinct lower(m.coin_type)) as coin_type_count
  from {{ ref('sui_coin_info') }} m
  group by 1
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

signals as (
  select
    e.block_date,
    e.tx_digest,
    regexp_replace(
      lower(
        case
          when c.supply_event_type is not null then '0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::usdc'
          when e.generic_coin_type is not null then e.generic_coin_type
          else p.resolved_coin_type
        end
      ),
      '^0x0*([0-9a-f]+)(::.*)$',
      '0x$1$2'
    ) as coin_type,
    case
      when c.supply_event_type is not null then c.supply_event_type
      else e.event_name
    end as supply_event_type
  from events_filtered e
  left join cctp_event_signatures c
    on e.package_address = c.package_address
    and e.module_name = c.module_name
    and e.event_name = c.event_name
  left join package_coin_types p
    -- ambiguous package->coin mappings are excluded intentionally because they
    -- cannot be resolved to one deterministic coin_type for downstream joins.
    on e.package_address_normalized = p.package_address
    and p.coin_type_count = 1
  where c.supply_event_type is not null
    or (
      e.module_name = 'treasury'
      and e.event_name in ('mint', 'burn')
      and (e.generic_coin_type is not null or p.resolved_coin_type is not null)
    )
),

aggregated as (
  select
    s.block_date,
    s.tx_digest,
    s.coin_type,
    bool_or(s.supply_event_type = 'mint') as has_mint_signal,
    bool_or(s.supply_event_type = 'burn') as has_burn_signal
  from signals s
  where s.coin_type is not null
  group by 1, 2, 3
)

select
  {{ dbt_utils.generate_surrogate_key(['a.tx_digest', 'a.coin_type']) }} as unique_key,
  a.block_date,
  a.tx_digest,
  a.coin_type,
  case
    when a.has_mint_signal and not a.has_burn_signal then 'mint'
    when a.has_burn_signal and not a.has_mint_signal then 'burn'
    else cast(null as varchar)
  end as supply_event_type,
  current_timestamp as _updated_at
from aggregated a
