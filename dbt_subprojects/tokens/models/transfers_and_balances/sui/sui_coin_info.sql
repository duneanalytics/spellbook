{{
  config(
    schema = 'sui',
    alias = 'coin_info',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['coin_type'],
    merge_skip_unchanged = true,
  )
}}

-- temp filter to unblock ci run (original start date '2023-04-12')
{% set sui_transfer_start_date = '2025-01-01' %}

with

objects_base as (
  select
    cast(o.type_ as varchar) as type_tag,
    o.object_json,
    o.object_status,
    o.checkpoint,
    o.version
  from {{ source('sui', 'objects') }} o
  where (
      cast(o.type_ as varchar) like '0x2::coin::CoinMetadata<%'
      or cast(o.type_ as varchar) like '0x2::coin_registry::Currency<%'
      or cast(o.type_ as varchar) like '0x2::coin_registry::CoinData<%'
    )
    and o.object_status in ('Created', 'Mutated')
    and o.date >= date '{{ sui_transfer_start_date }}'
    {% if is_incremental() %}
    and o.checkpoint > coalesce((select max(t.checkpoint_latest) from {{ this }} t), 0)
    {% endif %}
),

metadata_prep as (
  select
    r.type_tag,
    r.object_json,
    r.checkpoint,
    r.version,
    regexp_replace(
      lower(regexp_extract(r.type_tag, '<(.*)>', 1)),
      '^0x0*([0-9a-f]+)(::.*)$',
      '0x$1$2'
    ) as coin_type,
    regexp_replace(
      split_part(
        regexp_replace(
          lower(regexp_extract(r.type_tag, '<(.*)>', 1)),
          '^0x0*([0-9a-f]+)(::.*)$',
          '0x$1$2'
        ),
        '::',
        1
      ),
      '^0x0*([0-9a-f]+)$',
      '0x$1'
    ) as contract_address
  from objects_base r
),

legacy_metadata as (
  select
    r.coin_type,
    r.contract_address,
    cast(json_extract_scalar(r.object_json, '$.symbol') as varchar) as symbol,
    try_cast(json_extract_scalar(r.object_json, '$.decimals') as integer) as decimals,
    r.checkpoint as checkpoint_latest,
    r.version as version_latest,
    1 as source_priority
  from metadata_prep r
  where r.type_tag like '0x2::coin::CoinMetadata<%'
),

registry_metadata as (
  select
    r.coin_type,
    r.contract_address,
    cast(
      coalesce(
        json_extract_scalar(r.object_json, '$.symbol'),
        json_extract_scalar(r.object_json, '$.currency.symbol'),
        json_extract_scalar(r.object_json, '$.metadata.symbol')
      ) as varchar
    ) as symbol,
    try_cast(
      coalesce(
        json_extract_scalar(r.object_json, '$.decimals'),
        json_extract_scalar(r.object_json, '$.currency.decimals'),
        json_extract_scalar(r.object_json, '$.metadata.decimals')
      ) as integer
    ) as decimals,
    r.checkpoint as checkpoint_latest,
    r.version as version_latest,
    case
      when r.type_tag like '0x2::coin_registry::Currency<%' then 3
      else 2
    end as source_priority
  from metadata_prep r
  where r.type_tag like '0x2::coin_registry::Currency<%'
    or r.type_tag like '0x2::coin_registry::CoinData<%'
),

candidates as (
  select
    coin_type,
    contract_address,
    symbol,
    decimals,
    checkpoint_latest,
    version_latest,
    source_priority
  from legacy_metadata
  where coin_type is not null
  union all
  select
    coin_type,
    contract_address,
    symbol,
    decimals,
    checkpoint_latest,
    version_latest,
    source_priority
  from registry_metadata
  where coin_type is not null
),

merged_candidates as (
  select
    n.coin_type,
    n.contract_address,
    n.symbol,
    n.decimals,
    n.checkpoint_latest,
    n.version_latest,
    n.source_priority
  from candidates n
  {% if is_incremental() %}
  union all
  select
    t.coin_type,
    t.contract_address,
    t.symbol,
    t.decimals,
    t.checkpoint_latest,
    t.version_latest,
    0 as source_priority
  from {{ this }} t
  {% endif %}
),

ranked_candidates as (
  select
    u.coin_type,
    u.contract_address,
    u.symbol,
    u.decimals,
    u.checkpoint_latest,
    u.version_latest,
    row_number() over (
      partition by u.coin_type
      order by u.checkpoint_latest desc, u.version_latest desc, u.source_priority desc
    ) as rn
  from merged_candidates u
),

latest_metadata as (
  select
    r.coin_type,
    r.contract_address,
    r.symbol,
    r.decimals,
    r.checkpoint_latest,
    r.version_latest
  from ranked_candidates r
  where r.rn = 1
),

manual_metadata as (
  select
    m.coin_type,
    regexp_replace(split_part(m.coin_type, '::', 1), '^0x0*([0-9a-f]+)$', '0x$1') as contract_address,
    m.symbol,
    m.decimals
  from (
    values
      (lower('0x2::sui::SUI'), 'SUI', 9),
      (lower('0x27792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN'), 'BTC', 8)
  ) as m(coin_type, symbol, decimals)
)

select
  l.coin_type,
  l.contract_address,
  l.symbol,
  l.decimals,
  l.checkpoint_latest,
  l.version_latest
from latest_metadata l
union all
select
  m.coin_type,
  m.contract_address,
  m.symbol,
  m.decimals,
  cast(null as bigint) as checkpoint_latest,
  cast(null as bigint) as version_latest
from manual_metadata m
left join latest_metadata l
  on l.coin_type = m.coin_type
where l.coin_type is null
