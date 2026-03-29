{{
  config(
    schema = 'sui',
    alias = 'coin_info',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['coin_type'],
    merge_skip_unchanged = true,
    tags = ['tokens', 'sui'],
  )
}}

with

object_rows as (
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
    {% if is_incremental() %}
    and o.checkpoint > coalesce((select max(t.checkpoint_latest) from {{ this }} t), 0)
    {% endif %}
),

legacy_coin_metadata as (
  select
    regexp_replace(
      lower(regexp_extract(r.type_tag, '<(.*)>', 1)),
      '^0x0*([0-9a-f]+)(::.*)$',
      '0x$1$2'
    ) as coin_type,
    cast(json_extract_scalar(r.object_json, '$.symbol') as varchar) as coin_symbol,
    try_cast(json_extract_scalar(r.object_json, '$.decimals') as integer) as coin_decimals,
    r.checkpoint as checkpoint_latest,
    r.version as version_latest,
    1 as source_priority
  from object_rows r
  where r.type_tag like '0x2::coin::CoinMetadata<%'
),

coin_registry_metadata as (
  select
    regexp_replace(
      lower(regexp_extract(r.type_tag, '<(.*)>', 1)),
      '^0x0*([0-9a-f]+)(::.*)$',
      '0x$1$2'
    ) as coin_type,
    cast(
      coalesce(
        json_extract_scalar(r.object_json, '$.symbol'),
        json_extract_scalar(r.object_json, '$.currency.symbol'),
        json_extract_scalar(r.object_json, '$.metadata.symbol')
      ) as varchar
    ) as coin_symbol,
    try_cast(
      coalesce(
        json_extract_scalar(r.object_json, '$.decimals'),
        json_extract_scalar(r.object_json, '$.currency.decimals'),
        json_extract_scalar(r.object_json, '$.metadata.decimals')
      ) as integer
    ) as coin_decimals,
    r.checkpoint as checkpoint_latest,
    r.version as version_latest,
    case
      when r.type_tag like '0x2::coin_registry::Currency<%' then 3
      else 2
    end as source_priority
  from object_rows r
  where r.type_tag like '0x2::coin_registry::Currency<%'
    or r.type_tag like '0x2::coin_registry::CoinData<%'
),

new_rows as (
  select
    coin_type,
    coin_symbol,
    coin_decimals,
    checkpoint_latest,
    version_latest,
    source_priority
  from legacy_coin_metadata
  where coin_type is not null
    and coin_symbol is not null
    and coin_decimals is not null
  union all
  select
    coin_type,
    coin_symbol,
    coin_decimals,
    checkpoint_latest,
    version_latest,
    source_priority
  from coin_registry_metadata
  where coin_type is not null
    and coin_symbol is not null
    and coin_decimals is not null
),

unioned as (
  select
    n.coin_type,
    n.coin_symbol,
    n.coin_decimals,
    n.checkpoint_latest,
    n.version_latest,
    n.source_priority
  from new_rows n
  {% if is_incremental() %}
  union all
  select
    t.coin_type,
    t.coin_symbol,
    t.coin_decimals,
    t.checkpoint_latest,
    t.version_latest,
    0 as source_priority
  from {{ this }} t
  {% endif %}
),

ranked as (
  select
    u.coin_type,
    u.coin_symbol,
    u.coin_decimals,
    u.checkpoint_latest,
    u.version_latest,
    row_number() over (
      partition by u.coin_type
      order by u.checkpoint_latest desc, u.version_latest desc, u.source_priority desc
    ) as rn
  from unioned u
),

latest as (
  select
    r.coin_type,
    r.coin_symbol,
    r.coin_decimals,
    r.checkpoint_latest,
    r.version_latest
  from ranked r
  where r.rn = 1
),

manual as (
  select
    m.coin_type,
    m.coin_symbol,
    m.coin_decimals
  from (
    values
      (lower('0x2::sui::SUI'), 'SUI', 9),
      (lower('0x27792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN'), 'BTC', 8)
  ) as m(coin_type, coin_symbol, coin_decimals)
)

select
  l.coin_type,
  l.coin_symbol,
  l.coin_decimals,
  l.checkpoint_latest,
  l.version_latest
from latest l
union all
select
  m.coin_type,
  m.coin_symbol,
  m.coin_decimals,
  cast(null as bigint) as checkpoint_latest,
  cast(null as bigint) as version_latest
from manual m
left join latest l
  on l.coin_type = m.coin_type
where l.coin_type is null
