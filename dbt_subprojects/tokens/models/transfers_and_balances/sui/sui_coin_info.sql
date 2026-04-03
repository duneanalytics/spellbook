{{
  config(
    schema = 'sui',
    alias = 'coin_info',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['coin_type'],
    merge_skip_unchanged = true,
    tags = ['prod_exclude'],
  )
}}

{% set sui_transfer_start_date = '2023-04-12' %}

-- ranking policy:
-- choose the most recent metadata snapshot first (checkpoint/version),
-- then use source_priority only as a tie-breaker for equal recency.
-- source priority (highest to lowest):
-- 1) 0x2::coin_registry::Currency<T>
-- 2) 0x2::coin_registry::CoinData<T>
-- 3) 0x2::coin::CoinMetadata<T>

with

objects_base as (
  select
    cast(o.type_ as varchar) as type_tag,
    o.object_json,
    o.object_status,
    o.checkpoint,
    o.version
  from {{ source('sui', 'objects') }} o
  where o.type_ is not null
    and o.object_status in ('Created', 'Mutated')
    and o.date >= date '{{ sui_transfer_start_date }}'
    and (
      cast(o.type_ as varchar) like '0x2::coin::CoinMetadata<%'
      or cast(o.type_ as varchar) like '0x2::coin_registry::Currency<%'
      or cast(o.type_ as varchar) like '0x2::coin_registry::CoinData<%'
    )
    {% if is_incremental() %}
    and {{ incremental_predicate('o.date') }}
    {% endif %}
),

metadata_candidates as (
  select
    regexp_replace(
      lower(regexp_extract(o.type_tag, '<(.*)>', 1)),
      '^0x0*([0-9a-f]+)(::.*)$',
      '0x$1$2'
    ) as coin_type,
    regexp_replace(
      split_part(
        regexp_replace(
          lower(regexp_extract(o.type_tag, '<(.*)>', 1)),
          '^0x0*([0-9a-f]+)(::.*)$',
          '0x$1$2'
        ),
        '::',
        1
      ),
      '^0x0*([0-9a-f]+)$',
      '0x$1'
    ) as contract_address,
    cast(
      case
        when o.type_tag like '0x2::coin::CoinMetadata<%' then json_extract_scalar(o.object_json, '$.symbol')
        else coalesce(
          json_extract_scalar(o.object_json, '$.symbol'),
          json_extract_scalar(o.object_json, '$.currency.symbol'),
          json_extract_scalar(o.object_json, '$.metadata.symbol')
        )
      end as varchar
    ) as symbol,
    try_cast(
      case
        when o.type_tag like '0x2::coin::CoinMetadata<%' then json_extract_scalar(o.object_json, '$.decimals')
        else coalesce(
          json_extract_scalar(o.object_json, '$.decimals'),
          json_extract_scalar(o.object_json, '$.currency.decimals'),
          json_extract_scalar(o.object_json, '$.metadata.decimals')
        )
      end as integer
    ) as decimals,
    o.checkpoint as checkpoint_latest,
    o.version as version_latest,
    case
      when o.type_tag like '0x2::coin_registry::Currency<%' then 3
      when o.type_tag like '0x2::coin_registry::CoinData<%' then 2
      else 1
    end as source_priority
  from objects_base o
),

ranked as (
  select
    c.coin_type,
    c.contract_address,
    c.symbol,
    c.decimals,
    c.checkpoint_latest,
    c.version_latest,
    row_number() over (
      partition by c.coin_type
      order by c.checkpoint_latest desc, c.version_latest desc, c.source_priority desc
    ) as rn
  from metadata_candidates c
)

select
  r.coin_type,
  r.contract_address,
  r.symbol,
  r.decimals,
  r.checkpoint_latest,
  r.version_latest
from ranked r
where r.rn = 1
