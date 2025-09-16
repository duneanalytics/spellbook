{{ config(
  schema='dex_sui',
  alias='coin_metadata_staging',
  materialized='incremental',
  file_format='delta',
  incremental_strategy='merge',
  unique_key=['object_id_hex','version'],
  tags=['sui','dex','staging']
) }}

with src as (
  select
      -- stable keys for merge
      ('0x' || lower(to_hex(object_id)))                as object_id_hex,
      cast(type_ as varchar)                            as type_str,
      lower(regexp_extract(type_, '<(.*)>', 1))         as coin_type,

      -- metadata payload (native strings)
      json_extract_scalar(object_json, '$.symbol')      as coin_symbol,
      cast(json_extract_scalar(object_json, '$.decimals') as integer) as coin_decimals,

      -- ordering for “latest”
      checkpoint,
      version
  from {{ source('sui','objects') }}
  where type_ like '0x2::coin::CoinMetadata<%'

  -- prune to recent partitions on incremental runs; keep a buffer for late arrivals
  {% if is_incremental() %}
    and checkpoint >= (
      select coalesce(max(checkpoint), 0) from {{ this }}
    ) - 5000
  {% endif %}
)

select * from src