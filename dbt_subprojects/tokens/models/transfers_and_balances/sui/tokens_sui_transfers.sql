{{
  config(
    schema = 'tokens_sui',
    alias = 'transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook = '{{ hide_spells() }}'
  )
}}

{% set transfers_start_date = '2023-04-12' %}

with base_transfers as (
  select
    unique_key,
    blockchain,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_hash,
    tx_digest,
    evt_index,
    trace_address,
    token_standard,
    tx_from,
    tx_to,
    tx_index,
    transfer_type,
    from_owner_type,
    to_owner_type,
    "from",
    to,
    from_owner_object_id,
    to_owner_object_id,
    contract_address,
    amount_raw,
    coin_type
  from {{ ref('tokens_sui_base_transfers') }}
  where 1 = 1
  {% if is_incremental() -%}
    and {{ incremental_predicate('block_date') }}
  {% else -%}
    and block_date >= timestamp '{{ transfers_start_date }}'
  {% endif -%}
),

coins_in_scope as (
  select distinct
    coin_type
  from base_transfers
),

coin_metadata_ranked as (
  select
    regexp_replace(
      lower(regexp_extract(cast(o.type_ as varchar), '<(.*)>', 1)),
      '^0x0*([0-9a-f]+)(::.*)$',
      '0x$1$2'
    ) as coin_type,
    cast(json_extract_scalar(o.object_json, '$.symbol') as varchar) as symbol,
    cast(json_extract_scalar(o.object_json, '$.decimals') as integer) as decimals,
    row_number() over (
      partition by regexp_replace(
        lower(regexp_extract(cast(o.type_ as varchar), '<(.*)>', 1)),
        '^0x0*([0-9a-f]+)(::.*)$',
        '0x$1$2'
      )
      order by o.checkpoint desc, o.version desc
    ) as rn
  from {{ source('sui', 'objects') }} o
  inner join coins_in_scope cis
    on regexp_replace(
    lower(regexp_extract(cast(o.type_ as varchar), '<(.*)>', 1)),
    '^0x0*([0-9a-f]+)(::.*)$',
    '0x$1$2'
  ) = cis.coin_type
  where cast(o.type_ as varchar) like '0x2::coin::CoinMetadata<%'
),

coin_metadata as (
  select
    coin_type,
    symbol,
    decimals
  from coin_metadata_ranked
  where rn = 1
),

sui_price_tokens as (
  select
    regexp_replace(lower(contract_address_full), '^0x0*([0-9a-f]+)(::.*)$', '0x$1$2') as coin_type,
    symbol,
    decimals
  from {{ ref('prices_sui_tokens') }}
),

prices as (
  select
    timestamp,
    contract_address,
    decimals,
    symbol,
    price
  from {{ source('prices_external', 'hour') }}
  where blockchain = 'sui'
  {% if is_incremental() -%}
    and {{ incremental_predicate('timestamp') }}
  {% else -%}
    and timestamp >= timestamp '{{ transfers_start_date }}'
  {% endif -%}
),

trusted_tokens as (
  select
    contract_address
  from {{ source('prices', 'trusted_tokens') }}
  where blockchain = 'sui'
),

enriched as (
  select
    b.unique_key,
    b.blockchain,
    b.block_month,
    b.block_date,
    b.block_time,
    b.block_number,
    b.tx_hash,
    b.tx_digest,
    b.evt_index,
    b.trace_address,
    b.token_standard,
    b.tx_from,
    b.tx_to,
    b.tx_index,
    b.transfer_type,
    b.from_owner_type,
    b.to_owner_type,
    b."from",
    b.to,
    b.from_owner_object_id,
    b.to_owner_object_id,
    b.contract_address,
    b.coin_type,
    coalesce(sp.symbol, cm.symbol, p.symbol) as symbol,
    b.amount_raw,
    b.amount_raw / power(10, coalesce(sp.decimals, cm.decimals, p.decimals)) as amount,
    p.price as price_usd,
    b.amount_raw / power(10, coalesce(sp.decimals, cm.decimals, p.decimals)) * p.price as amount_usd,
    case
      when tt.contract_address is not null then true
      else false
    end as is_trusted_token
  from base_transfers b
  left join sui_price_tokens sp
    on sp.coin_type = b.coin_type
  left join coin_metadata cm
    on cm.coin_type = b.coin_type
  left join trusted_tokens tt
    on tt.contract_address = b.contract_address
  left join prices p
    on date_trunc('hour', b.block_time) = p.timestamp
    and b.contract_address = p.contract_address
),

final as (
  select
    unique_key,
    blockchain,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_hash,
    tx_digest,
    evt_index,
    trace_address,
    token_standard,
    tx_from,
    tx_to,
    tx_index,
    transfer_type,
    from_owner_type,
    to_owner_type,
    "from",
    to,
    from_owner_object_id,
    to_owner_object_id,
    contract_address,
    coin_type,
    symbol,
    amount_raw,
    amount,
    price_usd,
    case
      when is_trusted_token = true then amount_usd
      when is_trusted_token = false and amount_usd < 1000000000 then amount_usd
      when is_trusted_token = false and amount_usd >= 1000000000 then cast(null as double)
    end as amount_usd
  from enriched
)

select
  unique_key,
  blockchain,
  block_month,
  block_date,
  block_time,
  block_number,
  tx_hash,
  tx_digest,
  evt_index,
  trace_address,
  token_standard,
  tx_from,
  tx_to,
  tx_index,
  transfer_type,
  from_owner_type,
  to_owner_type,
  "from",
  to,
  from_owner_object_id,
  to_owner_object_id,
  contract_address,
  coin_type,
  symbol,
  amount_raw,
  amount,
  price_usd,
  amount_usd
from final
