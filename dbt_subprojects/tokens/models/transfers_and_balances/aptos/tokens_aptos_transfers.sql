{{
  config(
    schema = 'tokens_aptos',
    alias = 'transfers',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_date'],
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    merge_skip_unchanged = true,
    post_hook = '{{ private_data_explorer(blockchains = \'["aptos"]\',
                    spell_type = "sector",
                    spell_name = "tokens_aptos") }}'
  )
}}

{% set aptos_transfer_start_date = '2022-10-12' %}
{% set usd_amount_threshold = 1000000000 %}
{% set legacy_native_aptos_asset_type = '0x1::aptos_coin::AptosCoin' %}
{% set current_native_aptos_asset_type = '0x000000000000000000000000000000000000000000000000000000000000000a' %}
{% set native_aptos_symbol = 'APT' %}
{% set native_aptos_decimals = 8 %}

with native_assets as (
  select *
  from (
    values
      (
        '{{ legacy_native_aptos_asset_type }}',
        '{{ native_aptos_symbol }}',
        {{ native_aptos_decimals }},
        0x0000000000000000000000000000000000000000
      ),
      (
        '{{ current_native_aptos_asset_type }}',
        '{{ native_aptos_symbol }}',
        {{ native_aptos_decimals }},
        0x0000000000000000000000000000000000000000
      )
  ) as t (asset_type, symbol, decimals, price_contract_address)
),

base_transfers as (
  select
    b.*,
    n.symbol as native_symbol,
    n.decimals as native_decimals,
    coalesce(n.price_contract_address, to_utf8(b.asset_type)) as price_contract_address
  from {{ ref('tokens_aptos_base_transfers') }} b
  left join native_assets n
    on b.asset_type = n.asset_type
  where b.block_date >= date '{{ aptos_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('b.block_time') }}
    {% endif %}
),

tx_metadata as (
  select
    t.version as tx_version,
    t.block_height,
    t.tx_index,
    from_hex(
      lpad(to_hex(t.sender), 64, '0')
    ) as tx_from
  from {{ source('aptos', 'user_transactions') }} t
  where t.block_time >= timestamp '{{ aptos_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('t.block_time') }}
    {% endif %}
),

asset_metadata as (
  select
    m.asset_type,
    m.asset_symbol,
    m.decimals
  from {{ ref('aptos_fungible_asset_metadata_current') }} m
),

prices as (
  select
    p.timestamp,
    p.contract_address,
    p.symbol,
    p.decimals,
    p.price
  from {{ source('prices_external', 'hour') }} p
  where p.blockchain = 'aptos'
    and p.timestamp >= timestamp '{{ aptos_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('p.timestamp') }}
    {% endif %}
),

trusted_tokens as (
  select
    t.contract_address
  from {{ source('prices', 'trusted_tokens') }} t
  where t.blockchain = 'aptos'
),

transfers as (
  select
    b.unique_key,
    'aptos' as blockchain,
    b.block_month,
    b.block_date,
    b.block_time,
    tx.block_height,
    b.tx_version,
    b.tx_hash,
    b.event_index,
    b.counterpart_event_index,
    b.token_standard,
    tx.tx_from,
    cast(null as varbinary) as tx_to,
    b.from_address as "from",
    b.to_address as "to",
    b.contract_address,
    b.asset_type,
    b.from_storage_id,
    b.to_storage_id,
    coalesce(b.native_symbol, m.asset_symbol, p.symbol) as symbol,
    coalesce(b.native_decimals, m.decimals, p.decimals) as decimals,
    tx.tx_index,
    b.amount_raw,
    case
      when coalesce(b.native_decimals, m.decimals, p.decimals) is null then cast(null as double)
      else cast(b.amount_raw as double) / power(
        10,
        cast(coalesce(b.native_decimals, m.decimals, p.decimals) as double)
      )
    end as amount,
    p.price as price_usd,
    case
      when coalesce(b.native_decimals, m.decimals, p.decimals) is null then cast(null as double)
      else cast(b.amount_raw as double) / power(
        10,
        cast(coalesce(b.native_decimals, m.decimals, p.decimals) as double)
      ) * p.price
    end as amount_usd,
    case
      when tt.contract_address is not null then true
      else false
    end as is_trusted_token,
    b.transfer_type,
    b._updated_at
  from base_transfers b
  left join asset_metadata m
    on b.asset_type = m.asset_type
  left join tx_metadata tx
    on b.tx_version = tx.tx_version
  left join trusted_tokens tt
    on b.price_contract_address = tt.contract_address
  left join prices p
    on date_trunc('hour', b.block_time) = p.timestamp
    and b.price_contract_address = p.contract_address
)

select
  unique_key,
  blockchain,
  block_month,
  block_date,
  block_time,
  block_height,
  tx_version,
  tx_hash,
  event_index,
  counterpart_event_index,
  token_standard,
  tx_from,
  tx_to,
  "from",
  "to",
  contract_address,
  asset_type,
  from_storage_id,
  to_storage_id,
  symbol,
  decimals,
  tx_index,
  amount_raw,
  amount,
  price_usd,
  case
    when is_trusted_token = true then amount_usd
    when is_trusted_token = false and amount_usd < {{ usd_amount_threshold }} then amount_usd
    when is_trusted_token = false and amount_usd >= {{ usd_amount_threshold }} then cast(null as double)
  end as amount_usd,
  transfer_type,
  _updated_at
from transfers
