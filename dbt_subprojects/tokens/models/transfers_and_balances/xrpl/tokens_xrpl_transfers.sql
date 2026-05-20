{{
  config(
    schema = 'tokens_xrpl',
    alias = 'transfers',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_month'],
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    merge_skip_unchanged = true,
    post_hook = '{{ private_data_explorer(blockchains = \'["xrpl"]\',
                    spell_type = "sector",
                    spell_name = "tokens_xrpl") }}'
  )
}}

{% set xrpl_transfer_start_date = '2013-01-01' %}

with base_transfers as (
  select
    b.*,
    case
      when b.currency = 'XRP' then 0x0000000000000000000000000000000000000000
      when coalesce(b.currency_hex, b.currency) is not null
        and b.issuer is not null
        then to_utf8(concat(coalesce(b.currency_hex, b.currency), '.', b.issuer))
      else cast(null as varbinary)
    end as price_contract_address
  from {{ ref('tokens_xrpl_base_transfers') }} b
  where b.block_date >= date '{{ xrpl_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('b.block_date') }}
    {% endif %}
),

prices as (
  select
    p.timestamp,
    p.contract_address,
    p.symbol,
    p.price
  from {{ source('prices_external', 'hour') }} p
  where p.blockchain = 'xrpl'
    and p.timestamp >= timestamp '{{ xrpl_transfer_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('p.timestamp') }}
    {% endif %}
),

currency_mapping as (
  select
    currency_hex,
    symbol
  from {{ ref('tokens_xrpl_currency_mapping') }}
),

final as (
  select
    b.unique_key,
    b.blockchain,
    b.block_month,
    b.block_date,
    b.block_time,
    b.block_number,
    b.tx_hash,
    b.token_standard,
    b.tx_from,
    b.tx_to,
    b.tx_index,
    b."from",
    b.to,
    b.xrpl_asset_id,
    b.issuer,
    b.currency,
    b.currency_hex,
    coalesce(
      case
        when b.currency = 'XRP' then 'XRP'
        else cast(null as varchar)
      end,
      p.symbol,
      m.symbol,
      case
        when b.currency_hex is not null
          then nullif(replace(from_utf8(from_hex(b.currency_hex)), chr(0), ''), '')
        else cast(null as varchar)
      end,
      b.currency
    ) as symbol,
    case
      when b.currency = 'XRP' then 6
      else cast(null as integer)
    end as decimals,
    b.amount_raw,
    b.amount,
    p.price as price_usd,
    b.amount * p.price as amount_usd,
    b.transfer_type,
    b.transaction_type,
    b.transaction_result,
    b.partial_payment_flag,
    b._updated_at
  from base_transfers b
  left join prices p
    on date_trunc('hour', b.block_time) = p.timestamp
    and b.price_contract_address = p.contract_address
  left join currency_mapping m
    on b.currency_hex = m.currency_hex
)

select
  unique_key,
  blockchain,
  block_month,
  block_date,
  block_time,
  block_number,
  tx_hash,
  token_standard,
  tx_from,
  tx_to,
  tx_index,
  "from",
  to,
  xrpl_asset_id,
  issuer,
  currency,
  currency_hex,
  symbol,
  decimals,
  amount_raw,
  amount,
  price_usd,
  amount_usd,
  transfer_type,
  transaction_type,
  transaction_result,
  partial_payment_flag,
  _updated_at
from final
