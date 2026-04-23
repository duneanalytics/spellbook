{{
  config(
    schema = 'tokens_xrpl',
    alias = 'transaction_metadata',
    materialized = 'incremental',
    file_format = 'delta',
    partition_by = ['block_month'],
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    merge_skip_unchanged = true,
  )
}}

{% set xrpl_transfer_start_date = '2026-01-01' %} -- ci test, revert to '2012-06-01'

select
  hash as tx_hash,
  'xrpl' as blockchain,
  cast(date_trunc('month', ledger_close_date) as date) as block_month,
  ledger_close_date as block_date,
  cast(
    parse_datetime(
      regexp_replace(_ledger_close_time_human, ' UTC$', ''),
      'yyyy-MMM-dd HH:mm:ss.SSSSSSSSS'
    ) as timestamp
  ) as block_time,
  ledger_index as block_number,
  account as tx_from,
  destination as tx_to,
  try_cast(json_extract_scalar(metadata, '$.TransactionIndex') as bigint) as tx_index,
  transaction_type,
  result as transaction_result,
  sequence,
  fee,
  flags,
  owner,
  offer_sequence,
  check_id,
  channel,
  account_txn_id,
  amount.currency as amount_currency,
  amount.issuer as amount_issuer,
  amount.value as amount_value,
  amount.mpt_issuance_id as amount_mpt_issuance_id,
  amount2.currency as amount2_currency,
  amount2.issuer as amount2_issuer,
  amount2.value as amount2_value,
  deliver_max.currency as deliver_max_currency,
  deliver_max.issuer as deliver_max_issuer,
  deliver_max.value as deliver_max_value,
  deliver_min.currency as deliver_min_currency,
  deliver_min.issuer as deliver_min_issuer,
  deliver_min.value as deliver_min_value,
  asset.currency as asset_currency,
  asset.issuer as asset_issuer,
  asset2.currency as asset2_currency,
  asset2.issuer as asset2_issuer,
  balance.currency as balance_currency,
  balance.value as balance_value,
  lp_token_in.currency as lp_token_in_currency,
  lp_token_in.issuer as lp_token_in_issuer,
  lp_token_in.value as lp_token_in_value,
  lp_token_out.currency as lp_token_out_currency,
  lp_token_out.issuer as lp_token_out_issuer,
  lp_token_out.value as lp_token_out_value,
  coalesce(
    json_extract_scalar(metadata, '$.delivered_amount.currency'),
    case
      when json_extract_scalar(metadata, '$.delivered_amount') is not null then 'XRP'
      else cast(null as varchar)
    end
  ) as delivered_currency,
  case
    when coalesce(
      json_extract_scalar(metadata, '$.delivered_amount.currency'),
      case
        when json_extract_scalar(metadata, '$.delivered_amount') is not null then 'XRP'
        else cast(null as varchar)
      end
    ) = 'XRP' then cast(null as varchar)
    else json_extract_scalar(metadata, '$.delivered_amount.issuer')
  end as delivered_issuer,
  coalesce(
    json_extract_scalar(metadata, '$.delivered_amount.value'),
    json_extract_scalar(metadata, '$.delivered_amount')
  ) as delivered_value,
  metadata,
  current_timestamp as _updated_at
from {{ source('xrpl', 'transactions') }}
where ledger_close_date >= date '{{ xrpl_transfer_start_date }}'
  {% if is_incremental() %}
  and {{ incremental_predicate('ledger_close_date') }}
  {% endif %}
