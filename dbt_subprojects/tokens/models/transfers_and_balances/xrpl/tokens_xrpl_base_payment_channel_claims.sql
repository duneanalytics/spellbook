{{
  config(
    schema = 'tokens_xrpl',
    alias = 'base_payment_channel_claims',
    materialized = 'view'
  )
}}

with payment_channel_transactions as (
  select
    t.tx_hash,
    t.blockchain,
    t.block_month,
    t.block_date,
    t.block_time,
    t.block_number,
    t.tx_from,
    t.tx_to,
    t.tx_index,
    t.transaction_type,
    t.transaction_result
  from {{ ref('tokens_xrpl_transaction_metadata') }} t
  where t.transaction_type = 'PaymentChannelClaim'
    and t.transaction_result = 'tesSUCCESS'
),

paychannel_nodes as (
  select
    n.tx_hash,
    n.block_date,
    json_extract_scalar(n.final_fields, '$.Account') as channel_account,
    json_extract_scalar(n.final_fields, '$.Destination') as channel_destination,
    coalesce(
      json_extract_scalar(n.final_fields, '$.Balance.currency'),
      case
        when json_extract_scalar(n.final_fields, '$.Balance') is not null then 'XRP'
        else cast(null as varchar)
      end
    ) as balance_currency,
    coalesce(
      json_extract_scalar(n.final_fields, '$.Balance.value'),
      json_extract_scalar(n.final_fields, '$.Balance')
    ) as final_balance_value,
    coalesce(
      json_extract_scalar(n.previous_fields, '$.Balance.value'),
      json_extract_scalar(n.previous_fields, '$.Balance')
    ) as previous_balance_value
  from {{ ref('tokens_xrpl_affected_nodes') }} n
  where n.transaction_type = 'PaymentChannelClaim'
    and n.transaction_result = 'tesSUCCESS'
    and n.ledger_entry_type = 'PayChannel'
),

paychannel_claims as (
  select
    t.blockchain,
    t.block_month,
    t.block_date,
    t.block_time,
    t.block_number,
    t.tx_hash,
    t.tx_from,
    t.tx_to,
    t.tx_index,
    n.channel_account as "from",
    n.channel_destination as to,
    n.balance_currency as currency,
    try_cast(n.final_balance_value as double) - try_cast(n.previous_balance_value as double) as amount_raw,
    t.transaction_type,
    t.transaction_result
  from payment_channel_transactions t
  inner join paychannel_nodes n
    on t.tx_hash = n.tx_hash
    and t.block_date = n.block_date
)

select
  {{ dbt_utils.generate_surrogate_key(['p.tx_hash']) }} as unique_key,
  p.blockchain,
  p.block_month,
  p.block_date,
  p.block_time,
  p.block_number,
  p.tx_hash,
  'native' as token_standard,
  p.tx_from,
  p.tx_to,
  p.tx_index,
  p."from",
  p.to,
  'xrp' as xrpl_asset_id,
  'rrrrrrrrrrrrrrrrrrrrrhoLvTp' as issuer,
  coalesce(p.currency, 'XRP') as currency,
  cast(null as varchar) as currency_hex,
  p.amount_raw,
  p.amount_raw / 1000000.0 as amount,
  'payment_channel_claim' as transfer_type,
  p.transaction_type,
  p.transaction_result,
  false as partial_payment_flag,
  current_timestamp as _updated_at
from paychannel_claims p
where p.amount_raw > 0
