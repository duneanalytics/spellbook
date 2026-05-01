{{
  config(
    schema = 'tokens_xrpl',
    alias = 'base_check_cash',
    materialized = 'view',
  )
}}

with check_cash_transactions as (
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
    t.transaction_result,
    t.amount_currency,
    t.amount_issuer,
    t.amount_value,
    t.deliver_min_currency,
    t.deliver_min_issuer,
    t.deliver_min_value,
    t.delivered_currency,
    t.delivered_issuer,
    t.delivered_value
  from {{ ref('tokens_xrpl_transaction_metadata') }} t
  where t.transaction_type = 'CheckCash'
    and t.transaction_result = 'tesSUCCESS'
),

check_nodes as (
  select
    n.tx_hash,
    n.block_date,
    json_extract_scalar(n.final_fields, '$.Account') as check_account,
    json_extract_scalar(n.final_fields, '$.Destination') as check_destination,
    coalesce(
      json_extract_scalar(n.final_fields, '$.SendMax.currency'),
      case
        when json_extract_scalar(n.final_fields, '$.SendMax') is not null then 'XRP'
        else cast(null as varchar)
      end
    ) as send_max_currency,
    case
      when json_extract_scalar(n.final_fields, '$.SendMax.currency') = 'XRP' then cast(null as varchar)
      else json_extract_scalar(n.final_fields, '$.SendMax.issuer')
    end as send_max_issuer
  from {{ ref('tokens_xrpl_affected_nodes') }} n
  where n.transaction_type = 'CheckCash'
    and n.transaction_result = 'tesSUCCESS'
    and n.node_action = 'DeletedNode'
    and n.ledger_entry_type = 'Check'
),

prepared_transfers as (
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
    t.transaction_result,
    n.check_account,
    n.check_destination,
    coalesce(t.delivered_currency, n.send_max_currency, t.amount_currency, t.deliver_min_currency) as settled_currency,
    coalesce(t.delivered_issuer, n.send_max_issuer, t.amount_issuer, t.deliver_min_issuer) as settled_issuer,
    try_cast(coalesce(t.delivered_value, t.amount_value, t.deliver_min_value) as double) as settled_amount_raw
  from check_cash_transactions t
  inner join check_nodes n
    on t.tx_hash = n.tx_hash
    and t.block_date = n.block_date
),

normalized_transfers as (
  select
    {{ dbt_utils.generate_surrogate_key(['t.tx_hash']) }} as unique_key,
    t.blockchain,
    t.block_month,
    t.block_date,
    t.block_time,
    t.block_number,
    t.tx_hash,
    case
      when t.settled_currency = 'XRP' then 'native'
      else 'issued'
    end as token_standard,
    t.tx_from,
    t.tx_to,
    t.tx_index,
    t.check_account as "from",
    t.check_destination as to,
    case
      when t.settled_currency = 'XRP' then 'rrrrrrrrrrrrrrrrrrrrrhoLvTp'
      else t.settled_issuer
    end as issuer,
    t.settled_currency as currency,
    case
      when length(t.settled_currency) = 40
        then upper(t.settled_currency)
      else cast(null as varchar)
    end as currency_hex,
    t.settled_amount_raw as amount_raw,
    case
      when t.settled_currency = 'XRP' then t.settled_amount_raw / 1000000.0
      else t.settled_amount_raw
    end as amount,
    'check_cash' as transfer_type,
    t.transaction_type,
    t.transaction_result,
    false as partial_payment_flag,
    current_timestamp as _updated_at
  from prepared_transfers t
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
  case
    when currency = 'XRP' then 'xrp'
    when issuer is null or currency is null then cast(null as varchar)
    when currency_hex is not null then concat(lower(issuer), ':', lower(currency_hex))
    else concat(lower(issuer), ':', lower(currency))
  end as xrpl_asset_id,
  issuer,
  currency,
  currency_hex,
  amount_raw,
  amount,
  transfer_type,
  transaction_type,
  transaction_result,
  partial_payment_flag,
  _updated_at
from normalized_transfers
where amount_raw > 0
