{{
  config(
    schema = 'tokens_xrpl',
    alias = 'base_escrow_finish',
    materialized = 'view',
  )
}}

with escrow_finish_transactions as (
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
  where t.transaction_type = 'EscrowFinish'
    and t.transaction_result = 'tesSUCCESS'
),

escrow_nodes as (
  select
    n.tx_hash,
    n.block_date,
    json_extract_scalar(n.final_fields, '$.Account') as escrow_account,
    json_extract_scalar(n.final_fields, '$.Destination') as escrow_destination,
    coalesce(
      json_extract_scalar(n.final_fields, '$.Amount.currency'),
      case
        when json_extract_scalar(n.final_fields, '$.Amount') is not null then 'XRP'
        else cast(null as varchar)
      end
    ) as amount_currency,
    case
      when json_extract_scalar(n.final_fields, '$.Amount.currency') = 'XRP' then cast(null as varchar)
      else json_extract_scalar(n.final_fields, '$.Amount.issuer')
    end as amount_issuer,
    coalesce(
      json_extract_scalar(n.final_fields, '$.Amount.value'),
      json_extract_scalar(n.final_fields, '$.Amount')
    ) as amount_value
  from {{ ref('tokens_xrpl_affected_nodes') }} n
  where n.transaction_type = 'EscrowFinish'
    and n.transaction_result = 'tesSUCCESS'
    and n.node_action = 'DeletedNode'
    and n.ledger_entry_type = 'Escrow'
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
      when n.amount_currency = 'XRP' then 'native'
      else 'issued'
    end as token_standard,
    t.tx_from,
    t.tx_to,
    t.tx_index,
    n.escrow_account as "from",
    n.escrow_destination as to,
    case
      when n.amount_currency = 'XRP' then 'rrrrrrrrrrrrrrrrrrrrrhoLvTp'
      else n.amount_issuer
    end as issuer,
    n.amount_currency as currency,
    case
      when length(n.amount_currency) = 40 then upper(n.amount_currency)
      else cast(null as varchar)
    end as currency_hex,
    try_cast(n.amount_value as double) as amount_raw,
    case
      when n.amount_currency = 'XRP' then try_cast(n.amount_value as double) / 1000000.0
      else try_cast(n.amount_value as double)
    end as amount,
    'escrow_finish' as transfer_type,
    t.transaction_type,
    t.transaction_result,
    false as partial_payment_flag,
    current_timestamp as _updated_at
  from escrow_finish_transactions t
  inner join escrow_nodes n
    on t.tx_hash = n.tx_hash
    and t.block_date = n.block_date
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
