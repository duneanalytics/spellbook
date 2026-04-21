{{
  config(
    schema = 'tokens_xrpl',
    alias = 'amm_balance_deltas',
    materialized = 'view',
  )
}}

with amm_transactions as (
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
  where t.transaction_type in ('AMMDeposit', 'AMMWithdraw')
    and t.transaction_result = 'tesSUCCESS'
),

parsed_nodes as (
  select
    n.tx_hash,
    n.node_index,
    n.ledger_entry_type,
    json_extract_scalar(n.final_fields, '$.Account') as final_account,
    json_extract_scalar(n.final_fields, '$.LPTokenBalance.currency') as lp_token_currency,
    json_extract_scalar(n.final_fields, '$.Balance.currency') as balance_currency,
    try_cast(json_extract_scalar(n.final_fields, '$.Balance.value') as double) as final_balance_value,
    try_cast(json_extract_scalar(n.previous_fields, '$.Balance.value') as double) as previous_balance_value,
    json_extract_scalar(n.final_fields, '$.LowLimit.issuer') as low_limit_issuer,
    json_extract_scalar(n.final_fields, '$.HighLimit.issuer') as high_limit_issuer
  from {{ ref('tokens_xrpl_affected_nodes') }} n
  where n.transaction_type in ('AMMDeposit', 'AMMWithdraw')
    and n.transaction_result = 'tesSUCCESS'
    and n.ledger_entry_type in ('AMM', 'AccountRoot', 'RippleState')
),

amm_nodes as (
  select
    n.tx_hash,
    n.final_account as pool_account,
    n.lp_token_currency
  from parsed_nodes n
  where n.ledger_entry_type = 'AMM'
),

pool_xrp_deltas as (
  select
    n.tx_hash,
    n.node_index,
    a.pool_account,
    a.lp_token_currency,
    'XRP' as currency,
    cast(null as varchar) as token_issuer,
    n.final_balance_value - n.previous_balance_value as pool_balance_delta
  from parsed_nodes n
  inner join amm_nodes a
    on n.tx_hash = a.tx_hash
  where n.ledger_entry_type = 'AccountRoot'
    and n.final_account = a.pool_account
    and n.previous_balance_value is not null
),

pool_trustline_deltas as (
  select
    n.tx_hash,
    n.node_index,
    a.pool_account,
    a.lp_token_currency,
    n.balance_currency as currency,
    case
      when n.low_limit_issuer = a.pool_account then n.high_limit_issuer
      else n.low_limit_issuer
    end as token_issuer,
    case
      when n.low_limit_issuer = a.pool_account
        then n.final_balance_value - n.previous_balance_value
      when n.high_limit_issuer = a.pool_account
        then -1 * (n.final_balance_value - n.previous_balance_value)
      else cast(null as double)
    end as pool_balance_delta
  from parsed_nodes n
  inner join amm_nodes a
    on n.tx_hash = a.tx_hash
  where n.ledger_entry_type = 'RippleState'
    and (
      n.low_limit_issuer = a.pool_account
      or n.high_limit_issuer = a.pool_account
    )
    and n.previous_balance_value is not null
),

all_pool_deltas as (
  select
    tx_hash,
    node_index,
    pool_account,
    lp_token_currency,
    currency,
    token_issuer,
    pool_balance_delta
  from pool_xrp_deltas

  union all

  select
    tx_hash,
    node_index,
    pool_account,
    lp_token_currency,
    currency,
    token_issuer,
    pool_balance_delta
  from pool_trustline_deltas
),

classified_pool_deltas as (
  select
    tx_hash,
    node_index,
    pool_account,
    lp_token_currency,
    currency,
    token_issuer,
    pool_balance_delta,
    case
      when currency = lp_token_currency then 'lp'
      else 'asset'
    end as delta_asset_type
  from all_pool_deltas
),

normalized_deltas as (
  select
    {{ dbt_utils.generate_surrogate_key(['t.tx_hash', 'd.node_index']) }} as unique_key,
    t.blockchain,
    t.block_month,
    t.block_date,
    t.block_time,
    t.block_number,
    t.tx_hash,
    case
      when d.currency = 'XRP' then 'native'
      else 'issued'
    end as token_standard,
    t.tx_from,
    t.tx_to,
    t.tx_index,
    case
      when d.delta_asset_type = 'lp' and d.pool_balance_delta < 0 then d.pool_account
      when d.delta_asset_type = 'lp' and d.pool_balance_delta > 0 then t.tx_from
      when d.pool_balance_delta > 0 then t.tx_from
      else d.pool_account
    end as "from",
    case
      when d.delta_asset_type = 'lp' and d.pool_balance_delta < 0 then t.tx_from
      when d.delta_asset_type = 'lp' and d.pool_balance_delta > 0 then d.pool_account
      when d.pool_balance_delta > 0 then d.pool_account
      else t.tx_from
    end as to,
    case
      when d.currency = 'XRP' then 'rrrrrrrrrrrrrrrrrrrrrhoLvTp'
      when d.delta_asset_type = 'lp' then d.pool_account
      else d.token_issuer
    end as issuer,
    d.currency,
    case
      when length(d.currency) = 40 then upper(d.currency)
      else cast(null as varchar)
    end as currency_hex,
    abs(d.pool_balance_delta) as amount_raw,
    case
      when d.currency = 'XRP' then abs(d.pool_balance_delta) / 1000000.0
      else abs(d.pool_balance_delta)
    end as amount,
    case
      when d.delta_asset_type = 'lp' and d.pool_balance_delta < 0 then 'amm_lp_mint'
      when d.delta_asset_type = 'lp' and d.pool_balance_delta > 0 then 'amm_lp_burn'
      when d.pool_balance_delta > 0 then 'amm_deposit'
      else 'amm_withdraw'
    end as transfer_type,
    t.transaction_type,
    t.transaction_result,
    false as partial_payment_flag,
    current_timestamp as _updated_at
  from classified_pool_deltas d
  inner join amm_transactions t
    on d.tx_hash = t.tx_hash
  where d.pool_balance_delta is not null
    and d.pool_balance_delta != 0
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
from normalized_deltas
