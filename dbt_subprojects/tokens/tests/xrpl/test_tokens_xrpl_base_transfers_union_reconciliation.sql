-- Reconciliation invariant for XRPL base-transfer unioning.
--
-- Purpose:
-- Verify `tokens_xrpl_base_transfers` matches the exact row set emitted by the
-- upstream per-family XRPL base models over the same recent scope.
--
-- Failure interpretation:
-- Any returned row means the unified base-transfer boundary is missing rows,
-- includes extra rows, or has field drift relative to its upstream inputs.

with expected_rows as (
  select
    block_date,
    unique_key,
    blockchain,
    block_time,
    block_month,
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
    amount_raw,
    amount,
    transfer_type,
    transaction_type,
    transaction_result,
    partial_payment_flag
  from {{ ref('tokens_xrpl_base_payments') }}
  where {{ incremental_predicate('block_time') }}
  union all
  select
    block_date,
    unique_key,
    blockchain,
    block_time,
    block_month,
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
    amount_raw,
    amount,
    transfer_type,
    transaction_type,
    transaction_result,
    partial_payment_flag
  from {{ ref('tokens_xrpl_base_check_cash') }}
  where {{ incremental_predicate('block_time') }}
  union all
  select
    block_date,
    unique_key,
    blockchain,
    block_time,
    block_month,
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
    amount_raw,
    amount,
    transfer_type,
    transaction_type,
    transaction_result,
    partial_payment_flag
  from {{ ref('tokens_xrpl_base_escrow_finish') }}
  where {{ incremental_predicate('block_time') }}
  union all
  select
    block_date,
    unique_key,
    blockchain,
    block_time,
    block_month,
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
    amount_raw,
    amount,
    transfer_type,
    transaction_type,
    transaction_result,
    partial_payment_flag
  from {{ ref('tokens_xrpl_base_payment_channel_claims') }}
  where {{ incremental_predicate('block_time') }}
  union all
  select
    block_date,
    unique_key,
    blockchain,
    block_time,
    block_month,
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
    amount_raw,
    amount,
    transfer_type,
    transaction_type,
    transaction_result,
    partial_payment_flag
  from {{ ref('tokens_xrpl_base_amm_deposits') }}
  where {{ incremental_predicate('block_time') }}
  union all
  select
    block_date,
    unique_key,
    blockchain,
    block_time,
    block_month,
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
    amount_raw,
    amount,
    transfer_type,
    transaction_type,
    transaction_result,
    partial_payment_flag
  from {{ ref('tokens_xrpl_base_amm_withdraws') }}
  where {{ incremental_predicate('block_time') }}
),

actual_rows as (
  select
    block_date,
    unique_key,
    blockchain,
    block_time,
    block_month,
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
    amount_raw,
    amount,
    transfer_type,
    transaction_type,
    transaction_result,
    partial_payment_flag
  from {{ ref('tokens_xrpl_base_transfers') }}
  where {{ incremental_predicate('block_time') }}
)

select
  coalesce(e.block_date, a.block_date) as block_date,
  coalesce(e.unique_key, a.unique_key) as unique_key,
  e.tx_hash as expected_tx_hash,
  a.tx_hash as actual_tx_hash,
  e.transfer_type as expected_transfer_type,
  a.transfer_type as actual_transfer_type,
  e.currency as expected_currency,
  a.currency as actual_currency,
  e.amount_raw as expected_amount_raw,
  a.amount_raw as actual_amount_raw
from expected_rows e
full outer join actual_rows a
  on e.block_date = a.block_date
  and e.unique_key = a.unique_key
where e.unique_key is null
  or a.unique_key is null
  or e.blockchain is distinct from a.blockchain
  or e.block_time is distinct from a.block_time
  or e.block_month is distinct from a.block_month
  or e.block_number is distinct from a.block_number
  or e.tx_hash is distinct from a.tx_hash
  or e.token_standard is distinct from a.token_standard
  or e.tx_from is distinct from a.tx_from
  or e.tx_to is distinct from a.tx_to
  or e.tx_index is distinct from a.tx_index
  or e."from" is distinct from a."from"
  or e.to is distinct from a.to
  or e.xrpl_asset_id is distinct from a.xrpl_asset_id
  or e.issuer is distinct from a.issuer
  or e.currency is distinct from a.currency
  or e.currency_hex is distinct from a.currency_hex
  or e.amount_raw is distinct from a.amount_raw
  or e.amount is distinct from a.amount
  or e.transfer_type is distinct from a.transfer_type
  or e.transaction_type is distinct from a.transaction_type
  or e.transaction_result is distinct from a.transaction_result
  or e.partial_payment_flag is distinct from a.partial_payment_flag
