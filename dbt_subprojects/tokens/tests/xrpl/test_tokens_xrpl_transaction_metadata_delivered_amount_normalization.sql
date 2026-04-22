-- Mapping invariant for XRPL delivered-amount normalization.
--
-- Purpose:
-- Ensure `metadata.delivered_amount` is normalized consistently into the
-- typed `delivered_*` columns for object-valued issued-asset payloads,
-- scalar XRP payloads, and transactions with no delivered amount at all.
--
-- Failure interpretation:
-- Any returned row means XRPL delivered-amount parsing drifted and downstream
-- transfer models may consume incorrect asset or amount fields.

select
  m.block_date,
  m.tx_hash,
  m.transaction_type,
  m.transaction_result,
  m.delivered_currency,
  m.delivered_issuer,
  m.delivered_value
from {{ ref('tokens_xrpl_transaction_metadata') }} m
where {{ incremental_predicate('m.block_time') }}
  and (
    (
      json_extract(m.metadata, '$.delivered_amount') is null
      and (
        m.delivered_currency is not null
        or m.delivered_issuer is not null
        or m.delivered_value is not null
      )
    )
    or (
      json_extract(m.metadata, '$.delivered_amount') is not null
      and json_extract_scalar(m.metadata, '$.delivered_amount.currency') is null
      and (
        m.delivered_currency is distinct from 'XRP'
        or m.delivered_issuer is distinct from ''
        or m.delivered_value is distinct from json_extract_scalar(m.metadata, '$.delivered_amount')
      )
    )
    or (
      json_extract_scalar(m.metadata, '$.delivered_amount.currency') is not null
      and (
        m.delivered_currency is distinct from json_extract_scalar(m.metadata, '$.delivered_amount.currency')
        or m.delivered_issuer is distinct from case
          when json_extract_scalar(m.metadata, '$.delivered_amount.currency') = 'XRP' then ''
          else json_extract_scalar(m.metadata, '$.delivered_amount.issuer')
        end
        or m.delivered_value is distinct from coalesce(
          json_extract_scalar(m.metadata, '$.delivered_amount.value'),
          json_extract_scalar(m.metadata, '$.delivered_amount')
        )
      )
    )
  )
