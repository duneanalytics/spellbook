-- Shape invariant for Aptos residual base-transfer rows.
--
-- Purpose:
-- Validate that one-sided mint/burn residuals keep the expected endpoint shape.
-- Canonical USDC is the only intentional exception, where one-sided residuals
-- are self-attributed.
--
-- Failure interpretation:
-- Any returned row means residual shaping drifted in a way that can corrupt
-- downstream sender/receiver semantics.

{% set canonical_usdc_asset_type = '0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b' %}

select
  b.block_date,
  b.tx_version,
  b.event_index,
  b.counterpart_event_index,
  b.asset_type,
  b.transfer_type,
  b.from_address,
  b.to_address,
  b.from_storage_id,
  b.to_storage_id,
  b.amount_raw
from {{ ref('tokens_aptos_base_transfers') }} b
where {{ incremental_predicate('b.block_time') }}
  and b.transfer_type in ('mint', 'burn')
  and (
    b.amount_raw is null
    or b.amount_raw = cast(0 as uint256)
    or (
      b.transfer_type = 'mint'
      and (
        b.counterpart_event_index is not null
        or b.to_address is null
        or b.to_storage_id is null
        or (
          b.asset_type = '{{ canonical_usdc_asset_type }}'
          and (
            b.from_address is distinct from b.to_address
            or b.from_storage_id is distinct from b.to_storage_id
          )
        )
        or (
          b.asset_type != '{{ canonical_usdc_asset_type }}'
          and (
            b.from_address is not null
            or b.from_storage_id is not null
          )
        )
      )
    )
    or (
      b.transfer_type = 'burn'
      and (
        b.counterpart_event_index is not null
        or b.from_address is null
        or b.from_storage_id is null
        or (
          b.asset_type = '{{ canonical_usdc_asset_type }}'
          and (
            b.to_address is distinct from b.from_address
            or b.to_storage_id is distinct from b.from_storage_id
          )
        )
        or (
          b.asset_type != '{{ canonical_usdc_asset_type }}'
          and (
            b.to_address is not null
            or b.to_storage_id is not null
          )
        )
      )
    )
  )
