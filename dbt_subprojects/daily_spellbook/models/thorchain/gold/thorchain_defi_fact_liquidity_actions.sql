{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_liquidity_actions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = 'fact_liquidity_actions_id',
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'liquidity', 'fact']
) }}

WITH base AS (
  SELECT
    tx_hash,
    action_type as lp_action,
    pool as pool_name,
    from_addr as from_address,
    to_addr as to_address,
    rune_amount,
    rune_amount_usd,
    asset_amount,
    asset_amount_usd,
    stake_units,
    null as asset_tx_id,  -- Not available in our model
    null as asset_address,  -- Not available in our model
    chain as asset_blockchain,
    imp_loss_protection_amount as il_protection,
    null as il_protection_usd,  -- Will calculate if needed
    asymmetry as unstake_asymmetry,
    basis_points as unstake_basis_points,
    event_id,
    block_time,
    total_value_usd,
    liquidity_type
  FROM
    {{ ref('thorchain_silver_liquidity_actions') }}
  WHERE block_time >= current_date - interval '7' day
)

SELECT
  concat(
    cast(a.tx_hash as varchar), '-',
    cast(a.event_id as varchar), '-', 
    cast(a.lp_action as varchar)
  ) AS fact_liquidity_actions_id,
  a.block_time,
  a.tx_hash as tx_id,
  a.lp_action,
  a.pool_name,
  a.from_address,
  a.to_address,
  a.rune_amount,
  a.rune_amount_usd,
  a.asset_amount,
  a.asset_amount_usd,
  a.stake_units,
  a.asset_tx_id,
  a.asset_address,
  a.asset_blockchain,
  a.il_protection,
  a.il_protection_usd,
  a.unstake_asymmetry,
  a.unstake_basis_points,
  a.total_value_usd,
  a.liquidity_type,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp

FROM base a

{% if is_incremental() %}
WHERE a.block_time >= (
  SELECT MAX(block_time - INTERVAL '1' HOUR)
  FROM {{ this }}
) 
{% endif %}