{{ config(
    schema = 'thorchain',
    alias = 'defi_pool_block_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'fact_pool_block_balances_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'pool_balances', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                              "defi",
                              "defi_pool_block_balances",
                              \'["krishhh"]\') }}'
) }}

WITH base AS (
  SELECT
    block_id,
    pool_name,
    rune_amount,
    rune_amount_usd,
    asset_amount,
    asset_amount_usd,
    synth_amount,
    synth_amount_usd,
    _unique_key,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('thorchain_silver_pool_block_balances') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a._unique_key']
  ) }} AS fact_pool_block_balances_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  pool_name,
  rune_amount,
  rune_amount_usd,
  asset_amount,
  asset_amount_usd,
  synth_amount,
  synth_amount_usd,
  A._INSERTED_TIMESTAMP,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM
  base as a
JOIN {{ ref('thorchain_core_block') }} as b
  ON a.block_id = b.block_id
{% if is_incremental() or true %}
WHERE {{ incremental_predicate('b.block_timestamp') }}
{% endif %}