{{ config(
    schema = 'thorchain',
    alias = 'defi_pool_block_fees',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'fact_pool_block_fees_id'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'defi', 'pool_fees', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
    SELECT
        DAY,
        pool_name,
        rewards,
        total_liquidity_fees_rune,
        asset_liquidity_fees,
        rune_liquidity_fees,
        earnings,
        _unique_key,
        _INSERTED_TIMESTAMP
    FROM
    {{ ref('thorchain_silver_pool_block_fees') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('day') }}
    {% endif -%}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a._unique_key']
  ) }} AS fact_pool_block_fees_id,
  DAY,
  pool_name,
  rewards,
  total_liquidity_fees_rune,
  asset_liquidity_fees,
  rune_liquidity_fees,
  earnings,
  A._inserted_timestamp,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM
  base as a
