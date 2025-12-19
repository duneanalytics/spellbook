{{ config(
    schema = 'thorchain',
    alias = 'defi_upgrades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_upgrades_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'defi', 'upgrades', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
  SELECT
    block_id,
    tx_id,
    from_address,
    to_address,
    burn_asset,
    rune_amount,
    rune_amount_usd,
    mint_amount,
    mint_amount_usd,
    _unique_key,
    _inserted_timestamp
  FROM
    {{ ref('thorchain_silver_upgrades') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a._unique_key']
  ) }} AS fact_upgrades_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  tx_id,
  from_address,
  to_address,
  burn_asset,
  rune_amount,
  rune_amount_usd,
  mint_amount,
  mint_amount_usd,
  A._inserted_timestamp,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM
  base as a
JOIN {{ ref('thorchain_core_block') }} as b
  ON a.block_id = b.block_id
{% if is_incremental() %}
WHERE {{ incremental_predicate('b.block_timestamp') }}
{% endif -%}
