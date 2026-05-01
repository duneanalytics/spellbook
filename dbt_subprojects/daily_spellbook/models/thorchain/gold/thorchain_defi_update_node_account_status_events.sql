{{ config(
    schema = 'thorchain',
    alias = 'defi_update_node_account_status_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_update_node_account_status_events_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'defi', 'update_node_account_status_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
  SELECT
    block_timestamp,
    former_status,
    current_status,
    node_address,
    _inserted_timestamp
  FROM
    {{ ref('thorchain_silver_update_node_account_status_events') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.node_address', 'a.block_timestamp', 'a.current_status', 'a.former_status']
  ) }} AS fact_update_node_account_status_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  former_status,
  current_status,
  node_address,
  A._inserted_timestamp,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM
  base as a
JOIN {{ ref('thorchain_core_block') }} as b
  ON a.block_timestamp = b.timestamp
{% if is_incremental() %}
WHERE {{ incremental_predicate('b.block_timestamp') }}
{% endif -%}
