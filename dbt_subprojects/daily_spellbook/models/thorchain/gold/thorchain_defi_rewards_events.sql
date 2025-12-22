{{ config(
    schema = 'thorchain',
    alias = 'defi_rewards_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'fact_rewards_events_id'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'defi', 'rewards', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
  SELECT
    bond_e8,
    event_id,
    block_timestamp
  FROM
    {{ ref('thorchain_silver_rewards_events') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.event_id','a.block_timestamp']
  ) }} AS fact_rewards_events_id,
  cast(date_trunc('day', b.block_timestamp) AS date) AS day,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  bond_e8,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM
  base A
JOIN {{ ref('thorchain_core_block') }} as b
    ON A.block_timestamp = b.timestamp
{% if is_incremental() %}
WHERE {{ incremental_predicate('b.block_timestamp') }}
{% endif %}
