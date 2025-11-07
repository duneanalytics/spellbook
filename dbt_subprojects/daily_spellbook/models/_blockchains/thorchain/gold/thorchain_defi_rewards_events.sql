{{ config(
  schema = 'thorchain',
  alias = 'defi_rewards_events',
  materialized = 'incremental',
  file_format = 'delta',
  unique_key = ['block_month', 'fact_rewards_events_id'],
  incremental_strategy = 'merge',
  partition_by = ['block_month'],
  incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
  tags = ['thorchain', 'defi', 'rewards', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                              "defi",
                              "defi_rewards_events",
                              \'["krishhh"]\') }}'
) }}

WITH base AS (
  SELECT
    bond_e8,
    event_id,
    block_time,
    block_date,
    block_month,
    _inserted_timestamp
  FROM {{ ref('thorchain_silver_rewards_events') }}
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['event_id', 'block_time']) }} AS fact_rewards_events_id,
  block_time,
  block_date,
  block_month,
  -1 AS block_height,
  bond_e8,
  event_id,
  _inserted_timestamp,
  '{{ invocation_id }}' AS _audit_run_id,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM base a
{% if is_incremental() %}
WHERE {{ incremental_predicate('a.block_time') }}
{% endif %}
