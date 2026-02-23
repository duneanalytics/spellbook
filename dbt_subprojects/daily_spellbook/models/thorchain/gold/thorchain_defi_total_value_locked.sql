{{ config(
    schema = 'thorchain',
    alias = 'defi_total_value_locked',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'fact_total_value_locked_id'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'defi', 'total_value_locked', 'fact', 'tvl'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
    SELECT
        day,
        total_value_pooled,
        total_value_bonded,
        total_value_locked,
        _inserted_timestamp
    FROM
        {{ ref('thorchain_silver_total_value_locked') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('day') }}
    {% endif %}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.day']
  ) }} AS fact_total_value_locked_id,
  day,
  total_value_pooled,
  total_value_bonded,
  total_value_locked,
  A._inserted_timestamp,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM
  base as a
