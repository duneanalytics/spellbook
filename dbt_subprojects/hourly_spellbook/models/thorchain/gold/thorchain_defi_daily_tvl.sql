{{ config(
    schema = 'thorchain',
    alias = 'defi_daily_tvl',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'fact_daily_tvl_id'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'defi', 'daily_tvl', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
    SELECT
        day,
        total_value_pooled,
        total_value_pooled_usd,
        total_value_bonded,
        total_value_bonded_usd,
        total_value_locked,
        total_value_locked_usd
    FROM
        {{ ref('thorchain_silver_daily_tvl') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('day') }}
    {% endif -%}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.day']
  ) }} AS fact_daily_tvl_id,
  day,
  total_value_pooled,
  total_value_pooled_usd,
  total_value_bonded,
  total_value_bonded_usd,
  total_value_locked,
  total_value_locked_usd,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM
  base as a
