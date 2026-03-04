{{ config(
    schema = 'thorchain',
    alias = 'core_block',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'dim_block_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'core', 'blocks'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['cast(height as bigint)']) }} AS dim_block_id,
        cast(height as bigint) AS block_id,
        cast(from_unixtime(cast(timestamp / 1e9 as bigint)) as timestamp) AS block_time,
        cast(from_unixtime(cast(timestamp / 1e9 as bigint)) as timestamp) AS block_timestamp,
        date(from_unixtime(cast(timestamp / 1e9 as bigint))) AS block_date,
        date_trunc('hour', from_unixtime(cast(timestamp / 1e9 as bigint))) AS block_hour,
        date_trunc('week', from_unixtime(cast(timestamp / 1e9 as bigint))) AS block_week,
        date_trunc('month', from_unixtime(cast(timestamp / 1e9 as bigint))) AS block_month,
        quarter(from_unixtime(cast(timestamp / 1e9 as bigint))) AS block_quarter,
        year(from_unixtime(cast(timestamp / 1e9 as bigint))) AS block_year,
        day(from_unixtime(cast(timestamp / 1e9 as bigint))) AS block_dayofmonth,
        day_of_week(from_unixtime(cast(timestamp / 1e9 as bigint))) AS block_dayofweek,
        day_of_year(from_unixtime(cast(timestamp / 1e9 as bigint))) AS block_dayofyear,
        timestamp,
        hash,
        agg_state,
        current_timestamp AS _inserted_timestamp,
        cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
        current_timestamp AS inserted_timestamp,
        current_timestamp AS modified_timestamp
    FROM {{ source('thorchain', 'block_log') }}
    WHERE height IS NOT NULL
    {% if is_incremental() %}
      AND {{ incremental_predicate('cast(from_unixtime(cast(timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif %}
)

SELECT * FROM base
