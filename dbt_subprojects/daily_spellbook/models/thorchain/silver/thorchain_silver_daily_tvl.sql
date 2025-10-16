{{ config(
    schema = 'thorchain_silver',
    alias = 'daily_tvl',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'block_date'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags = ['thorchain', 'daily_tvl', 'silver']
) }}

WITH daily_rune_price AS (
    SELECT
        date(p.block_time) AS block_date,
        AVG(p.rune_usd) AS rune_usd
    FROM {{ ref('thorchain_silver_prices') }} p
    WHERE p.block_time >= current_date - interval '16' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('p.block_time') }}
    {% endif %}
    GROUP BY date(p.block_time)
),

base AS (
    SELECT
        br.block_date,
        date_trunc('month', br.block_date) as block_month,
        br.total_value_pooled,
        br.total_value_pooled * COALESCE(drp.rune_usd, 0) AS total_value_pooled_usd,
        br.total_value_bonded,
        br.total_value_bonded * COALESCE(drp.rune_usd, 0) AS total_value_bonded_usd,
        br.total_value_locked,
        br.total_value_locked * COALESCE(drp.rune_usd, 0) AS total_value_locked_usd
    FROM {{ ref('thorchain_silver_total_value_locked') }} br
    LEFT JOIN daily_rune_price drp
        ON br.block_date = drp.block_date
    WHERE br.block_date >= current_date - interval '16' day
)

SELECT * FROM base