{{ config(
    schema = 'thorchain_silver',
    alias = 'daily_tvl',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day'],
    partition_by = ['day_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'daily_tvl', 'silver']
) }}

-- Daily TVL aggregation with RUNE pricing (simplified approach)
WITH daily_rune_price AS (
    SELECT
        date(p.block_time) AS day,
        AVG(p.price) AS rune_usd
    FROM {{ ref('thorchain_silver_prices') }} p
    WHERE p.symbol = 'RUNE'
      AND p.block_time >= current_date - interval '7' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('p.block_time') }}
    {% endif %}
    GROUP BY date(p.block_time)
),

base AS (
    SELECT
        br.day,
        date_trunc('month', br.day) as day_month,
        br.total_value_pooled,
        br.total_value_pooled * COALESCE(drp.rune_usd, 0) AS total_value_pooled_usd,
        br.total_value_bonded,
        br.total_value_bonded * COALESCE(drp.rune_usd, 0) AS total_value_bonded_usd,
        br.total_value_locked,
        br.total_value_locked * COALESCE(drp.rune_usd, 0) AS total_value_locked_usd
    FROM {{ ref('thorchain_silver_total_value_locked') }} br  -- ✅ Now converted!
    LEFT JOIN daily_rune_price drp
        ON br.day = drp.day
    WHERE br.day >= current_date - interval '7' day
)

SELECT * FROM base
{% if is_incremental() %}
WHERE {{ incremental_predicate('base.day') }}
{% endif %}
