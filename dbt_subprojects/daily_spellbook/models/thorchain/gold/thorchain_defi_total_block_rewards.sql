{{ config(
    schema = 'thorchain',
    alias = 'defi_total_block_rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'fact_total_block_rewards_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'total_block_rewards', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                              "defi",
                              "defi_total_block_rewards",
                              \'["krishhh"]\') }}'
) }}

WITH base AS (
    SELECT
        block_id,
        reward_entity,
        rune_amount,
        rune_amount_usd,
        _unique_key,
        _inserted_timestamp,
        block_time
    FROM {{ ref('thorchain_silver_total_block_rewards') }}
    {% if not is_incremental() %}
    WHERE block_time >= current_date - interval '18' day
    {% endif %}
)

SELECT
    -- CRITICAL: Generate surrogate key (Trino equivalent of dbt_utils.generate_surrogate_key)
    to_hex(sha256(to_utf8(a._unique_key))) AS fact_total_block_rewards_id,
    
    -- CRITICAL: Always include partitioning columns first
    a.block_time,
    date(a.block_time) as block_date,
    date_trunc('month', a.block_time) as block_month,
    
    -- Block dimension reference (simplified - no redundant JOIN)
    '-1' AS dim_block_id,
    
    -- Rewards data (SAME AS FLIPSIDE OUTPUT)
    a.reward_entity,
    a.rune_amount,
    a.rune_amount_usd,
    
    -- Audit fields (Trino conversions)
    a._inserted_timestamp,
    cast(from_hex(replace(cast(uuid() as varchar), '-', '')) as varchar) AS _audit_run_id,  -- Trino equivalent of invocation_id
    current_timestamp AS inserted_timestamp,  -- Trino equivalent of SYSDATE()
    current_timestamp AS modified_timestamp

FROM base a

{% if is_incremental() %}
WHERE {{ incremental_predicate('a.block_time') }}
{% endif %}
