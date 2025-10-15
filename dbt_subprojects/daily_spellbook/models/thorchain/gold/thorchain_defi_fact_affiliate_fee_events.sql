{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_affiliate_fee_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_affiliate_fee_events_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'affiliate_fee_events', 'fact']
) }}

-- Deduplication and gold layer combined (no silver layer needed)
-- Note: Complex multi-column partition for deduplication
WITH deduplicated AS (
    SELECT
        tx_id,
        fee_amt,
        gross_amt,
        fee_bps,
        memo,
        asset,
        rune_address,
        thorname,
        event_id,
        block_timestamp,
        _updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY event_id, tx_id, fee_amt, gross_amt, fee_bps, memo, asset, rune_address, thorname, block_timestamp
            ORDER BY _updated_at DESC
        ) AS rn
    FROM {{ source('thorchain', 'affiliate_fee_events') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '15' day
),

base AS (
    SELECT
        tx_id,
        fee_amt,
        gross_amt,
        fee_bps,
        memo,
        asset,
        rune_address,
        thorname,
        event_id,
        block_timestamp,
        cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) AS block_time,
        date_trunc('month', cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)) AS block_month,
        current_timestamp AS _inserted_timestamp
    FROM deduplicated
    WHERE rn = 1
      {% if is_incremental() %}
        AND {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
      {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'a.event_id',
        'a.block_timestamp'
    ]) }} AS fact_affiliate_fee_events_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.tx_id,
    a.fee_amt,
    a.gross_amt,
    a.fee_bps,
    a.memo,
    a.asset,
    a.rune_address,
    a.thorname,
    a.event_id,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_dim_block') }} b
    ON a.block_timestamp = b.timestamp

