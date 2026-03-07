{{ config(
    schema = 'thorchain_silver',
    alias = 'pool_block_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', '_unique_key'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'pool_balances', 'silver']
) }}

SELECT
    b.block_timestamp,
    cast(date_trunc('day', b.block_timestamp) as date) as day,
    b.height AS block_id,
    bpd.pool_name,
    COALESCE(rune_e8 / pow(10, 8), 0) AS rune_amount,
    COALESCE(rune_e8 / pow(10, 8) * rune_usd, 0) AS rune_amount_usd,
    COALESCE(asset_e8 / pow(10, 8), 0) AS asset_amount,
    COALESCE(asset_e8 / pow(10, 8) * asset_usd, 0) AS asset_amount_usd,
    COALESCE(synth_e8 / pow(10, 8), 0) AS synth_amount,
    COALESCE(synth_e8 / pow(10, 8) * asset_usd, 0) AS synth_amount_usd,
    concat_ws(
        '-',
        cast(bpd.block_timestamp as varchar),
        bpd.pool_name
    ) AS _unique_key,
    bpd._inserted_timestamp
FROM
    {{ ref('thorchain_silver_block_pool_depths') }} as bpd
JOIN
    {{ ref('thorchain_silver_block_log') }} as b
    ON bpd.block_timestamp = b.timestamp
LEFT JOIN
    {{ ref('thorchain_silver_prices') }} as p
    ON b.height = p.block_id
    AND bpd.pool_name = p.pool_name
{% if is_incremental() %}
WHERE {{ incremental_predicate('b.block_timestamp') }}
{% endif %}