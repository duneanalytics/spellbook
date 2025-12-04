{{ config(
    schema = 'thorchain_silver',
    alias = 'swaps',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', '_unique_key'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'swaps', 'curated']
) }}

WITH swaps AS (
    SELECT
        tx_id,
        blockchain,
        from_address,
        to_address,
        from_asset,
        from_e8,
        to_asset,
        to_e8,
        memo,
        pool_name,
        to_e8_min,
        swap_slip_bp,
        liq_fee_e8,
        liq_fee_in_rune_e8,
        _DIRECTION,
        event_id,
        streaming_count,
        streaming_quantity,
        b.block_timestamp,
        b.height AS block_id,
        _TX_TYPE,
        a._INSERTED_TIMESTAMP,
        COUNT(1) over (
            PARTITION BY tx_id
        ) AS n_tx,
        RANK() over (
            PARTITION BY tx_id
            ORDER BY
            liq_fee_e8 ASC
        ) AS rank_liq_fee
    FROM
        {{ ref('thorchain_silver_swap_events') }} as a
    JOIN {{ ref('thorchain_silver_block_log') }} as b
        ON a.block_timestamp = b.timestamp
    {% if is_incremental() -%}
    WHERE {{ incremental_predicate('b.block_timestamp') }}
    {% endif -%}
), final as (
    SELECT
        cast(date_trunc('day', se.block_timestamp) AS date) AS day,
        se.block_timestamp,
        se.block_id,
        tx_id,
        blockchain,
        se.pool_name,
        from_address,
        CASE
            WHEN n_tx > 1
            AND rank_liq_fee = 1
            AND length(CAST(element_at(split(memo, ':'), 5) AS VARCHAR)) = 43 THEN CAST(element_at(split(
                memo,
                ':'
            ), 5) AS VARCHAR)
            WHEN n_tx > 1
            AND lower(substr(memo, 1, 1)) IN (
                's',
                '='
            )
            AND length(COALESCE(CAST(element_at(split(memo, ':'), 3) AS VARCHAR), '')) = 0 THEN from_address
            ELSE CAST(element_at(split(
                memo,
                ':'
            ), 3) AS VARCHAR)
            END AS native_to_address,
        to_address AS to_pool_address,
        CASE
            WHEN COALESCE(element_at(split(memo, ':'), 5), '') = '' THEN NULL
            WHEN strpos(element_at(split(memo, ':'), 5), '/') > 0 THEN element_at(split(element_at(split(memo, ':'), 5), '/'), 1)
            ELSE CAST(element_at(split(
                memo,
                ':'
            ), 5) AS VARCHAR)
            END AS affiliate_address,
        TRY_CAST(
            CASE
                WHEN COALESCE(element_at(split(memo, ':'), 6), '') = '' THEN NULL
                WHEN strpos(element_at(split(memo, ':'), 6), '/') > 0 THEN element_at(split(element_at(split(memo, ':'), 6), '/'), 1)
                ELSE element_at(split(
                memo,
                ':'
                ), 6)
            END AS INTEGER
        ) AS affiliate_fee_basis_points,
        split(COALESCE(element_at(split(element_at(split(memo, '|'), 1), ':'), 5), ''), '/') AS affiliate_addresses_array,
        from_asset,
        to_asset,
        COALESCE(from_e8 / pow(10, 8), 0) AS from_amount,
        COALESCE(to_e8 / pow(10, 8), 0) AS to_amount,
        COALESCE(to_e8_min / pow(10, 8), 0) AS min_to_amount,
        CASE
            WHEN from_asset = 'THOR.RUNE' THEN COALESCE(from_e8 * rune_usd / pow(10, 8), 0)
            ELSE COALESCE(from_e8 * asset_usd / pow(10, 8), 0)
            END AS from_amount_usd,
        CASE
            WHEN (
                to_asset = 'THOR.RUNE'
                OR to_asset = 'BNB.RUNE-B1A'
            ) THEN COALESCE(to_e8 * rune_usd / pow(10, 8), 0)
            ELSE COALESCE(to_e8 * asset_usd / pow(10, 8), 0)
            END AS to_amount_usd,
        rune_usd,
        asset_usd,
        CASE
            WHEN to_asset = 'THOR.RUNE' THEN COALESCE(to_e8_min * rune_usd / pow(10, 8), 0)
            ELSE COALESCE(to_e8_min * asset_usd / pow(10, 8), 0)
            END AS to_amount_min_usd,
        swap_slip_bp,
        COALESCE(liq_fee_in_rune_e8 / pow(10, 8), 0) AS liq_fee_rune,
        COALESCE(liq_fee_in_rune_e8 / pow(10, 8) * rune_usd, 0) AS liq_fee_rune_usd,
        CASE
            WHEN to_asset = 'THOR.RUNE' THEN COALESCE(liq_fee_e8 / pow(10, 8), 0)
            ELSE COALESCE(liq_fee_e8 / pow(10, 8), 0)
            END AS liq_fee_asset,
        CASE
            WHEN to_asset = 'THOR.RUNE' THEN COALESCE(liq_fee_e8 * rune_usd / pow(10, 8), 0)
            ELSE COALESCE(liq_fee_e8 * asset_usd / pow(10, 8), 0)
            END AS liq_fee_asset_usd,
        streaming_count,
        streaming_quantity,
        _TX_TYPE,
        _INSERTED_TIMESTAMP,
        event_id,
        value,
        index
    FROM
    swaps se
    LEFT JOIN {{ ref('thorchain_silver_prices') }} as p
        ON se.block_id = p.block_id
        AND se.pool_name = p.pool_name
    CROSS JOIN UNNEST(
        split(COALESCE(element_at(split(memo, ':'), 6), ''), '/')
    ) WITH ORDINALITY AS f(value, index)
), final_agg as (
    SELECT
        day
        , block_timestamp
        , block_id
        , tx_id
        , blockchain
        , pool_name
        , from_address
        , native_to_address
        , to_pool_address
        , affiliate_address
        , affiliate_fee_basis_points
        , affiliate_addresses_array
        , from_asset
        , to_asset
        , from_amount
        , to_amount
        , min_to_amount
        , from_amount_usd
        , to_amount_usd
        , rune_usd
        , asset_usd
        , to_amount_min_usd
        , swap_slip_bp
        , liq_fee_rune
        , liq_fee_rune_usd
        , liq_fee_asset
        , liq_fee_asset_usd
        , streaming_count
        , streaming_quantity
        , _TX_TYPE
        , _INSERTED_TIMESTAMP
        , event_id
        , ARRAY_AGG(TRY_CAST(TRIM(value) AS INTEGER) ORDER BY index) AS affiliate_fee_basis_points_array
    FROM
        final
    GROUP BY
        day
        , block_timestamp
        , block_id
        , tx_id
        , blockchain
        , pool_name
        , from_address
        , native_to_address
        , to_pool_address
        , affiliate_address
        , affiliate_fee_basis_points
        , affiliate_addresses_array
        , from_asset
        , to_asset
        , from_amount
        , to_amount
        , min_to_amount
        , from_amount_usd
        , to_amount_usd
        , rune_usd
        , asset_usd
        , to_amount_min_usd
        , swap_slip_bp
        , liq_fee_rune
        , liq_fee_rune_usd
        , liq_fee_asset
        , liq_fee_asset_usd
        , streaming_count
        , streaming_quantity
        , _TX_TYPE
        , _INSERTED_TIMESTAMP
        , event_id
)
select
    *
    , concat_ws(
        '-',
        tx_id,
        cast(block_id as varchar),
        to_asset,
        from_asset,
        COALESCE(
            native_to_address,
            ''
        ),
        from_address,
        pool_name,
        to_pool_address,
        cast(event_id as varchar)
    ) AS _unique_key
from final_agg