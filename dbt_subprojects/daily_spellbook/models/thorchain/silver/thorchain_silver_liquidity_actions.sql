{{ config(
    schema = 'thorchain_silver',
    alias = 'liquidity_actions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', '_unique_key'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'liquidity', 'curated', 'silver']
) }}

WITH stakes AS (
    SELECT
        block_timestamp,
        rune_tx_id,
        pool_name,
        rune_address,
        rune_e8,
        asset_e8,
        stake_units,
        asset_tx_id,
        asset_address,
        asset_blockchain,
        event_id,
        _inserted_timestamp
    FROM
        {{ ref('thorchain_silver_stake_events') }} as se
),
unstakes AS (
    SELECT
        block_timestamp,
        tx_id,
        pool_name,
        from_address,
        to_address,
        emit_rune_e8,
        emit_asset_e8,
        stake_units,
        imp_loss_protection_e8,
        asymmetry,
        basis_points,
        event_id,
        _inserted_timestamp
    FROM
        {{ ref('thorchain_silver_withdraw_events') }}
)
SELECT
    cast(date_trunc('day', b.block_timestamp) AS date) AS day,
    b.block_timestamp,
    b.height AS block_id,
    rune_tx_id AS tx_id,
    'add_liquidity' AS lp_action,
    se.pool_name,
    rune_address AS from_address,
    NULL AS to_address,
    COALESCE((rune_e8 / pow(10, 8)), 0) AS rune_amount,
    COALESCE((rune_e8 / pow(10, 8) * rune_usd), 0) AS rune_amount_usd,
    COALESCE((asset_e8 / pow(10, 8)), 0) AS asset_amount,
    COALESCE((asset_e8 / pow(10, 8) * asset_usd), 0) AS asset_amount_usd,
    stake_units,
    asset_tx_id,
    asset_address,
    asset_blockchain,
    NULL AS il_protection,
    NULL AS il_protection_usd,
    NULL AS unstake_asymmetry,
    NULL AS unstake_basis_points,
    concat_ws(
        '-',
        event_id,
        cast(se.block_timestamp as varchar),
        COALESCE(
            tx_id,
            ''
        ),
        lp_action,
        se.pool_name,
        COALESCE(
            from_address,
            ''
        ),
        COALESCE(
            to_address,
            ''
        ),
        COALESCE(
            asset_tx_id,
            ''
        )
    ) AS _unique_key,
    se._inserted_timestamp
FROM
  stakes se
JOIN {{ ref('thorchain_silver_block_log') }} as b
    ON se.block_timestamp = b.timestamp
LEFT JOIN {{ ref('thorchain_silver_prices') }} as p
    ON b.height = p.block_id
    AND se.pool_name = p.pool_name
{% if is_incremental() -%}
WHERE
    {{ incremental_predicate('b.block_timestamp') }}
{% endif -%}

UNION

SELECT
    cast(date_trunc('day', b.block_timestamp) AS date) AS day,
    b.block_timestamp,
    b.height AS block_id,
    tx_id,
    'remove_liquidity' AS lp_action,
    ue.pool_name,
    from_address,
    to_address,
    COALESCE(emit_rune_e8 / pow(10, 8), 0) AS rune_amount,
    COALESCE(emit_rune_e8 / pow(10, 8) * rune_usd, 0) AS rune_amount_usd,
    COALESCE(emit_asset_e8 / pow(10, 8), 0) AS asset_amount,
    COALESCE(emit_asset_e8 / pow(10, 8) * asset_usd, 0) AS asset_amount_usd,
    stake_units,
    NULL AS asset_tx_id,
    NULL AS asset_address,
    NULL AS asset_blockchain,
    imp_loss_protection_e8 / pow(
        10,
        8
    ) AS il_protection,
    imp_loss_protection_e8 / pow(
        10,
        8
    ) * rune_usd AS il_protection_usd,
    asymmetry AS unstake_asymmetry,
    basis_points AS unstake_basis_points,
    concat_ws(
        '-',
        event_id,
        cast(ue.block_timestamp as varchar),
        COALESCE(
            tx_id,
            ''
        ),
        lp_action,
        ue.pool_name,
        COALESCE(
            from_address,
            ''
        ),
        COALESCE(
            to_address,
            ''
        ),
        COALESCE(
            asset_tx_id,
            ''
        )
    ) AS _unique_key,
    ue._inserted_timestamp
FROM
  unstakes ue
JOIN {{ ref('thorchain_silver_block_log') }} as b
    ON ue.block_timestamp = b.timestamp
LEFT JOIN {{ ref('thorchain_silver_prices') }} as p
    ON b.height = p.block_id
    AND ue.pool_name = p.pool_name
{% if is_incremental() -%}
WHERE {{ incremental_predicate('b.block_timestamp') }}
{% endif -%}