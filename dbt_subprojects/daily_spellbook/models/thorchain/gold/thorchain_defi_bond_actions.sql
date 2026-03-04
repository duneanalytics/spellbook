{{ config(
    schema = 'thorchain',
    alias = 'defi_bond_actions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_bond_actions_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'defi', 'bond_actions', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH block_prices AS (
    SELECT
        AVG(rune_usd) AS rune_usd,
        block_id
    FROM
        {{ ref('thorchain_silver_prices') }}
    GROUP BY
        block_id
),
bond_events AS (
    SELECT
        block_timestamp,
        tx_id,
        from_address,
        to_address,
        asset,
        blockchain,
        bond_type,
        asset_e8,
        e8,
        memo,
        event_id,
        _TX_TYPE,
        _inserted_timestamp
    FROM
        {{ ref('thorchain_silver_bond_events') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['be.tx_id','be.from_address','be.to_address ','be.asset_e8','be.bond_type','be.e8','be.block_timestamp','be.blockchain','be.asset','be.memo']
    ) }} AS fact_bond_actions_id,
    b.block_timestamp,
    COALESCE(
        b.dim_block_id,
        '-1'
    ) AS dim_block_id,
    tx_id,
    from_address,
    to_address,
    asset,
    blockchain,
    bond_type,
    COALESCE(e8 / pow(10, 8), 0) AS asset_amount,
    COALESCE(
        rune_usd * asset_e8,
        0
    ) AS asset_usd,
    memo,
    _TX_TYPE,
    be._inserted_timestamp,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM
     bond_events be
JOIN {{ ref('thorchain_core_block') }} as b
    ON be.block_timestamp = b.timestamp
LEFT JOIN block_prices p
    ON b.block_id = p.block_id
{% if is_incremental() -%}
WHERE {{ incremental_predicate('b.block_timestamp') }}
OR tx_id IN (
    SELECT
        tx_id
    FROM
        {{ this }}
    WHERE
        dim_block_id = '-1'
)
{% endif -%}
