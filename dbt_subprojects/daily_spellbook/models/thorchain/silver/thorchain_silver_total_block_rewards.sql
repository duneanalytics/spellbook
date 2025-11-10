{{ config(
    schema = 'thorchain_silver',
    alias = 'total_block_rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_timestamp', 'block_id', 'reward_entity', '_unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'total_block_rewards', 'silver']
) }}

WITH block_prices AS (
    SELECT
        COALESCE(AVG(rune_usd), 0) AS rune_usd,
        block_id
    FROM {{ ref('thorchain_silver_prices') }}
    GROUP BY block_id
)
, fin AS (
    SELECT
        b.block_timestamp,
        b.height AS block_id,
        ree.pool_name AS reward_entity,
        COALESCE(ree.rune_e8 / pow(10, 8), 0) AS rune_amount,
        COALESCE(ree.rune_e8 / pow(10, 8) * COALESCE(p.rune_usd, 0), 0) AS rune_amount_usd,
        concat_ws(
            '-',
            cast(b.height as varchar),
            ree.pool_name
        ) AS _unique_key,
        ree._inserted_timestamp
    FROM
        {{ ref('thorchain_silver_rewards_event_entries') }} as ree
    JOIN {{ ref('thorchain_silver_block_log') }} as b
        ON ree.block_timestamp = b.timestamp
    LEFT JOIN {{ ref('thorchain_silver_prices') }} as p
        ON b.height = p.block_id
        AND ree.pool_name = p.pool_name
    {% if is_incremental() or true -%}
    WHERE 
    (
        {{ incremental_predicate('b.block_timestamp') }}   
        OR
        concat_ws(
            '-',
            cast(b.height as varchar),
            ree.pool_name
        ) IN 
        (
            SELECT
                _unique_key
            FROM
                {{ this }}
            WHERE
                rune_amount_USD IS NULL
        )
    )
    {% endif -%}
    UNION
    SELECT
        b.block_timestamp,
        b.height AS block_id,
        'bond_holders' AS reward_entity,
        bond_e8 / pow(
            10,
            8
        ) AS rune_amount,
        bond_e8 / pow(
            10,
            8
        ) * COALESCE(rune_usd, 0) AS rune_amount_usd,
        concat_ws(
            '-',
            cast(b.height as varchar),
            'bond_holders'
        ) AS _unique_key,
        re._inserted_timestamp
    FROM
        {{ ref('thorchain_silver_rewards_events') }} as re
    JOIN
        {{ ref('thorchain_silver_block_log') }} as b
        ON re.block_timestamp = b.timestamp
    LEFT JOIN
        block_prices as p
        ON b.height = p.block_id
    {% if is_incremental() or true -%}
    WHERE
    (
        {{ incremental_predicate('b.block_timestamp') }}
        OR concat_ws(
            '-',
            cast(b.height as varchar),
            'bond_holders'
        ) IN 
        (
            SELECT
                _unique_key
            FROM
                {{ this }}
            WHERE
                rune_amount_USD IS NULL
        )
    )
    {% endif -%}
)
SELECT
    block_timestamp,
    block_id,
    reward_entity,
    SUM(rune_amount) AS rune_amount,
    SUM(rune_amount_usd) AS rune_amount_usd,
    _unique_key,
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    fin
GROUP BY
    block_timestamp,
    block_id,
    reward_entity,
    _unique_key