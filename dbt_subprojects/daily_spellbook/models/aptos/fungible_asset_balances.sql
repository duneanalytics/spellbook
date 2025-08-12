{{ config(
    schema = 'aptos_fungible_asset',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['tx_hash', 'write_set_change_index'],
    partition_by = ['block_month'],
) }}

-- TODO: edge cases around deleted balances / objects

WITH coin_balances AS (
    SELECT
        tx_version,
        tx_hash,
        block_date,
        block_time,
        --
        write_set_change_index,
        move_resource_generic_type_params[1] AS asset_type,
        move_address,
        IF(move_is_deletion, '0', json_extract_scalar(move_data, '$.coin.value')) AS balance,
        CAST(json_extract_scalar(move_data, '$.frozen') AS BOOLEAN) AS is_frozen
    FROM {{ source('aptos', 'move_resources') }}
    WHERE 1=1
        AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
        AND move_resource_module = 'coin'
        AND move_resource_name = 'CoinStore'
        AND block_date < DATE('2025-08-05')  -- almost all migrated
        AND block_date = DATE('2025-01-01') -- DEBUG
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% endif %}
), fa_balance AS (
    SELECT
        tx_version,
        tx_hash,
        block_date,
        block_time,
        --
        COALESCE(c.write_set_change_index, mr.write_set_change_index) AS write_set_change_index,
        json_extract_scalar(move_data, '$.metadata.inner') AS asset_type,
        move_address,
        fs_owner.owner_address,
        move_is_deletion,
        COALESCE(
            c.balance, -- CFB
            json_extract_scalar(move_data, '$.balance') -- FS
        ) AS balance,
        CAST(json_extract_scalar(move_data, '$.frozen') AS BOOLEAN) AS is_frozen
    FROM {{ source('aptos', 'move_resources') }} mr
    LEFT JOIN (
        -- if CFB
        SELECT
            tx_version,
            move_address,
            write_set_change_index,
            json_extract_scalar(move_data, '$.balance.value') AS balance
        FROM {{ source('aptos', 'move_resources') }}
        WHERE 1=1
            AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
            AND move_resource_module = 'fungible_asset'
            AND move_resource_name = 'ConcurrentFungibleBalance'
            AND block_date = DATE('2025-01-01') -- DEBUG
        {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
        {% endif %}
    ) c USING (tx_version, move_address)
    LEFT JOIN (
        -- get owner
        SELECT
            tx_version,
            move_address,
            '0x' || LPAD(LTRIM(json_extract_scalar(move_data, '$.owner'), '0x'), 64, '0') AS owner_address
        FROM {{ source('aptos', 'move_resources') }}
        WHERE 1=1
            AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
            AND move_resource_module = 'object'
            AND move_resource_name = 'ObjectCore'
            AND block_date = DATE('2025-01-01') -- DEBUG
        {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
        {% endif %}
    ) AS fs_owner USING(tx_version, move_address)
    WHERE 1=1
        AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
        AND move_resource_module = 'fungible_asset'
        AND move_resource_name = 'FungibleStore'
        AND block_date = DATE('2025-01-01') -- DEBUG
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% endif %}
)

SELECT
    block_date,
    tx_version,
    block_time,
    date(date_trunc('month', block_time)) as block_month,
    tx_hash,
    --
    write_set_change_index,
    IF(LENGTH(SPLIT(asset_type, '::')[1]) != 66,
        '0x' || LPAD(LTRIM(SPLIT(asset_type, '::')[1],'0x'),64, '0') || 
        SUBSTR(asset_type, LENGTH(SPLIT(asset_type, '::')[1])+1, LENGTH(asset_type)),
        asset_type
    ) AS asset_type,
    move_address AS owner_address,
    CAST(NULL AS varbinary) AS storage_id,
    CAST(balance AS UINT256) AS amount,
    is_frozen, -- null on delete
    'v1' AS token_standard
FROM coin_balances

UNION ALL

SELECT
    block_date,
    tx_version,
    block_time,
    date(date_trunc('month', block_time)) as block_month,
    tx_hash,
    --
    write_set_change_index,
    '0x' || LPAD(LTRIM(asset_type, '0x'), 64, '0') AS asset_type,
    from_hex(owner_address) AS owner_address,
    move_address AS storage_id,
    CAST(IF(move_is_deletion, '0', balance) AS UINT256) AS amount,
    is_frozen,
    'v2' AS token_standard
FROM fa_balance
