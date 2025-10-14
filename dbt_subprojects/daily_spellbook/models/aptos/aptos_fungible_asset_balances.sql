{{ config(
    schema = 'aptos_fungible_asset',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_hash', 'write_set_change_index'],
    partition_by = ['block_month'],
    post_hook='{{ expose_spells(blockchains = \'["aptos"]\',
        spell_type = "project",
        spell_name = "fungible_asset",
        contributors = \'["ying-w"]\') }}'
) }}

-- Compared to GraphQL endpoint, this table has the following differences:
-- 1. Historical balances (rather than only current balance)
-- 2. When FungibleStore and ObjectCore are desync, representation can be different GraphQL (prior to delete event)
--   a. FungibleStore when ObjectCore is deleted ex 2424873868 idx 8 has no ObjectCore -> disallowed later
--   b. ObjectCore when FungibleStore is deleted ex 2975888978 idx 6 has no FungibleStore -> infer from event
-- 3. If an asset has both ConcurrentFungibleBalance and FungibleStore, write_set_change_index is former for this table

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
        {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
        {% endif %}
    ) AS fs_owner USING(tx_version, move_address)
    WHERE 1=1
        AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
        AND move_resource_module = 'fungible_asset'
        AND move_resource_name = 'FungibleStore'
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% endif %}
), fs_deletion AS (
    -- secondary (non-primary) FA Stores can be deleted without deleting object
    -- Store needs to have 0 balance to be deleted
    -- Ex 2975888978 store 0x1198c59f6c6f84f0a52f1a8524fbaac60946c7e2ac74a87bf7632fa50e23f34d
        SELECT
            ev.tx_version,
            ev.tx_hash,
            ev.block_date,
            ev.block_time,
            --
            mr.write_set_change_index,  -- use ObjectCore change index
            '0x' || LPAD(LTRIM(json_extract_scalar(data, '$.metadata'), '0x'), 64, '0') AS asset_type,
            address_32_from_hex(json_extract_scalar(data, '$.store')) AS storage_id,
            '0x' || LPAD(LTRIM(json_extract_scalar(data, '$.owner'), '0x'), 64, '0') AS owner_address,
            TRUE AS move_is_deletion, -- object isn't deleted but store is
            CAST(0 AS UINT256) AS amount
        FROM {{ source('aptos', 'events') }} ev
        LEFT JOIN {{ source('aptos', 'move_resources') }} mr
            ON mr.tx_version = ev.tx_version
            AND mr.move_address = address_32_from_hex(json_extract_scalar(ev.data, '$.store'))
            AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
            AND move_resource_module = 'object'
            AND move_resource_name = 'ObjectCore'
            AND mr.block_date = ev.block_date -- optimization
        WHERE 1=1
        AND ev.event_type = '0x1::fungible_asset::FungibleStoreDeletion'
        AND ev.block_date >= DATE('2025-04-28') -- date enabled
        AND mr.block_date >= DATE('2025-04-28') -- date enabled
    {% if is_incremental() %}
        AND {{ incremental_predicate('ev.block_time') }}
        AND {{ incremental_predicate('mr.block_time') }}
    {% endif %}
)

SELECT
    tx_version,
    tx_hash,
    block_date,
    block_time,
    date(date_trunc('month', block_time)) as block_month,
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
    tx_version,
    tx_hash,
    block_date,
    block_time,
    date(date_trunc('month', block_time)) as block_month,
    --
    write_set_change_index,
    '0x' || LPAD(LTRIM(asset_type, '0x'), 64, '0') AS asset_type,
    from_hex(owner_address) AS owner_address,
    move_address AS storage_id,
    CAST(IF(move_is_deletion, '0', balance) AS UINT256) AS amount,
    is_frozen,
    'v2' AS token_standard
FROM fa_balance

UNION ALL

SELECT
    tx_version,
    tx_hash,
    block_date,
    block_time,
    date(date_trunc('month', block_time)) as block_month,
    --
    write_set_change_index, -- uses object core
    asset_type,
    from_hex(owner_address) AS owner_address,
    storage_id,
    amount,
    NULL AS is_frozen,
    'v2' AS token_standard
FROM fs_deletion
