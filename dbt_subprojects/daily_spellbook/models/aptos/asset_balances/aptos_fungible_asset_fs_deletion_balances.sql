{{ config(
    schema = 'aptos_fungible_asset',
    alias = 'fs_deletion_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_hash', 'write_set_change_index'],
    partition_by = ['block_month']
) }}

-- secondary (non-primary) FungibleStore can be deleted without deleting object
-- Store needs to have 0 balance to be deleted
-- Ex 2975888978 store 0x1198c59f6c6f84f0a52f1a8524fbaac60946c7e2ac74a87bf7632fa50e23f34d

WITH fs_deletion AS (
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
        AND move_resource_name IN ('ObjectGroup','ObjectCore')
        AND mr.block_date = ev.block_date -- optimization
        AND mr.block_date >= DATE('2025-04-28') -- date enabled
        {% if is_incremental() or true %}
        AND {{ incremental_predicate('mr.block_time') }}
        {% endif -%}
    WHERE 1=1
    AND ev.event_type = '0x1::fungible_asset::FungibleStoreDeletion'
    AND ev.block_date >= DATE('2025-04-28') -- date enabled
    {% if is_incremental() or true -%}
    AND {{ incremental_predicate('ev.block_time') }}
    {% endif -%}
)

SELECT
    tx_version,
    tx_hash,
    block_date,
    block_time,
    date(date_trunc('month', block_time)) as block_month,
    --
    write_set_change_index, -- uses ObjectCore change index
    asset_type,
    from_hex(owner_address) AS owner_address,
    storage_id,
    amount,
    NULL AS is_frozen,
    'v2' AS token_standard
FROM fs_deletion
