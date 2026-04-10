{{ config(
    schema = 'aptos_fungible_asset',
    alias = 'fa_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'unique_key'],
    partition_by = ['block_month']
) }}

WITH fa_balance AS (
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
            c.balance, -- ConcurrentFungibleBalance
            json_extract_scalar(move_data, '$.balance') -- FungibleStore
        ) AS balance,
        CAST(json_extract_scalar(move_data, '$.frozen') AS BOOLEAN) AS is_frozen
    FROM {{ source('aptos', 'move_resources') }} mr
    LEFT JOIN (
        -- if Concurrent balance exists, use it
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
            {% if is_incremental() -%}
            AND {{ incremental_predicate('block_time') }}
            {% endif -%}
    ) c USING (tx_version, move_address)
    LEFT JOIN (
        -- get owner, if deleted in this tx then owner will be NULL
        -- to fix this, need to create an Objects table and map to owner before delete
        SELECT
            tx_version,
            move_address,
            '0x' || LPAD(LTRIM(json_extract_scalar(move_data, '$.owner'), '0x'), 64, '0') AS owner_address
        FROM {{ source('aptos', 'move_resources') }}
        WHERE 1=1
            AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
            AND move_resource_module = 'object'
            AND move_resource_name IN ('ObjectGroup','ObjectCore')
            {% if is_incremental() -%}
            AND {{ incremental_predicate('block_time') }}
            {% endif -%}
    ) AS fs_owner USING(tx_version, move_address)
    WHERE 1=1
        AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
        AND move_resource_module = 'fungible_asset'
        AND move_resource_name = 'FungibleStore'
        {% if is_incremental() -%}
        AND {{ incremental_predicate('block_time') }}
        {% endif -%}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'write_set_change_index']) }} AS unique_key,
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