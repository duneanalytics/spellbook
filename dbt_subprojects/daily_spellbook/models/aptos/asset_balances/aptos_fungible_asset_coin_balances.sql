{{ config(
    schema = 'aptos_fungible_asset',
    alias = 'coin_balances',
    materialized = 'table',
    file_format = 'delta',
    partition_by = ['block_month'],
    tags = ['static']
) }}

/*
    - this model has a cutoff date of 2025-09-02 to exclude coin balances after migration
    - leverage static tag to only run on initial build
*/

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
        AND block_date <= DATE('2025-09-02')
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