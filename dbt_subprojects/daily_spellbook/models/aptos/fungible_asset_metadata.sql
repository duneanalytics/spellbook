-- `fungible_asset_metadata` from indexer is a current table, this table has historical
-- for FA, the 3 resources are grouped under the same state key (fungible_asset::Metadata, fungible_asset::ConcurrentSupply, object::ObjectCore)
-- so will be emitted together (parse without new lookup)
-- edge cases
-- indexer table removes assets when LENGTH(asset_type) > 1_000 Ex 521538257
-- asset_name can be unicode: Ex 321165430
-- creator_address is the asset address for v1 and owner for v2 (can change for v2)
-- creator_address is not needed for coins/fa, it's a holdover from tokens (where it is used as key with name)
{{ config(
    materialized='incremental',
    unique_key=['tx_hash', 'asset_type'],
    partition_by = ['block_date'],
) }}

WITH mr_fa_metadata AS (
    SELECT
        tx_version,
        tx_hash,
        block_date,
        block_time,
        --
        write_set_change_index,
        move_address,
        json_extract_scalar(move_data, '$.name') AS asset_name,
        json_extract_scalar(move_data, '$.symbol') AS asset_symbol,
        CAST(json_extract_scalar(move_data, '$.decimals') AS INTEGER) AS decimals,
        json_extract_scalar(move_data, '$.icon_uri') AS icon_uri,
        json_extract_scalar(move_data, '$.project_uri') AS project_uri
    FROM {{ source('aptos', 'move_resources') }}
    WHERE 1=1
        AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
        AND move_resource_module = 'fungible_asset'
        AND move_resource_name = 'Metadata'
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% else %}
        AND block_date = DATE('2025-01-01') -- DEBUG
        AND block_date >= DATE('2023-07-27') -- beginning of FA (v2)
    {% endif %}
), mr_fa_supply AS (
    SELECT
        tx_version,
        move_address,
        CAST(
            COALESCE(
                json_extract_scalar(move_data, '$.current'),
                json_extract_scalar(move_data, '$.current.value')
            ) AS UINT256
        ) AS supply_v2,
        CAST(
            COALESCE(
                json_extract_scalar(move_data, '$.maximum.vec[0]'),
                json_extract_scalar(move_data, '$.current.max_value')
            ) AS UINT256
        ) AS maximum_v2
    FROM {{ source('aptos', 'move_resources') }}
    WHERE 1=1
        AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
        AND move_resource_module = 'fungible_asset'
        AND move_resource_name IN ('Supply', 'ConcurrentSupply')
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% else %}
        AND block_date = DATE('2025-01-01') -- DEBUG
        AND block_date >= DATE('2023-07-27') -- beginning of FA (v2)
    {% endif %}
), mr_fa_owner AS (
    SELECT
        tx_version,    
        move_address,
        json_extract_scalar(move_data, '$.owner') AS owner_address
    FROM {{ source('aptos', 'move_resources') }}
    WHERE 1=1
        AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
        AND move_resource_module = 'object'
        AND move_resource_name = 'ObjectCore'
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% else %}
        AND block_date = DATE('2025-01-01') -- DEBUG
        AND block_date >= DATE('2023-07-27') -- beginning of FA (v2)
    {% endif %}
), mr_coin_info AS (
    SELECT
        tx_version,
        tx_hash,
        block_date,
        block_time,
        --
        write_set_change_index,
        array_join(move_resource_generic_type_params, '') AS asset_type,
        move_address AS creator_address,
        json_extract_scalar(move_data, '$.name') AS asset_name,
        json_extract_scalar(move_data, '$.symbol') AS asset_symbol,
        CAST(json_extract_scalar(move_data, '$.decimals') AS INT) AS decimals,
        CAST(json_extract_scalar(move_data, '$.supply.vec[0].integer.vec[0].value') AS BIGINT) AS supply_v1
        -- only APT uses aggregator
        -- json_extract_scalar(move_data, '$.supply.vec[0].aggregator.vec[0].handle') AS supply_aggregator_table_handle_v1,
        -- json_extract_scalar(move_data, '$.supply.vec[0].aggregator.vec[0].key') AS supply_aggregator_table_key_v1,
    FROM {{ source('aptos', 'move_resources') }}
    WHERE 1=1
        AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
        AND move_resource_module = 'coin'
        AND move_resource_name = 'CoinInfo'
        AND block_date = DATE('2025-01-01') -- DEBUG
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% endif %}
)

SELECT
    block_date,
    tx_version,
    block_time,
    tx_hash,
    --
    write_set_change_index,
    IF(LENGTH(SPLIT(asset_type, '::')[1]) != 66,
        '0x' || LPAD(LTRIM(SPLIT(asset_type, '::')[1],'0x'),64, '0') || 
        SUBSTR(asset_type, LENGTH(SPLIT(asset_type, '::')[1])+1, LENGTH(asset_type)),
        asset_type
    ) AS asset_type,
    from_hex('0x' || LPAD(LTRIM(creator_address, '0x'), 64, '0')) AS owner_address,
    asset_name,
    asset_symbol,
    decimals,
    supply_v1 AS supply,
    'v1' AS token_standard,
    m.asset_type_v2 AS asset_type_migrated,
    -- below are v2 only
    NULL AS icon_uri,
    NULL AS project_uri,
    NULL AS maximum
FROM mr_coin_info
LEFT JOIN {{ ref('fungible_asset_migration') }} AS m
    ON m.asset_type_v1 = mr_coin_info.asset_type

UNION ALL

SELECT
    m.block_date,
    m.tx_version,
    m.block_time,
    m.tx_hash,
    --
    write_set_change_index,
    '0x' || LPAD(lower(to_hex(m.move_address)), 64, '0') AS asset_type,
    '0x' || LPAD(LTRIM(o.owner_address, '0x'), 64, '0') AS owner_address,
    asset_name,
    asset_symbol,
    decimals,
    s.supply_v2 AS supply,
    'v2' AS token_standard,
    mig.asset_type_v1 AS asset_type_migrated,
    -- below are v2 only
    icon_uri,
    project_uri,
    s.maximum_v2 AS maximum
FROM mr_fa_metadata m
LEFT JOIN mr_fa_supply AS s
    ON m.tx_version = s.tx_version AND m.move_address = s.move_address
LEFT JOIN mr_fa_owner AS o
    ON m.tx_version = o.tx_version AND m.move_address = o.move_address
LEFT JOIN {{ ref('fungible_asset_migration') }} AS mig
    ON mig.asset_type_v2 = m.asset_type
