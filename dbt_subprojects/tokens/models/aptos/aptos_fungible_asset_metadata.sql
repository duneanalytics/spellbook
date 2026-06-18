{{ config(
    schema = 'aptos_fungible_asset',
    alias = 'metadata',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_hash', 'asset_type'],
    partition_by = ['block_month'],
    post_hook='{{ expose_spells(blockchains = \'["aptos"]\',
        spell_type = "project",
        spell_name = "fungible_asset",
        contributors = \'["ying-w"]\') }}'
) }}

-- `fungible_asset_metadata` from indexer is a current table, this table has historical
-- for FA, the 3 resources are grouped under the same state key hash (fungible_asset::Metadata, fungible_asset::ConcurrentSupply, object::ObjectCore)
-- so will be emitted together (parse without new lookup)
-- edge cases:
-- indexer table removes assets when LENGTH(asset_type) > 1_000 Ex 521538257
-- asset_name can be unicode: Ex 321165430
-- creator_address is the asset address for v1 and owner for v2 (can change for v2)
-- creator_address is not needed for coins/fa, it's a holdover from tokens (where it is used as key with name)

-- v2: the 3 co-emitted FA resources share (tx_version, move_address), so read them in a SINGLE
-- pass -- one scan + conditional aggregation grouped by that key -- instead of 3 self-joined
-- scans of move_resources. (Trino inlines CTEs, so a shared CTE would re-scan; the GROUP BY is
-- what collapses the read. HAVING keeps the metadata-driven semantics of the old LEFT JOINs.)
WITH mr_fa AS (
    SELECT
        tx_version,
        move_address,
        MAX(CASE WHEN move_resource_module = 'fungible_asset' AND move_resource_name = 'Metadata' THEN tx_hash END) AS tx_hash,
        MAX(CASE WHEN move_resource_module = 'fungible_asset' AND move_resource_name = 'Metadata' THEN block_date END) AS block_date,
        MAX(CASE WHEN move_resource_module = 'fungible_asset' AND move_resource_name = 'Metadata' THEN block_time END) AS block_time,
        MAX(CASE WHEN move_resource_module = 'fungible_asset' AND move_resource_name = 'Metadata' THEN write_set_change_index END) AS write_set_change_index,
        MAX(CASE WHEN move_resource_module = 'fungible_asset' AND move_resource_name = 'Metadata' THEN json_extract_scalar(move_data, '$.name') END) AS asset_name,
        MAX(CASE WHEN move_resource_module = 'fungible_asset' AND move_resource_name = 'Metadata' THEN json_extract_scalar(move_data, '$.symbol') END) AS asset_symbol,
        MAX(CASE WHEN move_resource_module = 'fungible_asset' AND move_resource_name = 'Metadata' THEN CAST(json_extract_scalar(move_data, '$.decimals') AS INTEGER) END) AS decimals,
        MAX(CASE WHEN move_resource_module = 'fungible_asset' AND move_resource_name = 'Metadata' THEN json_extract_scalar(move_data, '$.icon_uri') END) AS icon_uri,
        MAX(CASE WHEN move_resource_module = 'fungible_asset' AND move_resource_name = 'Metadata' THEN json_extract_scalar(move_data, '$.project_uri') END) AS project_uri,
        MAX(CASE WHEN move_resource_module = 'fungible_asset' AND move_resource_name IN ('Supply', 'ConcurrentSupply') THEN
            CAST(
                COALESCE(
                    json_extract_scalar(move_data, '$.current'),
                    json_extract_scalar(move_data, '$.current.value')
                ) AS UINT256
            )
        END) AS supply_v2,
        MAX(CASE WHEN move_resource_module = 'fungible_asset' AND move_resource_name IN ('Supply', 'ConcurrentSupply') THEN
            CAST(
                COALESCE(
                    json_extract_scalar(move_data, '$.maximum.vec[0]'),
                    json_extract_scalar(move_data, '$.current.max_value')
                ) AS UINT256
            )
        END) AS maximum_v2,
        MAX(CASE WHEN move_resource_module = 'object' AND move_resource_name = 'ObjectCore' THEN
            '0x' || LPAD(LTRIM(json_extract_scalar(move_data, '$.owner'), '0x'), 64, '0')
        END) AS owner_address
    FROM {{ source('aptos', 'move_resources') }}
    WHERE 1=1
        AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
        AND (
            (move_resource_module = 'fungible_asset' AND move_resource_name IN ('Metadata', 'Supply', 'ConcurrentSupply'))
            OR (move_resource_module = 'object' AND move_resource_name = 'ObjectCore')
        )
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% else %}
        AND block_date >= DATE('2023-07-27') -- beginning of FA (v2)
    {% endif %}
    {% if target.name == 'ci' %}
    -- bound the full-refresh scan in CI so the build completes and the regression test runs; prod is unaffected
    AND block_date >= current_date - interval '14' day
    {% endif %}
    GROUP BY tx_version, move_address
    -- only emit assets that have a Metadata resource (matches the old Metadata-driven LEFT JOINs)
    HAVING MAX(CASE WHEN move_resource_module = 'fungible_asset' AND move_resource_name = 'Metadata' THEN 1 ELSE 0 END) = 1
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
        CAST(json_extract_scalar(move_data, '$.supply.vec[0].integer.vec[0].value') AS UINT256) AS supply_v1
        -- only APT uses aggregator
        -- json_extract_scalar(move_data, '$.supply.vec[0].aggregator.vec[0].handle') AS supply_aggregator_table_handle_v1,
        -- json_extract_scalar(move_data, '$.supply.vec[0].aggregator.vec[0].key') AS supply_aggregator_table_key_v1,
    FROM {{ source('aptos', 'move_resources') }}
    WHERE 1=1
        AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
        AND move_resource_module = 'coin'
        AND move_resource_name = 'CoinInfo'
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% endif %}
    {% if target.name == 'ci' %}
    -- bound the full-refresh scan in CI so the build completes and the regression test runs; prod is unaffected
    AND block_date >= current_date - interval '14' day
    {% endif %}
)

SELECT
    ci.tx_version,
    ci.tx_hash,
    ci.block_date,
    ci.block_time,
    date(date_trunc('month', ci.block_time)) as block_month,
    --
    write_set_change_index,
    IF(LENGTH(SPLIT(asset_type, '::')[1]) != 66,
        '0x' || LPAD(LTRIM(SPLIT(asset_type, '::')[1],'0x'),64, '0') || 
        SUBSTR(asset_type, LENGTH(SPLIT(asset_type, '::')[1])+1, LENGTH(asset_type)),
        asset_type
    ) AS asset_type,
    creator_address AS owner_address,
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
FROM mr_coin_info AS ci
LEFT JOIN {{ ref('aptos_fungible_asset_migration') }} AS m
    ON m.asset_type_v1 = ci.asset_type

UNION ALL

SELECT
    m.tx_version,
    m.tx_hash,
    m.block_date,
    m.block_time,
    date(date_trunc('month', m.block_time)) as block_month,
    --
    m.write_set_change_index,
    '0x' || LPAD(lower(to_hex(m.move_address)), 64, '0') AS asset_type,
    from_hex(m.owner_address) AS owner_address,
    m.asset_name,
    m.asset_symbol,
    m.decimals,
    m.supply_v2 AS supply,
    'v2' AS token_standard,
    mig.asset_type_v1 AS asset_type_migrated,
    -- below are v2 only
    m.icon_uri,
    m.project_uri,
    m.maximum_v2 AS maximum
FROM mr_fa m
LEFT JOIN {{ ref('aptos_fungible_asset_migration') }} AS mig
    ON mig.asset_type_v2 = '0x' || LPAD(lower(to_hex(m.move_address)), 64, '0')
