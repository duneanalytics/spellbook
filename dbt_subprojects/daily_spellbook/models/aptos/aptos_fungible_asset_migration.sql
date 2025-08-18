{{ config(
    schema = 'aptos_fungible_asset',
    alias = 'migrations',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['asset_type_v2'],
) }}

SELECT
    tx_version_latest,
    -- tx_hash_latest,
    block_date_latest,
    block_time_latest,
    date(date_trunc('month', block_time_latest)) as block_month_latest,
    --
    '0x' || LPAD(lower(to_hex(move_address)), 64, '0') AS asset_type_v2,
    '0x' || LPAD(LTRIM(json_extract_scalar(move_data, '$.type.account_address'), '0x'), 64, '0') || '::' ||
    FROM_UTF8(FROM_HEX(LTRIM(json_extract_scalar(move_data, '$.type.module_name'), '0x'))) || '::' ||
    FROM_UTF8(FROM_HEX(LTRIM(json_extract_scalar(move_data, '$.type.struct_name'), '0x'))) AS asset_type_v1
FROM (
    SELECT
        MAX(tx_version) AS tx_version_latest,
        MAX(block_date) AS block_date_latest,
        MAX(block_time) AS block_time_latest,
        --
        move_address,
        ANY_VALUE(move_data) AS move_data
    FROM {{ source('aptos', 'move_resources') }}
    WHERE 1=1
        AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
        AND move_resource_module = 'coin'
        AND move_resource_name = 'PairedCoinType'
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time_latest') }}
    {% else %}
        AND block_date = DATE('2025-08-01') -- DEBUG
        AND block_date >= DATE('2024-08-02') -- beginning of FA (v2) migration
    {% endif %}
    GROUP BY move_address
)
