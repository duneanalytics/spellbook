{{ config(
    schema = 'aptos_fungible_asset',
    alias = 'migration',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['asset_type_v2'],
) }}

-- coin to FA mapping is a deterministic lookup using SHA3
-- however, SHA3 is not implemented in SQL so instead lookup using resource
-- For pythno code using SHA3, see 'Finding migrated fungible assets' section of
-- https://medium.com/aptoslabs/data-analyst-guide-to-aptos-defi-swaps-pt2-e343ac6be84e 
SELECT
    -- latest
    tx_version,
    block_date,
    block_time,
    DATE(date_trunc('month', block_time)) as block_month,
    --
    '0x' || LPAD(lower(to_hex(move_address)), 64, '0') AS asset_type_v2,
    '0x' || LPAD(LTRIM(json_extract_scalar(move_data, '$.type.account_address'), '0x'), 64, '0') || '::' ||
    FROM_UTF8(FROM_HEX(LTRIM(json_extract_scalar(move_data, '$.type.module_name'), '0x'))) || '::' ||
    FROM_UTF8(FROM_HEX(LTRIM(json_extract_scalar(move_data, '$.type.struct_name'), '0x'))) AS asset_type_v1
FROM (
    SELECT
        MAX(tx_version) AS tx_version,
        MAX(block_date) AS block_date,
        MAX(block_time) AS block_time,
        --
        move_address,
        ANY_VALUE(move_data) AS move_data
    FROM {{ source('aptos', 'move_resources') }}
    WHERE 1=1
        AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
        AND move_resource_module = 'coin'
        AND move_resource_name = 'PairedCoinType'
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% else %}
        AND block_date >= DATE('2024-08-02') -- beginning of FA (v2) migration
    {% endif %}
    GROUP BY move_address
)
