{{ config(
    materialized='incremental',
    unique_key=['asset_type_v2'],
) }}

SELECT
    move_address AS asset_type_v2,
    '0x' || LPAD(LTRIM(json_extract_scalar(move_data, '$.type.account_address'), '0x'), 64, '0') || '::' ||
    FROM_UTF8(FROM_HEX(LTRIM(json_extract_scalar(move_data, '$.type.module_name'), '0x'))) || '::' ||
    FROM_UTF8(FROM_HEX(LTRIM(json_extract_scalar(move_data, '$.type.struct_name'), '0x'))) AS asset_type_v1
FROM (
    SELECT move_address, ANY_VALUE(move_data)   
    FROM {{ source('aptos', 'move_resources') }}
    WHERE 1=1
        AND move_module_address = 0x0000000000000000000000000000000000000000000000000000000000000001
        AND move_resource_module = 'coin'
        AND move_resource_name = 'PairedCoinType'
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        AND move_address NOT IN (SELECT asset_type_v2 FROM {{ this }})
    {% else %}
        AND block_date >= DATE('2024-08-02') -- beginning of FA (v2) migration
    {% endif %}
    GROUP BY 1
)
