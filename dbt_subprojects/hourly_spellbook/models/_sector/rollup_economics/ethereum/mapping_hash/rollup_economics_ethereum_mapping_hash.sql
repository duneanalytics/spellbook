{{ config(
    schema = 'rollup_economics_ethereum',
    alias = 'mapping_hash',
    materialized = 'table',
    unique_key = ['hash_value', 'updated_at']
) }}

WITH mapping_data AS (
    SELECT *
    FROM {{ source("growthepie", "l2economics_mapping", database="dune") }}
),

latest_hash AS (
    SELECT 
        md5(to_utf8(
            '[' || array_join(
                array_agg(distinct json_format(
                    json_parse(json_array(
                        coalesce(l2, ''), 
                        coalesce(name, ''), 
                        coalesce(settlement_layer, ''), 
                        coalesce(to_hex(from_address), '0x'), 
                        coalesce(to_hex(to_address), '0x'), 
                        coalesce(to_hex(method), '0x'), 
                        coalesce(namespace, '')
                    )))
                ), 
                ','
            ) || ']'
        )) AS hash_value
    FROM mapping_data
)

SELECT 
    hash_value, 
    CURRENT_TIMESTAMP AS updated_at
FROM latest_hash