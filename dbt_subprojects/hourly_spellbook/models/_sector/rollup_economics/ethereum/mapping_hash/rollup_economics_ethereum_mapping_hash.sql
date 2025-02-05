{{ config(
    materialized = 'table',
    schema = 'rollup_economics_ethereum',
    alias = 'mapping_hash'
) }}

WITH latest_hash AS (
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
    FROM {{ source("growthepie", "l2economics_mapping", database="dune") }}
)

SELECT 
    hash_value, 
    CURRENT_TIMESTAMP AS updated_at
FROM latest_hash;
