{% macro check_mapping_hash(settlement_layer) %}
    {%- set mapping_hash_check_query %}
        WITH mapping_data AS (
            SELECT *
            FROM {{ source("growthepie", "l2economics_mapping", database="dune") }}
            WHERE settlement_layer = '{{settlement_layer}}'
        ),
        latest_hash AS (
            SELECT hash_value 
            FROM {{ ref('rollup_economics_ethereum_mapping_hash') }} 
            ORDER BY updated_at DESC 
            LIMIT 1
        ),
        current_mapping_hash AS (
            SELECT 
                md5(to_utf8(
                    array_join(
                        array_agg(
                            json_format(
                                json_parse(json_array(
                                    coalesce(l2, ''), 
                                    coalesce(name, ''), 
                                    coalesce(settlement_layer, ''), 
                                    coalesce(to_hex(from_address), '0x'), 
                                    coalesce(to_hex(to_address), '0x'), 
                                    coalesce(to_hex(method), '0x'), 
                                    coalesce(namespace, '')
                                ))
                            )
                            ORDER BY l2, name, settlement_layer, from_address, to_address, method, namespace
                        ), 
                        ','
                    )
                )) AS hash_value
            FROM mapping_data
        )
        SELECT (SELECT hash_value FROM latest_hash) IS DISTINCT FROM (SELECT hash_value FROM current_mapping_hash) as needs_refresh
    {% endset %}

    {%- set needs_refresh = dbt_utils.get_single_value(mapping_hash_check_query, 'needs_refresh') %}
    {{ return(needs_refresh) }}
{% endmacro %}
