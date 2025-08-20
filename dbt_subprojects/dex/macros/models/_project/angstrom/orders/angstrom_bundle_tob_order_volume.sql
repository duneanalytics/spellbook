{% macro
    angstrom_bundle_tob_order_volume(
        angstrom_contract_addr,
        controller_v1_contract_addr,
        blockchain
    )
%}




WITH 
    tob_orders AS (
    SELECT 
        ab.*,
        if(ab.zero_for_1, asts.asset_in, asts.asset_out) AS asset_in,
        if(ab.zero_for_1, asts.asset_out, asts.asset_in) AS asset_out,
        asts.price_1over0
    FROM ({{angstrom_decoding_top_of_block_orders(angstrom_contract_addr, blockchain)}}) AS ab
    INNER JOIN ({{ angstrom_bundle_indexes_to_assets(angstrom_contract_addr, blockchain) }}) AS asts
        ON asts.bundle_pair_index = ab.pairs_index AND ab.block_number = asts.block_number AND ab.tx_hash = asts.tx_hash
    ),
    tob_orders_with_pool AS (
        SELECT
            u.*,
            f.pool_id AS pool_id
        FROM tob_orders AS u
        INNER JOIN ({{ angstrom_pool_info(controller_v1_contract_addr, blockchain) }}) AS f
            ON u.block_number = f.block_number AND
                ((varbinary_substring(f.topic1, 13, 20) = u.asset_in OR varbinary_substring(f.topic2, 13, 20) = u.asset_in) AND 
                (varbinary_substring(f.topic1, 13, 20) = u.asset_out OR varbinary_substring(f.topic2, 13, 20) = u.asset_out)) 
    )
SELECT
    *
FROM tob_orders_with_pool

{% endmacro %}
