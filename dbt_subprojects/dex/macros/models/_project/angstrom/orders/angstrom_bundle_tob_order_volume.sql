{% macro
    angstrom_bundle_tob_order_volume(
        angstrom_contract_addr,
        controller_v1_contract_addr,
        earliest_block,
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
    FROM ({{angstrom_decoding_top_of_block_orders(angstrom_contract_addr, earliest_block, blockchain)}}) AS ab
    INNER JOIN ({{ angstrom_bundle_indexes_to_assets(angstrom_contract_addr, earliest_block, blockchain) }}) AS asts
        ON asts.bundle_pair_index = ab.pairs_index AND ab.block_number = asts.block_number AND ab.tx_hash = asts.tx_hash
    ),
    tob_orders_with_pool AS (
        SELECT
            u.*,
            f.pool_id AS pool_id
        FROM tob_orders AS u
        INNER JOIN ({{ angstrom_pool_info(controller_v1_contract_addr, earliest_block, blockchain) }}) AS f
            ON u.block_number = f.block_number AND
                ((varbinary_substring(f.topic1, 13, 20) = u.asset_in OR varbinary_substring(f.topic2, 13, 20) = u.asset_in) AND 
                (varbinary_substring(f.topic1, 13, 20) = u.asset_out OR varbinary_substring(f.topic2, 13, 20) = u.asset_out)) 
    )
SELECT
    t.*,
    if(t.zero_for_1, f.tob_donated_amount_t0, 0) AS fees_paid_asset_in,
    if(t.zero_for_1, 0, f.tob_donated_amount_t0) AS fees_paid_asset_out
FROM tob_orders_with_pool AS t
INNER JOIN ({{ angstrom_bundle_tob_fees_donated(angstrom_contract_addr, controller_v1_contract_addr, earliest_block, blockchain) }}) AS f
    ON t.tx_hash = f.tx_hash AND t.pairs_index = f.pair_index

{% endmacro %}
