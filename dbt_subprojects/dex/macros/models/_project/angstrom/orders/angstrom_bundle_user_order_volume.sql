{% macro
    angstrom_bundle_user_order_volume(        
        angstrom_contract_addr, 
        controller_v1_contract_addr,
        earliest_block,
        blockchain
    )
%}



WITH
    user_orders AS (
        SELECT 
            ab.*,
            if(ab.order_quantities_kind = 'Exact', ab.order_quantities_exact_quantity, ab.order_quantities_partial_filled_quantity) AS fill_amount,
            if(ab.zero_for_one, asts.asset_in, asts.asset_out) AS asset_in,
            if(ab.zero_for_one, asts.asset_out, asts.asset_in) AS asset_out,
            asts.price_1over0
        FROM ({{angstrom_decoding_user_orders(angstrom_contract_addr, earliest_block, blockchain)}}) AS ab
        INNER JOIN ({{ angstrom_bundle_indexes_to_assets(angstrom_contract_addr, earliest_block, blockchain) }}) AS asts
            ON asts.bundle_pair_index = ab.pair_index AND ab.block_number = asts.block_number AND ab.tx_hash = asts.tx_hash
    ),
    user_orders_with_pool AS (
        SELECT
            u.*,
            f.pool_id AS pool_id,
            f.bundle_fee AS bundle_fee
        FROM user_orders AS u
        INNER JOIN ({{ angstrom_pool_info(controller_v1_contract_addr, earliest_block, blockchain) }}) AS f
            ON u.block_number = f.block_number AND
                ((varbinary_substring(f.topic1, 13, 20) = u.asset_in OR varbinary_substring(f.topic2, 13, 20) = u.asset_in) AND 
                (varbinary_substring(f.topic1, 13, 20) = u.asset_out OR varbinary_substring(f.topic2, 13, 20) = u.asset_out)) 
    ),
    user_orders_with_priced_assets AS (
        SELECT 
            u.*,
            if(u.zero_for_one, a.t1_amount, a.t0_amount) AS token_sold_amt,
            if(u.zero_for_one, a.t0_amount, a.t1_amount) AS token_bought_amt
        FROM user_orders_with_pool AS u
        CROSS JOIN LATERAL ({{ angstrom_user_order_fill_amount('u.zero_for_one', 'u.exact_in', 'u.fill_amount', 'u.extra_fee_asset0', 'u.bundle_fee', 'u.price_1over0') }}) AS a    
    )
SELECT
    *
FROM user_orders_with_priced_assets





{% endmacro %}


