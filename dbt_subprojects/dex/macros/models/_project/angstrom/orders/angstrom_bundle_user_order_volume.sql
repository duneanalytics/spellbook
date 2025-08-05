{% macro
    angstrom_bundle_user_order_volume(        
        angstrom_contract_addr, 
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
        FROM ({{angstrom_decoding_user_orders(angstrom_contract_addr, blockchain)}}) AS ab
        CROSS JOIN LATERAL ({{ angstrom_bundle_indexes_to_assets(angstrom_contract_addr, blockchain) }}) AS asts
        WHERE asts.bundle_pair_index = ab.pair_index AND ab.block_number = asts.block_number AND ab.tx_hash = asts.tx_hash
    ),
    orders_with_assets AS (
        SELECT
            u.*,
            a.*
        FROM user_orders AS u
        CROSS JOIN LATERAL ({{ angstrom_pool_fees(angstrom_contract_addr, blockchain) }}) AS f
        CROSS JOIN LATERAL ({{ angstrom_user_order_fill_amount('u.zero_for_one', 'u.exact_in', 'u.fill_amount', 'u.extra_fee_asset0', 'f.bundle_fee', 'u.price_1over0') }}) AS a
        WHERE     
            ((varbinary_substring(f.topic1, 13, 20) = u.asset_in OR varbinary_substring(f.topic2, 13, 20) = u.asset_out) AND 
            (varbinary_substring(f.topic1, 13, 20) = u.asset_out OR varbinary_substring(f.topic2, 13, 20) = u.asset_in))
    )
SELECT
    *
FROM orders_with_assets





{% endmacro %}
