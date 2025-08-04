{% macro
    angstrom_bundle_user_order_volume(raw_tx_input_hex, fetched_bn)
%}



WITH
    user_orders AS (
        SELECT 
            ab.*,
            if(ab.order_quantities_kind = 'Exact', ab.order_quantities_exact_quantity, ab.order_quantities_partial_filled_quantity) AS fill_amount,
            asts.*
        FROM ({{angstrom_decoding_user_orders(raw_tx_input_hex)}}) AS ab
        CROSS JOIN ({{ angstrom_bundle_indexes_to_assets(raw_tx_input_hex, 'ab.pair_index', 'ab.zfo') }}) AS asts
    ),
    orders_with_assets AS (
        SELECT
            u.*,
            a.*
        FROM user_orders AS u
        CROSS JOIN ({{ angstrom_pool_fees(fetched_bn) }}) AS f
        CROSS JOIN ({{ angstrom_user_order_fill_amount('!u.zero_for_one', 'u.exact_in', 'u.fill_amount', 'u.extra_fee_asset0', 'f.bundle_fee', 'u.price_1over0') }}) AS a
    )
SELECT
    *
FROM orders_with_assets





{% endmacro %}
