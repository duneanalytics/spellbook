{% macro
    angstrom_bundle_user_order_volume(raw_tx_input_hex)
%}



WITH
    user_orders AS (
        SELECT 
            ab.*,
            if(ab.order_quantities_kind = 'Exact', ab.order_quantities_exact_quantity, ab.order_quantities_partial_filled_quantity) AS fill_amount,
            asts.*
        FROM ({{angstrom_bundle_user_order_volume(raw_tx_input_hex)}}) AS ab
        CROSS JOIN ({{ angstrom_bundle_indexes_to_assets(raw_tx_input_hex, 'ab.pair_index', 'ab.zfo') }}) AS asts
    ),
    orders_with_assets AS (
        SELECT
            u.*,
            f.*
        FROM user_orders AS u
        CROSS JOIN ({{ angstrom_user_order_fill_amount('!u.zero_for_one', 'u.exact_in', 'u.fill_amount', 'u.extra_fee_asset0', 0, 'u.price_1over0') }}) AS f -- TODO: get pool fee
    )
SELECT
    -- TODO: generalize query, logic is all in the macros tho, so shouldn't take long
    *
FROM orders_with_assets





{% endmacro %}
