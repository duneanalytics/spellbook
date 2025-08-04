{% macro
    angstrom_bundle_tob_order_volume(raw_tx_input_hex)
%}



SELECT 
    ab.*,
    asts.*
FROM ({{angstrom_decoding_top_of_block_orders(raw_tx_input_hex)}}) AS ab
CROSS JOIN LATERAL ({{ angstrom_bundle_indexes_to_assets(raw_tx_input_hex, 'ab.pairs_index', 'ab.zero_for_1') }}) AS asts

{% endmacro %}
