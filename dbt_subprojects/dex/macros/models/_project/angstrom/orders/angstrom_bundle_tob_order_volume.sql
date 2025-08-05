{% macro
    angstrom_bundle_tob_order_volume(
        angstrom_contract_addr,
        blockchain
    )
%}



SELECT 
    ab.*,
    if(ab.zero_for_1, asts.asset_in, asts.asset_out) AS asset_in,
    if(ab.zero_for_1, asts.asset_out, asts.asset_in) AS asset_out,
    asts.price_1over0
FROM ({{angstrom_decoding_top_of_block_orders(angstrom_contract_addr, blockchain)}}) AS ab
CROSS JOIN LATERAL ({{ angstrom_bundle_indexes_to_assets(angstrom_contract_addr, blockchain) }}) AS asts
WHERE asts.bundle_pair_index = ab.pairs_index AND ab.block_number = asts.block_number AND ab.tx_hash = asts.tx_hash

{% endmacro %}
