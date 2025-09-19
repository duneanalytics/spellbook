{% macro
    angstrom_bundle_tob_fees_donated(        
        angstrom_contract_addr, 
        controller_v1_contract_addr,
        earliest_block,
        blockchain,
        controller_pool_configured_log_topic0
    )
%}


WITH
    user_order_fees_by_pool AS (
        SELECT 
            tx_hash,
            pair_index,
            SUM(lp_fees_paid_asset_in + lp_fees_paid_asset_out) AS total_fees_paid_t0
        FROM ({{ angstrom_bundle_user_order_volume(angstrom_contract_addr, controller_v1_contract_addr, earliest_block, blockchain, controller_pool_configured_log_topic0) }})
        GROUP BY tx_hash, pair_index
    ),
    donated_amounts_by_pool_all AS (
        SELECT
            tx_hash,
            pair_index,
            if(kind = 'MultiTick', reduce(quantities, 0, (s, x) -> s + x, s -> s), amount) AS amt_donated
        FROM ({{ angstrom_decoding_pool_updates(angstrom_contract_addr, earliest_block, blockchain) }})
    )
SELECT
    d.tx_hash AS tx_hash,
    d.pair_index AS pair_index,
    d.amt_donated,
    d.amt_donated - coalesce(u.total_fees_paid_t0, 0) AS tob_donated_amount_t0
FROM donated_amounts_by_pool_all AS d 
LEFT JOIN user_order_fees_by_pool AS u
    ON u.tx_hash = d.tx_hash AND u.pair_index = d.pair_index


{% endmacro %}