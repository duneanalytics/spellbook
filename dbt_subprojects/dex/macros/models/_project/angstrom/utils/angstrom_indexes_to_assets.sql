{% macro
    angstrom_bundle_indexes_to_assets(
        angstrom_contract_addr, 
        blockchain
    )
%}

WITH
    assets AS (
        SELECT  
            tx_hash,
            block_number,
            bundle_idx - 1 AS bundle_idx,
            token_address
        FROM ({{angstrom_decoding_assets(angstrom_contract_addr, blockchain)}})
    ),
    pairs AS (
        SELECT 
            tx_hash,
            block_number,
            bundle_idx - 1 AS bundle_idx, 
            index0,
            index1,
            price_1over0
        FROM ({{angstrom_decoding_pairs(angstrom_contract_addr, blockchain)}})
    ),
    _asset_in AS (
        SELECT
            p.block_number AS block_number,
            p.tx_hash AS tx_hash,
            p.bundle_idx AS p_index,
            p.price_1over0 AS price_1over0,
            a.token_address AS asset_in
        FROM assets AS a
        JOIN pairs AS p ON a.bundle_idx = p.index0 AND a.block_number = p.block_number AND a.tx_hash = p.tx_hash
    ),
    _asset_out AS (
        SELECT
            p.block_number AS block_number,
            p.tx_hash AS tx_hash,
            p.bundle_idx AS p_index,
            a.token_address AS asset_out
        FROM assets AS a
        JOIN pairs AS p ON a.bundle_idx = p.index1 AND a.block_number = p.block_number AND a.tx_hash = p.tx_hash
    ),
    zfo_assets AS (
        SELECT
            i.block_number AS block_number,
            i.tx_hash AS tx_hash,
            i.price_1over0 AS price_1over0,
            i.p_index AS bundle_pair_index,
            i.asset_in AS asset_in,
            o.asset_out AS asset_out
        FROM _asset_in i 
        CROSS JOIN _asset_out o
        WHERE i.p_index = o.p_index AND i.block_number = o.block_number AND i.tx_hash = o.tx_hash
    )
SELECT
    *
FROM zfo_assets




{% endmacro %}
