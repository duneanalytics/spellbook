{% macro
    angstrom_bundle_indexes_to_assets(raw_tx_input_hex, pair_index, zfo)
%}

-- TODO: test this

WITH
    assets AS (
        SELECT *
        FROM ({{angstrom_decoding_assets(raw_tx_input_hex)}})
    ),
    pairs AS (
        SELECT 
            index0,
            index1,
            price_1over0
        FROM ({{angstrom_decoding_pairs(raw_tx_input_hex)}})
        WHERE bundle_idx = pair_index
    ),
    _asset_in AS (
        SELECT
            price_1over0,
            asset_in,
        FROM assets AS a
        CROSS JOIN pairs AS p
        WHERE a.bundle_idx = p.index0
    ),
    _asset_out AS (
        SELECT
            asset_out,
        FROM assets AS a
        CROSS JOIN pairs AS p
        WHERE a.bundle_idx = p.index1
    ),
    zfo_assets AS (
        SELECT
            price_1over0,
            if(zfo, [asset_in, asset_out], [asset_out, asset_in]) AS zfo_sorted_assets
        FROM _asset_in i 
        CROSS JOIN _asset_out o
    )
SELECT
    price_1over0,
    zfo_sorted_assets[1] AS asset_in,
    zfo_sorted_assets[2] AS asset_out
FROM zfo_assets



{% endmacro %}

