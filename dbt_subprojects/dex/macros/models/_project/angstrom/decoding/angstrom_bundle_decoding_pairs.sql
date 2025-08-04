{% macro
    angstrom_decoding_pairs(raw_tx_input_hex)
%}


WITH vec_pade AS (
    SELECT buf
    FROM ({{ angstrom_decoding_recursive(raw_tx_input_hex, 'step1') }})
)
SELECT 
    bundle_idx,
    index0, 
    index1, 
    store_index, 
    price_1over0
FROM (
    WITH RECURSIVE decode_pair (buf, len, idx, index0, index1, store_index, price_1over0) AS (
        SELECT
            varbinary_substring(buf, 4, varbinary_length(buf) - 3),
            varbinary_to_integer(varbinary_substring(buf, 1, 3)) / 38 AS len,
            0 AS idx,
            0 AS index0,
            0 AS index1,
            0 AS store_index,
            CAST(0 AS uint256) AS price_1over0
        FROM
            vec_pade

        UNION ALL

        SELECT
            varbinary_substring(buf, 39, varbinary_length(buf) - 38) AS enc,
            len,
            idx + 1 AS new_idx,
            varbinary_to_integer(varbinary_substring(buf, 1, 2)) AS index0,
            varbinary_to_integer(varbinary_substring(buf, 3, 2)) AS index1,
            varbinary_to_integer(varbinary_substring(buf, 5, 2)) AS store_index,
            varbinary_to_uint256(varbinary_substring(buf, 7, 32)) AS price_1over0
        FROM
            decode_pair
        WHERE
            idx < len
    )
    SELECT
        idx AS bundle_idx,
        index0,
        index1,
        store_index,
        price_1over0
    FROM
        decode_pair
    WHERE
        idx > 0
)



{% endmacro %}