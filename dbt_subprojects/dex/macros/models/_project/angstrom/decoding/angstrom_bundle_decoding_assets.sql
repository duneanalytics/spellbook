{% macro
    angstrom_decoding_assets(raw_tx_input_hex)
%}


WITH vec_pade AS (
    SELECT buf
    FROM ({{ angstrom_decoding_recursive(raw_tx_input_hex, 'step0') }})
)
SELECT 
    bundle_idx,
    token_address,
    save_amount,
    take_amount,
    settle_amount
FROM (
    WITH RECURSIVE decode_asset (buf, len, idx, addr, save, take, settle) AS (
        SELECT
            varbinary_substring(buf, 4, varbinary_length(buf) - 3),
            varbinary_to_integer(varbinary_substring(buf, 1, 3)) / 68 AS len,
            0 AS idx,
            CAST(NULL AS varbinary) AS addr,
            CAST(0 AS uint256) AS save,
            CAST(0 AS uint256) AS take,
            CAST(0 AS uint256) AS settle
        FROM
            vec_pade

        UNION ALL

        SELECT
            varbinary_substring(buf, 69, varbinary_length(buf) - 68) AS enc,
            len,
            idx + 1 AS new_idx,
            varbinary_substring(buf, 1, 20) AS addr,
            varbinary_to_uint256(varbinary_substring(buf, 21, 16)) AS save,
            varbinary_to_uint256(varbinary_substring(buf, 37, 16)) AS take,
            varbinary_to_uint256(varbinary_substring(buf, 53, 16)) AS settle
        FROM
            decode_asset
        WHERE
            idx < len
    )
    SELECT
        idx AS bundle_idx,
        addr AS token_address,
        save AS save_amount,
        take AS take_amount,
        settle AS settle_amount
    FROM
        decode_asset
    WHERE
        idx > 0
)


{% endmacro %}