{% macro
    angstrom_decoding_pool_updates(raw_tx_input_hex)
%}


WITH vec_pade AS (
    SELECT buf
    FROM ({{ angstrom_decoding_recursive(raw_tx_input_hex, step2) }})
)
SELECT 
    idx AS bundle_idx,
    zero_for_one,
    pair_index,
    swap_in_quantity,
    kind,
    start_tick,
    start_liquidity,
    quantities,
    reward_checksum,
    amount,
    expected_liquidity
FROM (
    WITH RECURSIVE decode_pool_update (
        buf,
        len,
        idx,
        zero_for_one,
        pair_index,
        swap_in_quantity,
        kind,
        start_tick,
        start_liquidity,
        quantities,
        reward_checksum,
        amount,
        expected_liquidity
    ) AS (
        SELECT
            varbinary_substring(buf, 4, varbinary_length(buf) - 3),
            varbinary_to_bigint(varbinary_substring(buf, 1, 3)),
            0,
            CAST(NULL AS boolean),
            CAST(0 AS bigint),
            CAST(0 AS uint256),
            CAST(NULL AS varchar),
            CAST(NULL AS bigint),
            CAST(NULL AS uint256),
            CAST(ARRAY[] AS array(uint256)),
            CAST(NULL AS uint256),
            CAST(NULL AS uint256),
            CAST(NULL AS uint256)
        FROM vec_pade

        UNION ALL

        SELECT
            final_parse[1],
            final_parse[2],
            final_parse[3],
            final_parse[4],
            final_parse[5],
            final_parse[6],
            final_parse[7],
            final_parse[8],
            final_parse[9],
            final_parse[10],
            final_parse[11],
            final_parse[12],
            final_parse[13]
        FROM (
            SELECT
                CASE kind_id
                    WHEN 0 THEN ROW(
                        next_buf,
                        varbinary_length(next_buf),
                        idx,
                        zero_for_one,
                        pair_index,
                        swap_in_quantity,
                        kind,
                        varbinary_to_bigint(varbinary_substring(buf, 20, 3)),
                        varbinary_to_uint256(varbinary_substring(buf, 23, 16)),
                        IF(arr_len != 0,
                            transform(sequence(0, (arr_len / 16) - 1), n -> varbinary_to_uint256(varbinary_substring(buf, 42 + n * 16, 16))),
                            ARRAY[]
                        ),
                        varbinary_to_uint256(varbinary_substring(buf, 42 + arr_len, 20)),
                        NULL,
                        NULL
                    )
                    WHEN 1 THEN ROW(
                        next_buf,
                        varbinary_length(next_buf),
                        idx,
                        zero_for_one,
                        pair_index,
                        swap_in_quantity,
                        kind,
                        varbinary_to_bigint(varbinary_substring(buf, 20, 3)),
                        varbinary_to_uint256(varbinary_substring(buf, 23, 16)),
                        IF(arr_len != 0,
                            transform(sequence(0, (arr_len / 16) - 1), n -> varbinary_to_uint256(varbinary_substring(buf, 42 + n * 16, 16))),
                            ARRAY[]
                        ),
                        varbinary_to_uint256(varbinary_substring(buf, 42 + arr_len, 20)),
                        NULL,
                        NULL
                    )
                    WHEN 2 THEN ROW(
                        next_buf,
                        varbinary_length(next_buf),
                        idx,
                        zero_for_one,
                        pair_index,
                        swap_in_quantity,
                        kind,
                        NULL,
                        NULL,
                        ARRAY[],
                        NULL,
                        varbinary_to_uint256(varbinary_substring(buf, 20, 16)),
                        varbinary_to_uint256(varbinary_substring(buf, 36, 16))
                    )
                    ELSE ROW(
                        next_buf,
                        varbinary_length(next_buf),
                        idx,
                        zero_for_one,
                        pair_index,
                        swap_in_quantity,
                        kind,
                        NULL,
                        NULL,
                        ARRAY[],
                        NULL,
                        varbinary_to_uint256(varbinary_substring(buf, 20, 16)),
                        varbinary_to_uint256(varbinary_substring(buf, 36, 16))
                    )
                END AS final_parse
            FROM (
                WITH decode_simple AS (
                    SELECT
                        CASE varbinary_to_integer(varbinary_substring(buf, 1, 1))
                            WHEN 0 THEN (
                                0,
                                buf,
                                varbinary_substring(buf, 62 + varbinary_to_bigint(varbinary_substring(buf, 39, 3))),
                                varbinary_to_bigint(varbinary_substring(buf, 39, 3)),
                                idx + 1,
                                false,
                                varbinary_to_bigint(varbinary_substring(buf, 2, 2)),
                                varbinary_to_uint256(varbinary_substring(buf, 4, 16)),
                                'MultiTick'
                            )
                            WHEN 1 THEN (
                                1,
                                buf,
                                varbinary_substring(buf, 62 + varbinary_to_bigint(varbinary_substring(buf, 39, 3))),
                                varbinary_to_bigint(varbinary_substring(buf, 39, 3)),
                                idx + 1,
                                true,
                                varbinary_to_bigint(varbinary_substring(buf, 2, 2)),
                                varbinary_to_uint256(varbinary_substring(buf, 4, 16)),
                                'MultiTick'
                            )
                            WHEN 2 THEN (
                                2,
                                buf,
                                varbinary_substring(buf, 52),
                                0,
                                idx + 1,
                                false,
                                varbinary_to_bigint(varbinary_substring(buf, 2, 2)),
                                varbinary_to_uint256(varbinary_substring(buf, 4, 16)),
                                'CurrentOnly'
                            )
                            ELSE (
                                3,
                                buf,
                                varbinary_substring(buf, 52),
                                0,
                                idx + 1,
                                true,
                                varbinary_to_bigint(varbinary_substring(buf, 2, 2)),
                                varbinary_to_uint256(varbinary_substring(buf, 4, 16)),
                                'CurrentOnly'
                            )
                        END AS inner_decoded
                    FROM decode_pool_update
                    WHERE idx < len
                )
                SELECT
                    inner_decoded[1] AS kind_id,
                    inner_decoded[2] AS buf,
                    inner_decoded[3] AS next_buf,
                    inner_decoded[4] AS arr_len,
                    inner_decoded[5] AS idx,
                    inner_decoded[6] AS zero_for_one,
                    inner_decoded[7] AS pair_index,
                    inner_decoded[8] AS swap_in_quantity,
                    inner_decoded[9] AS kind
                FROM decode_simple
            )
        )
    )
    SELECT *
    FROM decode_pool_update
    WHERE idx > 0
)
ORDER BY idx DESC;


{% endmacro %}