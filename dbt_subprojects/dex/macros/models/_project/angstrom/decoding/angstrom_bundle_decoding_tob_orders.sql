{% macro
    angstrom_decoding_top_of_block_orders(raw_tx_input_hex)
%}


WITH vec_pade AS (
    SELECT buf
    FROM ({{ angstrom_decoding_recursive(raw_tx_input_hex, 'step3') }})
)
SELECT
    use_internal,
    quantity_in,
    quantity_out,
    max_gas_asset_0,
    gas_used_asset_0,
    pairs_index,
    zero_for_1,
    recipient,
    signature_kind,
    signature_ecdsa_v,
    signature_ecdsa_r,
    signature_ecdsa_s,
    signature_contract_from,
    signature_contract_signature
FROM (
    WITH RECURSIVE decode_tob_order (
        buf,
        pointer,
        idx,
        use_internal,
        quantity_in,
        quantity_out,
        max_gas_asset_0,
        gas_used_asset_0,
        pairs_index,
        zero_for_1,
        recipient,
        signature_kind,
        signature_ecdsa_v,
        signature_ecdsa_r,
        signature_ecdsa_s,
        signature_contract_from,
        signature_contract_signature
    ) AS (
        SELECT
            varbinary_substring(buf, 4, varbinary_length(buf) - 3),
            4,
            0,
            CAST(NULL AS boolean),
            CAST(NULL AS uint256),
            CAST(NULL AS uint256),
            CAST(NULL AS uint256),
            CAST(NULL AS uint256),
            CAST(NULL AS bigint),
            CAST(NULL AS boolean),
            CAST(NULL AS varbinary),
            CAST(NULL AS varchar),
            CAST(NULL AS bigint),
            CAST(NULL AS varbinary),
            CAST(NULL AS varbinary),
            CAST(NULL AS varbinary),
            CAST(NULL AS varbinary)
        FROM vec_pade

        UNION ALL

        SELECT
            buf,
            pointer,
            idx,
            use_internal,
            quantity_in,
            quantity_out,
            max_gas_asset_0,
            gas_used_asset_0,
            pairs_index,
            zero_for_1,
            recipient,
            signature_kind,
            signature_ecdsa_v,
            signature_ecdsa_r,
            signature_ecdsa_s,
            signature_contract_from,
            signature_contract_signature
        FROM (
            WITH 
            trimmed_as_fields AS (
                SELECT
                    idx + 1 AS idx,
                    ARRAY[
                        bitwise_and(bitwise_right_shift(varbinary_to_integer(varbinary_substring(buf, 1, 1)), 3), 1),
                        bitwise_and(bitwise_right_shift(varbinary_to_integer(varbinary_substring(buf, 1, 1)), 2), 1),
                        bitwise_and(bitwise_right_shift(varbinary_to_integer(varbinary_substring(buf, 1, 1)), 1), 1),
                        bitwise_and(varbinary_to_integer(varbinary_substring(buf, 1, 1)), 1)
                    ] AS bitmap,
                    buf,
                    2 AS pointer
                FROM decode_tob_order
            ),
            use_internal_field AS (
                SELECT
                    idx,
                    if(bitmap[4] = 1, true, false) AS use_internal,
                    bitmap,
                    pointer,
                    buf
                FROM trimmed_as_fields
            ),
            quantity_in_field AS (
                SELECT
                    idx,
                    use_internal,
                    varbinary_to_uint256(varbinary_substring(buf, pointer, 16)) AS quantity_in,
                    bitmap,
                    pointer + 16 AS pointer,
                    buf
                FROM use_internal_field
            ),
            quantity_out_field AS (
                SELECT
                    idx,
                    use_internal,
                    quantity_in,
                    varbinary_to_uint256(varbinary_substring(buf, pointer, 16)) AS quantity_out,
                    bitmap,
                    pointer + 16 AS pointer,
                    buf
                FROM quantity_in_field
            ),
            max_gas_asset_0_field AS (
                SELECT
                    idx,
                    use_internal,
                    quantity_in,
                    quantity_out,
                    varbinary_to_uint256(varbinary_substring(buf, pointer, 16)) AS max_gas_asset_0,
                    bitmap,
                    pointer + 16 AS pointer,
                    buf
                FROM quantity_out_field
            ),
            gas_used_asset_0_field AS (
                SELECT
                    idx,
                    use_internal,
                    quantity_in,
                    quantity_out,
                    max_gas_asset_0,
                    varbinary_to_uint256(varbinary_substring(buf, pointer, 16)) AS gas_used_asset_0,
                    bitmap,
                    pointer + 16 AS pointer,
                    buf
                FROM max_gas_asset_0_field
            ),
            pairs_index_field AS (
                SELECT
                    idx,
                    use_internal,
                    quantity_in,
                    quantity_out,
                    max_gas_asset_0,
                    gas_used_asset_0,
                    varbinary_to_bigint(varbinary_substring(buf, pointer, 2)) AS pairs_index,
                    bitmap,
                    pointer + 2 AS pointer,
                    buf
                FROM gas_used_asset_0_field
            ),
            zero_for_1_field AS (
                SELECT
                    idx,
                    use_internal,
                    quantity_in,
                    quantity_out,
                    max_gas_asset_0,
                    gas_used_asset_0,
                    pairs_index,
                    if(bitmap[3] = 1, true, false) AS zero_for_1,
                    bitmap,
                    pointer,
                    buf
                FROM pairs_index_field
            ),
            recipient_field AS (
                SELECT
                    idx,
                    use_internal,
                    quantity_in,
                    quantity_out,
                    max_gas_asset_0,
                    gas_used_asset_0,
                    pairs_index,
                    zero_for_1,
                    if(bitmap[2] = 1, varbinary_substring(buf, pointer, 20), NULL) AS recipient,
                    bitmap,
                    if(bitmap[2] = 1, pointer + 20, pointer) AS pointer,
                    buf
                FROM zero_for_1_field
            ),
            signature_field AS (
                SELECT
                    idx,
                    use_internal,
                    quantity_in,
                    quantity_out,
                    max_gas_asset_0,
                    gas_used_asset_0,
                    pairs_index,
                    zero_for_1,
                    recipient,
                    if(bitmap[1] = 1, 'Ecdsa', 'Contract') AS signature_kind,
                    if(bitmap[1] = 1, varbinary_to_bigint(varbinary_substring(buf, pointer, 1)), NULL) AS signature_ecdsa_v,
                    if(bitmap[1] = 1, varbinary_substring(buf, pointer + 1, 32), NULL) AS signature_ecdsa_r,
                    if(bitmap[1] = 1, varbinary_substring(buf, pointer + 33, 32), NULL) AS signature_ecdsa_s,
                    if(bitmap[1] = 0, varbinary_substring(buf, pointer, 20), NULL) AS signature_contract_from,
                    if(bitmap[1] = 0, varbinary_substring(buf, pointer + 23, varbinary_to_integer(varbinary_substring(buf, pointer + 20, 3))), NULL) AS signature_contract_signature,
                    bitmap,
                    if(bitmap[1] = 1, pointer + 65, pointer + 23 + varbinary_to_integer(varbinary_substring(buf, pointer + 20, 3))) AS pointer,
                    buf
                FROM recipient_field
            ),
            all_fields_collapsed AS (
                SELECT 
                    buf,
                    pointer,
                    idx,
                    use_internal,
                    quantity_in,
                    quantity_out,
                    max_gas_asset_0,
                    gas_used_asset_0,
                    pairs_index,
                    zero_for_1,
                    recipient,
                    signature_kind,
                    signature_ecdsa_v,
                    signature_ecdsa_r,
                    signature_ecdsa_s,
                    signature_contract_from,
                    signature_contract_signature
                FROM signature_field
            )
            SELECT 
                varbinary_substring(buf, pointer) AS buf,
                pointer,
                idx,
                use_internal,
                quantity_in,
                quantity_out,
                max_gas_asset_0,
                gas_used_asset_0,
                pairs_index,
                zero_for_1,
                recipient,
                signature_kind,
                signature_ecdsa_v,
                signature_ecdsa_r,
                signature_ecdsa_s,
                signature_contract_from,
                signature_contract_signature
            FROM all_fields_collapsed
            WHERE varbinary_length(buf) != 0
        )
    )
    
    SELECT *
    FROM decode_tob_order
    WHERE idx > 0
)
ORDER BY idx DESC


{% endmacro %}