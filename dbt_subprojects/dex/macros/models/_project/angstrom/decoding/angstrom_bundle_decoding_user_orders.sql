{% macro
    angstrom_decoding_user_orders(
        angstrom_contract_addr,
        earliest_block,
        blockchain
    )
%}


WITH vec_pade AS (
    SELECT 
        tx_hash,
        block_number,
        buf
    FROM ({{ angstrom_decoding_recursive(angstrom_contract_addr, earliest_block, blockchain, 'step4') }})
)
SELECT
    tx_hash,
    block_number,
    ref_id,
    use_internal,
    pair_index,
    min_price,
    if((recipient IS NULL OR recipient = 0x0000000000000000000000000000000000000000) AND signature_contract_from IS NOT NULL, signature_contract_from, recipient) AS recipient,
    hook_data,
    zero_for_one,
    standing_validation_nonce,
    standing_validation_deadline,
    order_quantities_kind,
    order_quantities_partial_min_quantity_in,
    order_quantities_partial_max_quantity_in,
    order_quantities_partial_filled_quantity,
    order_quantities_exact_quantity,
    max_extra_fee_asset0,
    extra_fee_asset0,
    exact_in,
    signature_kind,
    signature_ecdsa_v,
    signature_ecdsa_r,
    signature_ecdsa_s,
    signature_contract_from,
    signature_contract_signature
FROM (
    WITH RECURSIVE decode_user_order (
        tx_hash,
        block_number,
        buf,
        pointer,
        idx,
        ref_id,
        use_internal,
        pair_index,
        min_price,
        recipient,
        hook_data,
        zero_for_one,
        standing_validation_nonce,
        standing_validation_deadline,
        order_quantities_kind,
        order_quantities_partial_min_quantity_in,
        order_quantities_partial_max_quantity_in,
        order_quantities_partial_filled_quantity,
        order_quantities_exact_quantity,
        max_extra_fee_asset0,
        extra_fee_asset0,
        exact_in,
        signature_kind,
        signature_ecdsa_v,
        signature_ecdsa_r,
        signature_ecdsa_s,
        signature_contract_from,
        signature_contract_signature
    ) AS (
        SELECT
            tx_hash,
            block_number,
            varbinary_substring(buf, 4, varbinary_length(buf) - 3),
            4,
            0,
            CAST(0 AS bigint),
            CAST(NULL AS boolean),
            CAST(0 AS bigint),
            CAST(0 AS uint256),
            CAST(NULL AS varbinary),
            CAST(NULL AS varbinary),
            CAST(NULL AS boolean),
            CAST(NULL AS bigint),
            CAST(NULL AS bigint),
            CAST(NULL AS varchar),
            CAST(NULL AS uint256),
            CAST(NULL AS uint256),
            CAST(NULL AS uint256),
            CAST(NULL AS uint256),
            CAST(0 AS uint256),
            CAST(0 AS uint256),
            CAST(NULL AS boolean),
            CAST(NULL AS varchar),
            CAST(NULL AS bigint),
            CAST(NULL AS varbinary),
            CAST(NULL AS varbinary),
            CAST(NULL AS varbinary),
            CAST(NULL AS varbinary)
        FROM vec_pade

        UNION ALL

        SELECT
            tx_hash,
            block_number,
            buf,
            pointer,
            idx,
            ref_id,
            use_internal,
            pair_index,
            min_price,
            recipient,
            hook_data,
            zero_for_one,
            standing_validation_nonce,
            standing_validation_deadline,
            order_quantities_kind,
            order_quantities_partial_min_quantity_in,
            order_quantities_partial_max_quantity_in,
            order_quantities_partial_filled_quantity,
            order_quantities_exact_quantity,
            max_extra_fee_asset0,
            extra_fee_asset0,
            exact_in,
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
                    tx_hash,
                    block_number,
                    idx + 1 AS idx,
                    ARRAY[
                        bitwise_and(bitwise_right_shift(varbinary_to_integer(varbinary_substring(buf, 1, 1)), 7), 1),
                        bitwise_and(bitwise_right_shift(varbinary_to_integer(varbinary_substring(buf, 1, 1)), 6), 1),
                        bitwise_and(bitwise_right_shift(varbinary_to_integer(varbinary_substring(buf, 1, 1)), 5), 1),
                        bitwise_and(bitwise_right_shift(varbinary_to_integer(varbinary_substring(buf, 1, 1)), 4), 1),
                        bitwise_and(bitwise_right_shift(varbinary_to_integer(varbinary_substring(buf, 1, 1)), 3), 1),
                        bitwise_and(bitwise_right_shift(varbinary_to_integer(varbinary_substring(buf, 1, 1)), 2), 1),
                        bitwise_and(bitwise_right_shift(varbinary_to_integer(varbinary_substring(buf, 1, 1)), 1), 1),
                        bitwise_and(varbinary_to_integer(varbinary_substring(buf, 1, 1)), 1)
                    ] AS bitmap,
                    buf,
                    2 AS pointer
                FROM decode_user_order
            ),
            -- ref id
            ref_id_field AS (
                SELECT
                    tx_hash,
                    block_number,
                    idx,
                    varbinary_to_bigint(varbinary_substring(buf, pointer, 4)) AS ref_id,
                    bitmap,
                    pointer + 4 AS pointer,
                    buf
                FROM trimmed_as_fields
            ),
            use_internal_field AS (
                SELECT
                    tx_hash,
                    block_number,
                    idx,
                    ref_id,
                    if(bitmap[8] = 1, true, false) AS use_internal,
                    bitmap,
                    pointer,
                    buf
                FROM ref_id_field
            ),
            pair_index_field AS (
                SELECT
                    tx_hash,
                    block_number,
                    idx,
                    ref_id,
                    use_internal,
                    varbinary_to_bigint(varbinary_substring(buf, pointer, 2)) AS pair_index,
                    bitmap,
                    pointer + 2 AS pointer,
                    buf
                FROM use_internal_field
            ),
            min_price_field AS (
                SELECT
                    tx_hash,
                    block_number,
                    idx,
                    ref_id,
                    use_internal,
                    pair_index,
                    varbinary_to_uint256(varbinary_substring(buf, pointer, 32)) AS min_price,
                    bitmap,
                    pointer + 32 AS pointer,
                    buf
                FROM pair_index_field
            ),
            recipient_field AS (
                SELECT
                    tx_hash,
                    block_number,
                    idx,
                    ref_id,
                    use_internal,
                    pair_index,
                    min_price,
                    if(bitmap[7] = 1, varbinary_substring(buf, pointer, 20), NULL) AS recipient,
                    bitmap,
                    if(bitmap[7] = 1, pointer + 20, pointer) AS pointer,
                    buf
                FROM min_price_field
            ),
            hook_data_field AS (
                SELECT
                    tx_hash,
                    block_number,
                    idx,
                    ref_id,
                    use_internal,
                    pair_index,
                    min_price,
                    recipient,
                    if(bitmap[6] = 1, varbinary_substring(buf, pointer + 3, varbinary_to_integer(varbinary_substring(buf, pointer, 3))), NULL) AS hook_data,
                    bitmap,
                    if(bitmap[6] = 1, pointer + 3 + varbinary_to_integer(varbinary_substring(buf, pointer, 3)), pointer) AS pointer,
                    buf
                FROM recipient_field
            ),
            zero_for_one_field AS (
                SELECT
                    tx_hash,
                    block_number,
                    idx,
                    ref_id,
                    use_internal,
                    pair_index,
                    min_price,
                    recipient,
                    hook_data,
                    if(bitmap[5] = 1, true, false) AS zero_for_one,
                    bitmap,
                    pointer,
                    buf
                FROM hook_data_field
            ),
            standing_validation_field AS (
                SELECT
                    tx_hash,
                    block_number,
                    idx,
                    ref_id,
                    use_internal,
                    pair_index,
                    min_price,
                    recipient,
                    hook_data,
                    zero_for_one,
                    if(bitmap[4] = 1, varbinary_to_bigint(varbinary_substring(buf, pointer, 8)), NULL) AS standing_validation_nonce,
                    if(bitmap[4] = 1, varbinary_to_bigint(varbinary_substring(buf, pointer + 8, 5)), NULL) AS standing_validation_deadline,
                    bitmap,
                    if(bitmap[4] = 1, pointer + 13, pointer) AS pointer,
                    buf
                FROM zero_for_one_field
            ),
            order_quantities_field AS (
                SELECT
                    tx_hash,
                    block_number,
                    idx,
                    ref_id,
                    use_internal,
                    pair_index,
                    min_price,
                    recipient,
                    hook_data,
                    zero_for_one,
                    standing_validation_nonce,
                    standing_validation_deadline,
                    if(bitmap[3] = 1, 'Partial', 'Exact') AS order_quantities_kind,
                    if(bitmap[3] = 1, varbinary_to_uint256(varbinary_substring(buf, pointer, 16)), NULL) AS order_quantities_partial_min_quantity_in,
                    if(bitmap[3] = 1, varbinary_to_uint256(varbinary_substring(buf, pointer + 16, 16)), NULL) AS order_quantities_partial_max_quantity_in,
                    if(bitmap[3] = 1, varbinary_to_uint256(varbinary_substring(buf, pointer + 32, 16)), NULL) AS order_quantities_partial_filled_quantity,
                    if(bitmap[3] = 0, varbinary_to_uint256(varbinary_substring(buf, pointer, 16)), NULL) AS order_quantities_exact_quantity,
                    bitmap,
                    if(bitmap[3] = 1, pointer + 48, pointer + 16) AS pointer,
                    buf
                FROM standing_validation_field
            ),
            max_extra_fee_asset0_field AS (
                SELECT
                    tx_hash,
                    block_number,
                    idx,
                    ref_id,
                    use_internal,
                    pair_index,
                    min_price,
                    recipient,
                    hook_data,
                    zero_for_one,
                    standing_validation_nonce,
                    standing_validation_deadline,
                    order_quantities_kind,
                    order_quantities_partial_min_quantity_in,
                    order_quantities_partial_max_quantity_in,
                    order_quantities_partial_filled_quantity,
                    order_quantities_exact_quantity,
                    varbinary_to_uint256(varbinary_substring(buf, pointer, 16)) AS max_extra_fee_asset0,
                    bitmap,
                    pointer + 16 AS pointer,
                    buf
                FROM order_quantities_field
            ),
            extra_fee_asset0_field AS (
                SELECT
                    tx_hash,
                    block_number,
                    idx,
                    ref_id,
                    use_internal,
                    pair_index,
                    min_price,
                    recipient,
                    hook_data,
                    zero_for_one,
                    standing_validation_nonce,
                    standing_validation_deadline,
                    order_quantities_kind,
                    order_quantities_partial_min_quantity_in,
                    order_quantities_partial_max_quantity_in,
                    order_quantities_partial_filled_quantity,
                    order_quantities_exact_quantity,
                    max_extra_fee_asset0,
                    varbinary_to_uint256(varbinary_substring(buf, pointer, 16)) AS extra_fee_asset0,
                    bitmap,
                    pointer + 16 AS pointer,
                    buf
                FROM max_extra_fee_asset0_field
            ),
            exact_in_field AS (
                SELECT
                    tx_hash,
                    block_number,
                    idx,
                    ref_id,
                    use_internal,
                    pair_index,
                    min_price,
                    recipient,
                    hook_data,
                    zero_for_one,
                    standing_validation_nonce,
                    standing_validation_deadline,
                    order_quantities_kind,
                    order_quantities_partial_min_quantity_in,
                    order_quantities_partial_max_quantity_in,
                    order_quantities_partial_filled_quantity,
                    order_quantities_exact_quantity,
                    max_extra_fee_asset0,
                    extra_fee_asset0,
                    if(bitmap[2] = 1, true, false) AS exact_in,
                    bitmap,
                    pointer,
                    buf
                FROM extra_fee_asset0_field
            ),
            signature_field AS (
                SELECT
                    tx_hash,
                    block_number,
                    idx,
                    ref_id,
                    use_internal,
                    pair_index,
                    min_price,
                    recipient,
                    hook_data,
                    zero_for_one,
                    standing_validation_nonce,
                    standing_validation_deadline,
                    order_quantities_kind,
                    order_quantities_partial_min_quantity_in,
                    order_quantities_partial_max_quantity_in,
                    order_quantities_partial_filled_quantity,
                    order_quantities_exact_quantity,
                    max_extra_fee_asset0,
                    extra_fee_asset0,
                    exact_in,
                    if(bitmap[1] = 1, 'Ecdsa', 'Contract') AS signature_kind,
                    if(bitmap[1] = 1, varbinary_to_bigint(varbinary_substring(buf, pointer, 1)), NULL) AS signature_ecdsa_v,
                    if(bitmap[1] = 1, varbinary_substring(buf, pointer + 1, 32), NULL) AS signature_ecdsa_r,
                    if(bitmap[1] = 1, varbinary_substring(buf, pointer + 33, 32), NULL) AS signature_ecdsa_s,
                    if(bitmap[1] = 0, varbinary_substring(buf, pointer, 20), NULL) AS signature_contract_from,
                    if(bitmap[1] = 0, varbinary_substring(buf, pointer + 23, varbinary_to_integer(varbinary_substring(buf, pointer + 20, 3))), NULL) AS signature_contract_signature,
                    bitmap,
                    if(bitmap[1] = 1, pointer + 65, pointer + 23 + varbinary_to_integer(varbinary_substring(buf, pointer + 20, 3))) AS pointer,
                    buf
                FROM exact_in_field
            ),
            all_fields_collapsed AS (
                SELECT 
                    tx_hash,
                    block_number,
                    ref_id,
                    use_internal,
                    pair_index,
                    min_price,
                    recipient,
                    hook_data,
                    zero_for_one,
                    standing_validation_nonce,
                    standing_validation_deadline,
                    order_quantities_kind,
                    order_quantities_partial_min_quantity_in,
                    order_quantities_partial_max_quantity_in,
                    order_quantities_partial_filled_quantity,
                    order_quantities_exact_quantity,
                    max_extra_fee_asset0,
                    extra_fee_asset0,
                    exact_in,
                    signature_kind,
                    signature_ecdsa_v,
                    signature_ecdsa_r,
                    signature_ecdsa_s,
                    signature_contract_from,
                    signature_contract_signature,
                    pointer,
                    idx,
                    buf
                FROM signature_field
            )
            SELECT 
                tx_hash,
                block_number,
                varbinary_substring(buf, pointer) AS buf,
                pointer,
                idx,
                ref_id,
                use_internal,
                pair_index,
                min_price,
                recipient,
                hook_data,
                zero_for_one,
                standing_validation_nonce,
                standing_validation_deadline,
                order_quantities_kind,
                order_quantities_partial_min_quantity_in,
                order_quantities_partial_max_quantity_in,
                order_quantities_partial_filled_quantity,
                order_quantities_exact_quantity,
                max_extra_fee_asset0,
                extra_fee_asset0,
                exact_in,
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
    FROM decode_user_order
    WHERE idx > 0
)
ORDER BY idx DESC




{% endmacro %}