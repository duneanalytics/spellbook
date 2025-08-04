


WITH dexs AS
(
    


WITH
    tx_data AS (
        SELECT 
            block_number,
            block_time,
            hash AS tx_hash,
            index AS tx_index,
            to AS angstrom_address,
            data AS tx_data
        FROM "delta_prod"."ethereum"."transactions"
        WHERE to = angstrom_contract_addr AND varbinary_substring(data, 1, 4) = 0x09c5eabe
    ),
    tob_orders AS (
        SELECT 
            block_number,
            block_time,
            quantity_in AS token_bought_amount_raw,
            quantity_out AS token_sold_amount_raw,
            asset_in AS token_bought_address,
            asset_out AS token_sold_address,
            recipient AS taker,
            angstrom_address AS maker,
            angstrom_address AS project_contract_address,
            tx_hash,
            row_number() OVER (PARTITION BY tx_hash) AS evt_index
        FROM 



SELECT 
    ab.*,
    asts.*
FROM (


WITH vec_pade AS (
    SELECT buf
    FROM (
 -- 0. assets, 1. pairs, 2. pool_updates, 3. top_of_block_orders, 4. user_orders


WITH
    trimmed_input AS (
        SELECT 
            1 AS next_offset,
            varbinary_substring(input_hex, 69) AS next_buf
    ),
    -- assets
    step0 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM trimmed_input
        )
    ),
    -- pairs
    step1 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step0
        )
    ),
    -- pool updates
    step2 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step1
        )
    ),
    -- top of block orders
    step3 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step2
        )
    ),
    -- user orders
    step4 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step3
        )
    )
SELECT
    buf
FROM 



)
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
ORDER BY idx DESC;


) AS ab
CROSS JOIN (

WITH
    assets AS (
        SELECT *
        FROM (


WITH vec_pade AS (
    SELECT buf
    FROM (
 -- 0. assets, 1. pairs, 2. pool_updates, 3. top_of_block_orders, 4. user_orders


WITH
    trimmed_input AS (
        SELECT 
            1 AS next_offset,
            varbinary_substring(input_hex, 69) AS next_buf
    ),
    -- assets
    step0 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM trimmed_input
        )
    ),
    -- pairs
    step1 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step0
        )
    ),
    -- pool updates
    step2 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step1
        )
    ),
    -- top of block orders
    step3 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step2
        )
    ),
    -- user orders
    step4 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step3
        )
    )
SELECT
    buf
FROM 



)
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
);


)
    ),
    pairs AS (
        SELECT 
            index0,
            index1,
            price_1over0
        FROM (


WITH vec_pade AS (
    SELECT buf
    FROM (
 -- 0. assets, 1. pairs, 2. pool_updates, 3. top_of_block_orders, 4. user_orders


WITH
    trimmed_input AS (
        SELECT 
            1 AS next_offset,
            varbinary_substring(input_hex, 69) AS next_buf
    ),
    -- assets
    step0 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM trimmed_input
        )
    ),
    -- pairs
    step1 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step0
        )
    ),
    -- pool updates
    step2 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step1
        )
    ),
    -- top of block orders
    step3 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step2
        )
    ),
    -- user orders
    step4 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step3
        )
    )
SELECT
    buf
FROM 



)
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
);



)
        WHERE bundle_idx = pair_index
    ),
    _asset_in AS (
        SELECT
            price_1over0,
            token_address AS asset_in
        FROM assets AS a
        CROSS JOIN pairs AS p
        WHERE a.bundle_idx = p.index0 AND p.bundle_idx = pair_index
    ),
    _asset_out AS (
        SELECT
            token_address AS asset_out
        FROM assets AS a
        CROSS JOIN pairs AS p
        WHERE a.bundle_idx = p.index1 AND p.bundle_idx = pair_index
    ),
    zfo_assets AS (
        SELECT
            price_1over0,
            if(zfo, ARRAY[asset_in, asset_out], ARRAY[asset_out, asset_in]) AS zfo_sorted_assets
        FROM _asset_in i 
        CROSS JOIN _asset_out o
    )
SELECT
    zfo_sorted_assets[1] AS asset_in,
    zfo_sorted_assets[2] AS asset_out,
    price_1over0
FROM zfo_assets



) AS asts



    ),
    user_orders AS (
        SELECT 
            block_number,
            block_time,
            t0_amount AS token_bought_amount_raw,
            t1_amount AS token_sold_amount_raw,
            asset_in AS token_bought_address,
            asset_out AS token_sold_address,
            recipient AS taker,
            angstrom_address AS maker,
            angstrom_address AS project_contract_address,
            tx_hash,
            row_number() OVER (PARTITION BY tx_hash) + tc.tob_cnt AS evt_index
        FROM 



WITH
    user_orders AS (
        SELECT 
            ab.*,
            if(ab.order_quantities_kind = 'Exact', ab.order_quantities_exact_quantity, ab.order_quantities_partial_filled_quantity) AS fill_amount,
            asts.*
        FROM (


WITH vec_pade AS (
    SELECT buf
    FROM (
 -- 0. assets, 1. pairs, 2. pool_updates, 3. top_of_block_orders, 4. user_orders


WITH
    trimmed_input AS (
        SELECT 
            1 AS next_offset,
            varbinary_substring(input_hex, 69) AS next_buf
    ),
    -- assets
    step0 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM trimmed_input
        )
    ),
    -- pairs
    step1 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step0
        )
    ),
    -- pool updates
    step2 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step1
        )
    ),
    -- top of block orders
    step3 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step2
        )
    ),
    -- user orders
    step4 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step3
        )
    )
SELECT
    buf
FROM 



)
)
SELECT
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
    WITH RECURSIVE decode_user_order (
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
                    idx,
                    varbinary_to_bigint(varbinary_substring(buf, pointer, 4)) AS ref_id,
                    bitmap,
                    pointer + 4 AS pointer,
                    buf
                FROM trimmed_as_fields
            ),
            use_internal_field AS (
                SELECT
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
ORDER BY idx DESC;




) AS ab
        CROSS JOIN (

WITH
    assets AS (
        SELECT *
        FROM (


WITH vec_pade AS (
    SELECT buf
    FROM (
 -- 0. assets, 1. pairs, 2. pool_updates, 3. top_of_block_orders, 4. user_orders


WITH
    trimmed_input AS (
        SELECT 
            1 AS next_offset,
            varbinary_substring(input_hex, 69) AS next_buf
    ),
    -- assets
    step0 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM trimmed_input
        )
    ),
    -- pairs
    step1 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step0
        )
    ),
    -- pool updates
    step2 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step1
        )
    ),
    -- top of block orders
    step3 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step2
        )
    ),
    -- user orders
    step4 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step3
        )
    )
SELECT
    buf
FROM 



)
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
);


)
    ),
    pairs AS (
        SELECT 
            index0,
            index1,
            price_1over0
        FROM (


WITH vec_pade AS (
    SELECT buf
    FROM (
 -- 0. assets, 1. pairs, 2. pool_updates, 3. top_of_block_orders, 4. user_orders


WITH
    trimmed_input AS (
        SELECT 
            1 AS next_offset,
            varbinary_substring(input_hex, 69) AS next_buf
    ),
    -- assets
    step0 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM trimmed_input
        )
    ),
    -- pairs
    step1 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step0
        )
    ),
    -- pool updates
    step2 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step1
        )
    ),
    -- top of block orders
    step3 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step2
        )
    ),
    -- user orders
    step4 AS (
        SELECT
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step3
        )
    )
SELECT
    buf
FROM 



)
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
);



)
        WHERE bundle_idx = pair_index
    ),
    _asset_in AS (
        SELECT
            price_1over0,
            token_address AS asset_in
        FROM assets AS a
        CROSS JOIN pairs AS p
        WHERE a.bundle_idx = p.index0 AND p.bundle_idx = pair_index
    ),
    _asset_out AS (
        SELECT
            token_address AS asset_out
        FROM assets AS a
        CROSS JOIN pairs AS p
        WHERE a.bundle_idx = p.index1 AND p.bundle_idx = pair_index
    ),
    zfo_assets AS (
        SELECT
            price_1over0,
            if(zfo, ARRAY[asset_in, asset_out], ARRAY[asset_out, asset_in]) AS zfo_sorted_assets
        FROM _asset_in i 
        CROSS JOIN _asset_out o
    )
SELECT
    zfo_sorted_assets[1] AS asset_in,
    zfo_sorted_assets[2] AS asset_out,
    price_1over0
FROM zfo_assets



) AS asts
    ),
    orders_with_assets AS (
        SELECT
            u.*,
            a.*
        FROM user_orders AS u
        CROSS JOIN (

-- TODO generalize blockchain + addresses, (maybe use abi too?)

SELECT 
    varbinary_to_integer(varbinary_substring(data, 62, 3)) AS bundle_fee,
    varbinary_to_integer(varbinary_substring(data, 94, 3)) AS unlocked_fee,
    varbinary_to_integer(varbinary_substring(data, 126, 3)) AS protocol_unlocked_fee
FROM ethereum.logs
WHERE 
    contract_address = 0xFE77113460CF1833c4440FD17B4463f472010e10 AND 
    topic0 = 0xf325a037d71efc98bc41dc5257edefd43a1d1162e206373e53af271a7a3224e9 AND
    block_number <= fetched_bn AND 
    (varbinary_substring(topic1, 13, 20) = asset0 OR varbinary_substring(topic2, 13, 20) = asset0) AND 
    (varbinary_substring(topic1, 13, 20) = asset1 OR varbinary_substring(topic2, 13, 20) = asset1)
ORDER BY block_number DESC 
LIMIT 1


) AS f
        CROSS JOIN (

WITH
    case_bools AS (
        SELECT 
            ARRAY[is_bid, exact_in] AS cases,
            CAST(fill_amount AS uint256) AS fill_amount,
            CAST(ray_ucp AS uint256) AS ray_ucp
    ),
    amount_case AS (
        SELECT
            CASE cases
                WHEN ARRAY[true, true] THEN 
                    ARRAY[fill_amount, floor(if(fee = 0, pow(10, 54) / ray_ucp, floor(((pow(10, 54) / ray_ucp) * (pow(10, 6) - fee)) / pow(10, 6))) * fill_amount / pow(10, 27)) - gas]
                WHEN ARRAY[true, false] THEN 
                    ARRAY[ceiling((fill_amount + gas) * pow(10, 27) / if(fee = 0, pow(10, 54) / ray_ucp, floor(((pow(10, 54) / ray_ucp) * (pow(10, 6) - fee)) / pow(10, 6)))), fill_amount]
                WHEN ARRAY[false, true] THEN 
                    ARRAY[floor(if(fee = 0, ray_ucp, floor(ray_ucp * (pow(10, 6) - fee) / pow(10, 6))) * (fill_amount - gas) / pow(10, 27)), ceiling(floor(if(fee = 0, ray_ucp, floor(ray_ucp * (pow(10, 6) - fee) / pow(10, 6))) * (fill_amount - gas) / pow(10, 27)) * pow(10, 27) / ray_ucp)]
                WHEN ARRAY[false, false] THEN
                    ARRAY[fill_amount, ceiling(fill_amount * pow(10, 27) / ray_ucp)]
            END AS cases_for_params
        FROM case_bools
    )
SELECT
    CAST(cases_for_params[2] AS uint256) AS t0_amount,
    CAST(cases_for_params[1] AS uint256) AS t1_amount
FROM amount_case

-- TODO: investigate tiny rounding error (approx 10^-20 units off, so very insignificant)


) AS a
    )
SELECT
    *
FROM orders_with_assets





 AS uo
        CROSS JOIN ( SELECT COUNT(*) AS tob_cnt FROM tob_orders ) AS tc
    )
SELECT
    'ethereum' AS blockchain
    , 'angstrom' AS project
    , '2' AS version
    , CAST(date_trunc('month', block_time) AS date) AS block_month
    , CAST(date_trunc('day', block_time) AS date) AS block_date
    , block_time
    , block_number
    , token_bought_amount_raw
    , token_sold_amount_raw
    , token_bought_address
    , token_sold_address
    , taker
    , maker
    , project_contract_address
    , tx_hash
    , evt_index
FROM tob_orders

UNION ALL 

SELECT 
    'ethereum' AS blockchain
    , 'angstrom' AS project
    , '2' AS version
    , CAST(date_trunc('month', block_time) AS date) AS block_month
    , CAST(date_trunc('day', block_time) AS date) AS block_date
    , block_time
    , block_number
    , token_bought_amount_raw
    , token_sold_amount_raw
    , token_bought_address
    , token_sold_address
    , taker
    , maker
    , project_contract_address
    , tx_hash
    , evt_index
FROM user_orders



)

SELECT * FROM dexs