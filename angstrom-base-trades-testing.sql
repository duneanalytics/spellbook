


WITH dexs AS
(
    

-------------------- TO TEST ------------------

-- single, TOB: 23077861 - 0xb72c702151c9004f3f327a82cfe451f69a206c21b82fa98419791ebc0bc29b94
-- single, USER: 23077829 - 0x32716081b3461e4f4770e14d97565c003aecf647837d151a8380f6b9722e7faf
-- multi, TOB: 
    -- 23085211 - 0xbb0cb5d7062a838a9b590a202a6e9b6478aa7e9a78824a21576dae1662b7dbcb
    -- 23085199 - 0xf07e41f652e68359a2c2fa1e571fdd05fa0eb4430da3941ce96744ac873408b1
    -- 23085183 - 0x627d33d7a00554446b2e4d109bc695c5d5b1131ed68980a24250e36103102c89
-- multi, USER: 
    -- 23084306 - 0x5f0a2eb5ea030dc3f18d03901ffe4ec161bb5fb5942e9904a3d1a75d5e6e53cc
    -- 23084299 - 0xd46f57a0e3aaa61a5f711cd7d2cf90f083e7e37d9125dd07e300a27d554c9c46
    -- 23083864 - 0x6e299e112769472208e63bd05bf40787ff9168c4731c6daa601c25b67f125d95

-----------------------------------------------



WITH
    tx_data_cte AS (
        

-- maybe use abi for log??

SELECT 
    block_number,
    block_time,
    hash AS tx_hash,
    index AS tx_index,
    to AS angstrom_address,
    data AS tx_data
FROM "delta_prod"."ethereum"."transactions"
WHERE to = 0x0000000aa232009084Bd71A5797d089AA4Edfad4 AND varbinary_substring(data, 1, 4) = 0x09c5eabe 



    ),
    tob_orders AS (
        SELECT 
            t.block_number AS block_number,
            t.block_time AS block_time,
            p.quantity_in       AS token_bought_amount_raw,
            p.quantity_out      AS token_sold_amount_raw,
            p.asset_out          AS token_bought_address,
            p.asset_in         AS token_sold_address,
            p.recipient AS taker,
            t.angstrom_address AS maker,
            t.angstrom_address AS project_contract_address,
            t.tx_hash AS tx_hash,
            row_number() over (partition by t.tx_hash) as evt_index
        FROM tx_data_cte t
        INNER JOIN (



SELECT 
    ab.*,
    if(ab.zero_for_1, asts.asset_in, asts.asset_out) AS asset_in,
    if(ab.zero_for_1, asts.asset_out, asts.asset_in) AS asset_out,
    asts.price_1over0
FROM (


WITH vec_pade AS (
    SELECT 
        tx_hash,
        block_number,
        buf
    FROM (
 -- 0. assets, 1. pairs, 2. pool_updates, 3. top_of_block_orders, 4. user_orders


WITH
    tx_data_cte AS (
        

-- maybe use abi for log??

SELECT 
    block_number,
    block_time,
    hash AS tx_hash,
    index AS tx_index,
    to AS angstrom_address,
    data AS tx_data
FROM "delta_prod"."ethereum"."transactions"
WHERE to = 0x0000000aa232009084Bd71A5797d089AA4Edfad4 AND varbinary_substring(data, 1, 4) = 0x09c5eabe 



    ),
    trimmed_input AS (
        SELECT 
            tx_hash,
            block_number,
            1 AS next_offset,
            varbinary_substring(t.tx_data, 69) AS next_buf
        FROM tx_data_cte AS t
    ),
    -- assets
    step0 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM trimmed_input
        )
    ),
    -- pairs
    step1 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step0
        )
    ),
    -- pool updates
    step2 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step1
        )
    ),
    -- top of block orders
    step3 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step2
        )
    ),
    -- user orders
    step4 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step3
        )
    )
SELECT 
    tx_hash,
    block_number,
    buf
FROM step3


)
)
SELECT
    tx_hash,
    block_number,
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
        tx_hash,
        block_number,
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
            tx_hash,
            block_number,
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
            tx_hash,
            block_number,
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
                    tx_hash,
                    block_number,
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
                    tx_hash,
                    block_number,
                    idx,
                    if(bitmap[4] = 1, true, false) AS use_internal,
                    bitmap,
                    pointer,
                    buf
                FROM trimmed_as_fields
            ),
            quantity_in_field AS (
                SELECT
                    tx_hash,
                    block_number,
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
                    tx_hash,
                    block_number,
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
                    tx_hash,
                    block_number,
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
                    tx_hash,
                    block_number,
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
                    tx_hash,
                    block_number,
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
                    tx_hash,
                    block_number,
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
                    tx_hash,
                    block_number,
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
                    tx_hash,
                    block_number,
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
                    tx_hash,
                    block_number,
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
                tx_hash,
                block_number,
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


) AS ab
INNER JOIN (

WITH
    assets AS (
        SELECT  
            tx_hash,
            block_number,
            bundle_idx - 1 AS bundle_idx,
            token_address
        FROM (


WITH vec_pade AS (
    SELECT 
        tx_hash,
        block_number,
        buf
    FROM (
 -- 0. assets, 1. pairs, 2. pool_updates, 3. top_of_block_orders, 4. user_orders


WITH
    tx_data_cte AS (
        

-- maybe use abi for log??

SELECT 
    block_number,
    block_time,
    hash AS tx_hash,
    index AS tx_index,
    to AS angstrom_address,
    data AS tx_data
FROM "delta_prod"."ethereum"."transactions"
WHERE to = 0x0000000aa232009084Bd71A5797d089AA4Edfad4 AND varbinary_substring(data, 1, 4) = 0x09c5eabe 



    ),
    trimmed_input AS (
        SELECT 
            tx_hash,
            block_number,
            1 AS next_offset,
            varbinary_substring(t.tx_data, 69) AS next_buf
        FROM tx_data_cte AS t
    ),
    -- assets
    step0 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM trimmed_input
        )
    ),
    -- pairs
    step1 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step0
        )
    ),
    -- pool updates
    step2 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step1
        )
    ),
    -- top of block orders
    step3 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step2
        )
    ),
    -- user orders
    step4 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step3
        )
    )
SELECT 
    tx_hash,
    block_number,
    buf
FROM step0


)
)
SELECT 
    tx_hash,
    block_number,
    bundle_idx,
    token_address,
    save_amount,
    take_amount,
    settle_amount
FROM (
    WITH RECURSIVE decode_asset (tx_hash, block_number, buf, len, idx, addr, save, take, settle) AS (
        SELECT
            tx_hash,
            block_number,
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
            tx_hash,
            block_number,
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
        tx_hash,
        block_number,
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


)
    ),
    pairs AS (
        SELECT 
            tx_hash,
            block_number,
            bundle_idx - 1 AS bundle_idx, 
            index0,
            index1,
            price_1over0
        FROM (


WITH vec_pade AS (
    SELECT 
        tx_hash,
        block_number,
        buf
    FROM (
 -- 0. assets, 1. pairs, 2. pool_updates, 3. top_of_block_orders, 4. user_orders


WITH
    tx_data_cte AS (
        

-- maybe use abi for log??

SELECT 
    block_number,
    block_time,
    hash AS tx_hash,
    index AS tx_index,
    to AS angstrom_address,
    data AS tx_data
FROM "delta_prod"."ethereum"."transactions"
WHERE to = 0x0000000aa232009084Bd71A5797d089AA4Edfad4 AND varbinary_substring(data, 1, 4) = 0x09c5eabe 



    ),
    trimmed_input AS (
        SELECT 
            tx_hash,
            block_number,
            1 AS next_offset,
            varbinary_substring(t.tx_data, 69) AS next_buf
        FROM tx_data_cte AS t
    ),
    -- assets
    step0 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM trimmed_input
        )
    ),
    -- pairs
    step1 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step0
        )
    ),
    -- pool updates
    step2 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step1
        )
    ),
    -- top of block orders
    step3 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step2
        )
    ),
    -- user orders
    step4 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step3
        )
    )
SELECT 
    tx_hash,
    block_number,
    buf
FROM step1


)
)
SELECT 
    tx_hash,
    block_number,
    bundle_idx,
    index0, 
    index1, 
    store_index, 
    price_1over0
FROM (
    WITH RECURSIVE decode_pair (tx_hash, block_number, buf, len, idx, index0, index1, store_index, price_1over0) AS (
        SELECT
            tx_hash,
            block_number,
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
            tx_hash,
            block_number,
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
        tx_hash,
        block_number,
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



)
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




) AS asts
    ON asts.bundle_pair_index = ab.pairs_index AND ab.block_number = asts.block_number AND ab.tx_hash = asts.tx_hash

) AS p
            ON t.tx_hash = p.tx_hash AND t.block_number = p.block_number
    ),
    user_orders AS (
        SELECT 
            t.block_number AS block_number,
            t.block_time AS block_time,
            p.t0_amount AS token_bought_amount_raw,
            p.t1_amount AS token_sold_amount_raw,
            p.asset_out AS token_bought_address,
            p.asset_in AS token_sold_address,
            p.recipient AS taker,
            t.angstrom_address AS maker,
            t.angstrom_address AS project_contract_address,
            t.tx_hash AS tx_hash,
            row_number() OVER (PARTITION BY t.tx_hash) + tc.tob_cnt AS evt_index
        FROM tx_data_cte t
        INNER JOIN (



WITH
    user_orders AS (
        SELECT 
            ab.*,
            if(ab.order_quantities_kind = 'Exact', ab.order_quantities_exact_quantity, ab.order_quantities_partial_filled_quantity) AS fill_amount,
            if(ab.zero_for_one, asts.asset_in, asts.asset_out) AS asset_in,
            if(ab.zero_for_one, asts.asset_out, asts.asset_in) AS asset_out,
            asts.price_1over0
        FROM (


WITH vec_pade AS (
    SELECT 
        tx_hash,
        block_number,
        buf
    FROM (
 -- 0. assets, 1. pairs, 2. pool_updates, 3. top_of_block_orders, 4. user_orders


WITH
    tx_data_cte AS (
        

-- maybe use abi for log??

SELECT 
    block_number,
    block_time,
    hash AS tx_hash,
    index AS tx_index,
    to AS angstrom_address,
    data AS tx_data
FROM "delta_prod"."ethereum"."transactions"
WHERE to = 0x0000000aa232009084Bd71A5797d089AA4Edfad4 AND varbinary_substring(data, 1, 4) = 0x09c5eabe 



    ),
    trimmed_input AS (
        SELECT 
            tx_hash,
            block_number,
            1 AS next_offset,
            varbinary_substring(t.tx_data, 69) AS next_buf
        FROM tx_data_cte AS t
    ),
    -- assets
    step0 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM trimmed_input
        )
    ),
    -- pairs
    step1 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step0
        )
    ),
    -- pool updates
    step2 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step1
        )
    ),
    -- top of block orders
    step3 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step2
        )
    ),
    -- user orders
    step4 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step3
        )
    )
SELECT 
    tx_hash,
    block_number,
    buf
FROM step4


)
)
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




) AS ab
        INNER JOIN (

WITH
    assets AS (
        SELECT  
            tx_hash,
            block_number,
            bundle_idx - 1 AS bundle_idx,
            token_address
        FROM (


WITH vec_pade AS (
    SELECT 
        tx_hash,
        block_number,
        buf
    FROM (
 -- 0. assets, 1. pairs, 2. pool_updates, 3. top_of_block_orders, 4. user_orders


WITH
    tx_data_cte AS (
        

-- maybe use abi for log??

SELECT 
    block_number,
    block_time,
    hash AS tx_hash,
    index AS tx_index,
    to AS angstrom_address,
    data AS tx_data
FROM "delta_prod"."ethereum"."transactions"
WHERE to = 0x0000000aa232009084Bd71A5797d089AA4Edfad4 AND varbinary_substring(data, 1, 4) = 0x09c5eabe 



    ),
    trimmed_input AS (
        SELECT 
            tx_hash,
            block_number,
            1 AS next_offset,
            varbinary_substring(t.tx_data, 69) AS next_buf
        FROM tx_data_cte AS t
    ),
    -- assets
    step0 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM trimmed_input
        )
    ),
    -- pairs
    step1 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step0
        )
    ),
    -- pool updates
    step2 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step1
        )
    ),
    -- top of block orders
    step3 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step2
        )
    ),
    -- user orders
    step4 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step3
        )
    )
SELECT 
    tx_hash,
    block_number,
    buf
FROM step0


)
)
SELECT 
    tx_hash,
    block_number,
    bundle_idx,
    token_address,
    save_amount,
    take_amount,
    settle_amount
FROM (
    WITH RECURSIVE decode_asset (tx_hash, block_number, buf, len, idx, addr, save, take, settle) AS (
        SELECT
            tx_hash,
            block_number,
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
            tx_hash,
            block_number,
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
        tx_hash,
        block_number,
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


)
    ),
    pairs AS (
        SELECT 
            tx_hash,
            block_number,
            bundle_idx - 1 AS bundle_idx, 
            index0,
            index1,
            price_1over0
        FROM (


WITH vec_pade AS (
    SELECT 
        tx_hash,
        block_number,
        buf
    FROM (
 -- 0. assets, 1. pairs, 2. pool_updates, 3. top_of_block_orders, 4. user_orders


WITH
    tx_data_cte AS (
        

-- maybe use abi for log??

SELECT 
    block_number,
    block_time,
    hash AS tx_hash,
    index AS tx_index,
    to AS angstrom_address,
    data AS tx_data
FROM "delta_prod"."ethereum"."transactions"
WHERE to = 0x0000000aa232009084Bd71A5797d089AA4Edfad4 AND varbinary_substring(data, 1, 4) = 0x09c5eabe 



    ),
    trimmed_input AS (
        SELECT 
            tx_hash,
            block_number,
            1 AS next_offset,
            varbinary_substring(t.tx_data, 69) AS next_buf
        FROM tx_data_cte AS t
    ),
    -- assets
    step0 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM trimmed_input
        )
    ),
    -- pairs
    step1 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step0
        )
    ),
    -- pool updates
    step2 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step1
        )
    ),
    -- top of block orders
    step3 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step2
        )
    ),
    -- user orders
    step4 AS (
        SELECT
            tx_hash,
            block_number,
            len + 3 + offset AS next_offset,
            varbinary_substring(next_buf, offset, len + 3) AS buf,
            next_buf
        FROM (
            SELECT 
                tx_hash,
                block_number,
                next_offset AS offset,
                varbinary_to_integer(varbinary_substring(next_buf, next_offset, 3)) AS len,
                next_buf
            FROM step3
        )
    )
SELECT 
    tx_hash,
    block_number,
    buf
FROM step1


)
)
SELECT 
    tx_hash,
    block_number,
    bundle_idx,
    index0, 
    index1, 
    store_index, 
    price_1over0
FROM (
    WITH RECURSIVE decode_pair (tx_hash, block_number, buf, len, idx, index0, index1, store_index, price_1over0) AS (
        SELECT
            tx_hash,
            block_number,
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
            tx_hash,
            block_number,
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
        tx_hash,
        block_number,
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



)
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




) AS asts
            ON asts.bundle_pair_index = ab.pair_index AND ab.block_number = asts.block_number AND ab.tx_hash = asts.tx_hash
    ),
    orders_with_assets AS (
        SELECT
            u.*,
            f.bundle_fee AS bundle_fee
        FROM user_orders AS u
        INNER JOIN (

-- maybe use abi for log??

WITH fee_events AS (
    SELECT 
        block_number,
        varbinary_to_integer(varbinary_substring(l.data, 62, 3)) AS bundle_fee,
        varbinary_to_integer(varbinary_substring(l.data, 94, 3)) AS unlocked_fee,
        varbinary_to_integer(varbinary_substring(l.data, 126, 3)) AS protocol_unlocked_fee,
        topic1,
        topic2
    FROM "delta_prod"."ethereum"."logs" AS l
    WHERE 
        contract_address = 0xFE77113460CF1833c4440FD17B4463f472010e10 AND 
        topic0 = 0xf325a037d71efc98bc41dc5257edefd43a1d1162e206373e53af271a7a3224e9
),
block_range AS (
    SELECT number AS block_number
    FROM "delta_prod"."ethereum"."blocks"
    WHERE number >= (SELECT MIN(block_number) FROM fee_events)
),
topic_pairs AS (
    SELECT DISTINCT topic1, topic2
    FROM fee_events
),
block_topic_combinations AS (
    SELECT 
        br.block_number,
        tp.topic1,
        tp.topic2
    FROM block_range br
    CROSS JOIN topic_pairs tp
),
latest_fees_per_pair AS (
    SELECT 
        btc.block_number,
        btc.topic1,
        btc.topic2,
        fe.bundle_fee,
        fe.unlocked_fee,
        fe.protocol_unlocked_fee,
        ROW_NUMBER() OVER (
            PARTITION BY btc.block_number, btc.topic1, btc.topic2 
            ORDER BY fe.block_number DESC
        ) AS rn
    FROM block_topic_combinations btc
    LEFT JOIN fee_events fe 
        ON fe.topic1 = btc.topic1 
        AND fe.topic2 = btc.topic2
        AND fe.block_number <= btc.block_number
)
SELECT 
    block_number,
    bundle_fee,
    unlocked_fee,
    protocol_unlocked_fee,
    topic1,
    topic2
FROM latest_fees_per_pair
WHERE rn = 1 AND bundle_fee IS NOT NULL
ORDER BY block_number DESC, topic1, topic2


) AS f
            ON u.block_number = f.block_number AND
                ((varbinary_substring(f.topic1, 13, 20) = u.asset_in OR varbinary_substring(f.topic2, 13, 20) = u.asset_in) AND 
                (varbinary_substring(f.topic1, 13, 20) = u.asset_out OR varbinary_substring(f.topic2, 13, 20) = u.asset_out)) 
    ),
    orders_with_priced_assets AS (
        SELECT 
            u.*,
            a.*
        FROM orders_with_assets AS u
        CROSS JOIN LATERAL (

WITH
    case_bools AS (
        SELECT 
            ARRAY[NOT u.zero_for_one, u.exact_in] AS cases,
            CAST(u.fill_amount AS uint256) AS fill_amount,
            CAST(u.price_1over0 AS uint256) AS ray_ucp,
            u.bundle_fee AS fee,
            u.extra_fee_asset0 AS gas
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
FROM orders_with_priced_assets





) AS p 
            ON t.tx_hash = p.tx_hash AND t.block_number = p.block_number
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