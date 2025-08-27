{% macro
    angstrom_decoding_recursive(
        angstrom_contract_addr,
        earliest_block,
        blockchain,
        field_step
    )
%}
 -- 0. assets, 1. pairs, 2. pool_updates, 3. top_of_block_orders, 4. user_orders


WITH
    tx_data_cte AS (
        {{ angstrom_tx_data(angstrom_contract_addr, earliest_block, blockchain) }}
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
FROM {{ field_step }}


{% endmacro %}

