{% macro
    angstrom_decoding_recursive(
        raw_tx_input_hex,
        field_step
    )
%}
 -- 0. assets, 1. pairs, 2. pool_updates, 3. top_of_block_orders, 4. user_orders


WITH
    trimmed_input AS (
        SELECT 
            1 AS next_offset,
            varbinary_substring({{ raw_tx_input_hex }}, 69) AS next_buf
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
SELECT buf
FROM {{ field_step }}


{% endmacro %}

