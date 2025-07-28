-- the idea of this is to get the offset of the buffer given a specific field to be decoded. This may not be possible or necessary?

{% macro
    angstrom_decoding_recursive(
        input_hex,
        field_index -- 0. assets, 1. pairs, 2. pool_updates, 3. top_of_block_orders, 4. user_orders
    )
%}





WITH
    -- assets
    step0 AS (
        SELECT
            1 AS offset,
            varbinary_to_integer(varbinary_substring(input_hex, offset, 3)) / 68 AS len, -- 68 bytes per `Asset`
            len + 3 AS next_offset,
            varbinary_substring(input_hex, offset, next_offset) AS next_buf

    ),
    -- pairs
    step1 AS (
        SELECT
            next_offset AS offset,
            varbinary_to_integer(varbinary_substring(next_buf, offset, 3)) / 38 AS len, -- 38 bytes per `Pair`
            len + 3 AS next_offset,
            varbinary_substring(next_buf, offset, next_offset) AS next_buf
        FROM step0
    )
    -- TODO: recusively trace down to step 4
SELECT
    offset
FROM ... -- Can we pass in the recusive step to query from?



{% endmacro %}