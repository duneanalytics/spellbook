WITH test_data AS (
    SELECT boc, expected, offset
    FROM (
        VALUES
        -- all zeroes
        (
            0xb5ee9c7201010101004000007b0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000040,
            map_from_entries(ARRAY[
                ('int2', CAST(CAST(0 AS int) AS JSON)),
                ('int8', CAST(CAST(0 AS int) AS JSON)),
                ('int32', CAST(CAST(0 AS int) AS JSON)),
                ('int64', CAST(CAST(0 AS varchar) AS JSON)),
                ('int128', CAST(CAST(0 AS varchar) AS JSON))
            ]),
            0
        ),
        -- max values
        (
            0xb5ee9c7201010101004000007b7f5fffffffdfffffffffffffffdfffffffffffffffffffffffffffffffdfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffc0,
            map_from_entries(ARRAY[
                ('int2', CAST(CAST(1 AS int) AS JSON)),
                ('int8', CAST(CAST(127 AS int) AS JSON)),
                ('int32', CAST(CAST(2147483647 AS int) AS JSON)),
                ('int64', CAST(CAST('9223372036854775807' AS varchar) AS JSON)),
                ('int128', CAST(CAST('170141183460469231731687303715884105727' AS varchar) AS JSON))
                ]),
            0
        ),
        -- min values
        (
            0xb5ee9c7201010101004000007b81e0000000600000000000000060000000000000000000000000000000600000000000000000000000000000000000000000000000000000000000000060,
            map_from_entries(ARRAY[
                ('int2', CAST(CAST(-1 AS int) AS JSON)),
                ('int8', CAST(CAST(-127 AS int) AS JSON)),
                ('int32', CAST(CAST(-2147483647 AS int) AS JSON)),
                ('int64', CAST(CAST('-9223372036854775807' AS varchar) AS JSON)),
                ('int128', CAST(CAST('-170141183460469231731687303715884105727' AS varchar) AS JSON))
                ]),
            0
        )
    )
    AS temp (boc, expected, offset)
), test_results AS (    
    SELECT {{ ton_from_boc('boc', [
    ton_begin_parse(),
    ton_skip_bits('offset'),
    ton_load_int(8, 'int8'),
    ton_load_int(2, 'int2'),
    ton_load_int(32, 'int32'),
    ton_load_int(64, 'int64'),
    ton_load_int(128, 'int128')
    ]) }} as result, expected 
    FROM test_data
)
SELECT json_format(CAST(result AS json)) AS result, json_format(CAST(expected AS json)) AS expected FROM test_results
WHERE result != expected