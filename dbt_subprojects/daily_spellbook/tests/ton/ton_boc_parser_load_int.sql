WITH test_data AS (
    SELECT boc, CAST(expected AS ROW(int8 bigint, int2 bigint, int32 bigint, int64 varchar, int128 varchar)) AS expected, offset
    FROM (
        VALUES
        -- all zeroes
        (
            0xb5ee9c7201010101004000007b0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000040,
            ROW(0, 0, 0, '0', '0'),
            0
        ),
        -- max values
        (
            0xb5ee9c7201010101004000007b7f5fffffffdfffffffffffffffdfffffffffffffffffffffffffffffffdfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffc0,
            ROW(127, 1, 2147483647, '9223372036854775807', '170141183460469231731687303715884105727'),
            0
        ),
        -- min values
        (
            0xb5ee9c7201010101004000007b81e0000000600000000000000060000000000000000000000000000000600000000000000000000000000000000000000000000000000000000000000060,
            ROW(-127, -1, -2147483647, '-9223372036854775807', '-170141183460469231731687303715884105727'),
            0
        )
    )
    AS temp (boc, expected, offset)
), test_results AS (    
    SELECT {{ ton_from_boc('boc', [
    ton_begin_parse(),
    ton_skip_bits('offset'),
    ton_load_int(8, as='int8'),
    ton_load_int(2, as='int2'),
    ton_load_int(32, as='int32'),
    ton_load_int(64, as='int64'),
    ton_load_int(128, as='int128')
    ]) }} as result, expected 
    FROM test_data
)
SELECT json_format(CAST(result AS json)) AS result, json_format(CAST(expected AS json)) AS expected FROM test_results
WHERE result != expected