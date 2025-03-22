WITH test_data AS (
    SELECT boc, CAST(expected AS ROW(value int)) AS expected, offset_ref
    FROM (
        VALUES
        -- first ref
        (
            0xb5ee9c7201010201000d0001080000000b0100080012d687,
            ROW(1234567),
            0
        ),
        -- second ref (first one is empty)
        (
            0xb5ee9c720101030100100002080000000b0102000000080012d687,
            ROW(1234567),
            1
        ),
        -- second ref (the same like first one)
        (
            0xb5ee9c7201010201000e0002080000000b010100080012d687,
            ROW(1234567),
            1
        ),
        -- third ref (the same like first one)
        (
            0xb5ee9c7201010201000f0003080000000b01010100080012d687,
            ROW(1234567),
            2
        ),
        -- third ref (more complex)
        (
            0xb5ee9c720101030100160003080000000b01010101080012d6870200080012d687,
            ROW(1234567),
            2
        ),
        -- third ref (more complex)
        (
            0xb5ee9c720101030100170003080000000b01010101080012d6870200090012d687c0,
            ROW(1234567),
            2
        )
    )
    AS temp (boc, expected, offset_ref)
), test_results AS (    
    SELECT {{ ton_from_boc('boc', [
    ton_begin_parse(),
    ton_skip_refs('offset_ref'),
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_uint(32, as='value')
    ]) }} as result, expected 
    FROM test_data
)
SELECT json_format(CAST(result AS json)) AS result, json_format(CAST(expected AS json)) AS expected FROM test_results
WHERE result != expected