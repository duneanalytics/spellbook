WITH test_data AS (
    SELECT boc, CAST(expected AS ROW(value1 UINT256)) AS expected
    FROM (
        VALUES
        -- empty cell
        (
            0xb5ee9c7201010101000300000140,
            ROW(CAST(null AS UINT256))
        ),
        -- nonempty cell
        (
            0xb5ee9c7201010201000a000101c001000800003039,
            ROW(CAST(12345 AS UINT256))
        ),
        -- nonempty cell but empty maybe flag
        (
            0xb5ee9c7201010201000a0001014001000800003039,
            ROW(CAST(null AS UINT256))
        )
    )
    AS temp (boc, expected)
), test_results AS (    
    SELECT {{ ton_from_boc('boc', [
    ton_begin_parse(),
    ton_load_maybe_ref(),
    ton_begin_parse(),
    ton_load_uint(32, 'value1'),
    ]) }} as result, expected 
    FROM test_data
)
SELECT result, expected FROM test_results
WHERE result != expected