WITH test_data AS (
    SELECT boc, CAST(expected AS ROW(opcode bigint, sub_opcode bigint, value bigint)) AS expected
    FROM (
        VALUES
        -- condition is not met
        (
            0xb5ee9c7201010101000e0000180000007b000001c90000002a,
            ROW(123, 457, null)
        ),
        -- condition is met
        (
            0xb5ee9c7201010101000e0000180000007b000001c80000002a,
            ROW(123, 456, 42)
        )
    )
    AS temp (boc, expected)
), test_results AS (    
    SELECT {{ ton_from_boc('boc', [
    ton_begin_parse(),
    ton_load_int(32, as='opcode'),
    ton_load_int(32, as='sub_opcode'),
    ton_return_if_neq('sub_opcode', 456),
    ton_load_int(32, as='value')
    ]) }} as result, expected 
    FROM test_data
)
SELECT result, expected FROM test_results
WHERE cast(result as json)!= cast(expected as json)