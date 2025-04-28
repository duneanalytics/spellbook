WITH test_data AS (
    SELECT boc, CAST(expected AS ROW(address varchar)) AS expected
    FROM (
        VALUES
        (
            0xb5ee9c72010102010028000101c0010043801b7d5443d4b7302d1c49680b51eeb0b25f9a28f28f24a831af7ec9e439050948f0,
            ROW('0:DBEAA21EA5B98168E24B405A8F758592FCD147947925418D7BF64F21C8284A47')
        ),
        -- addr_none
        (
            0xb5ee9c72010102010007000101c001000120,
            ROW('addr_none')
        ),
        -- empty maybe
        (
            0xb5ee9c7201010201002800010140010043801b7d5443d4b7302d1c49680b51eeb0b25f9a28f28f24a831af7ec9e439050948f0,
            ROW('addr_none')
        )
    )
    AS temp (boc, expected)
), test_results AS (    
    SELECT {{ ton_from_boc('boc', [
    ton_begin_parse(),
    ton_load_maybe_ref(),
    ton_begin_parse(),
    ton_load_address('address'),
    ]) }} as result, expected 
    FROM test_data
)
SELECT result, expected FROM test_results
WHERE result != expected