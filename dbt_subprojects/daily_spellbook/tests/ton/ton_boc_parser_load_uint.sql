WITH test_data AS (
    SELECT boc, CAST(expected AS ROW(uint8 bigint, uint1 bigint, uint32 bigint, uint64 UINT256, uint128 UINT256, uint256 UINT256)) AS expected, offset
    FROM (
        VALUES
        -- all zeroes
        (
            0xb5ee9c7201010101004000007b0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000040,
            ROW(0, 0, 0, CAST(0 AS UINT256), CAST(0 AS UINT256), CAST(0 AS UINT256)),
            0
        ),
        -- -- max value
        (
            0xb5ee9c7201010101004000007bffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffc0,
            ROW(255, 1, 4294967295, CAST('18446744073709551615' AS UINT256), CAST('340282366920938463463374607431768211455' AS UINT256), CAST('115792089237316195423570985008687907853269984665640564039457584007913129639935' AS UINT256)),
            0
        ),
        -- -- random values
        (
            0xb5ee9c7201010101004000007bad8501cefa406cba0fa6c3cde8fb562f892d072203505ffe98f19bcce32ee745565478015a7a727fb687de8c710739a41481cd137503b0341cae1218da40,
            ROW(173, 1, 168009204, CAST('9284579784594529233' AS UINT256), CAST('327885090305647464340472349599389424070' AS UINT256), CAST('42430022509486004761842093696456834131842743534333780353852533344986326118836' AS UINT256)),
            0
        ),
        -- -- random values with random offset
        (
            0xb5ee9c7201010101004900008df21bb9bce6ba70f6be56c280e77d20365d07d361e6f47dab17c496839101a82fff4c78cde6719773a2ab2a3c00ad3d393fdb43ef4638839cd20a40e689ba81d81a0e57090c6d20,
            ROW(173, 1, 168009204, CAST('9284579784594529233' AS UINT256), CAST('327885090305647464340472349599389424070' AS UINT256), CAST('42430022509486004761842093696456834131842743534333780353852533344986326118836' AS UINT256)),
            73
        )
    )
    AS temp (boc, expected, offset)
), test_results AS (    
    SELECT {{ ton_from_boc('boc', [
    ton_begin_parse(),
    ton_skip_bits('offset'),
    ton_load_uint(8, as='uint8'),
    ton_load_uint(1, as='uint1'),
    ton_load_uint(32, as='uint32'),
    ton_load_uint(64, as='uint64'),
    ton_load_uint(128, as='uint128'), 
    ton_load_uint(256, as='uint256')
    ]) }} as result, expected 
    FROM test_data
)
SELECT result, expected FROM test_results
WHERE result != expected