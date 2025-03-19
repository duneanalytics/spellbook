WITH test_data AS (
    SELECT boc, expected, offset
    FROM (
        VALUES
        -- all zeroes
        (
            0xb5ee9c7201010101004000007b0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000040,
            map_from_entries(ARRAY[
                ('uint1', CAST(CAST(0 AS int) AS JSON)),
                ('uint8', CAST(CAST(0 AS int) AS JSON)),
                ('uint32', CAST(CAST(0 AS int) AS JSON)),
                ('uint64', CAST(CAST(0 AS varchar) AS JSON)),
                ('uint128', CAST(CAST(0 AS varchar) AS JSON)),
                ('uint256', CAST(CAST(0 AS varchar) AS JSON))
                ]),
            0
        ),
        -- max value
        (
            0xb5ee9c7201010101004000007bffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffc0,
            map_from_entries(ARRAY[
                ('uint1', CAST(CAST(1 AS int) AS JSON)),
                ('uint8', CAST(CAST(255 AS int) AS JSON)),
                ('uint32', CAST(CAST(4294967295 AS bigint) AS JSON)),
                ('uint64', CAST(CAST('18446744073709551615' AS varchar) AS JSON)),
                ('uint128', CAST(CAST('340282366920938463463374607431768211455' AS varchar) AS JSON)),
                ('uint256', CAST(CAST('115792089237316195423570985008687907853269984665640564039457584007913129639935' AS varchar) AS JSON))
                ]),
            0
        ),
        -- random values
        (
            0xb5ee9c7201010101004000007bad8501cefa406cba0fa6c3cde8fb562f892d072203505ffe98f19bcce32ee745565478015a7a727fb687de8c710739a41481cd137503b0341cae1218da40,
            map_from_entries(ARRAY[
                ('uint1', CAST(CAST(1 AS int) AS JSON)),
                ('uint8', CAST(CAST(173 AS int) AS JSON)),
                ('uint32', CAST(CAST(168009204 AS bigint) AS JSON)),
                ('uint64', CAST(CAST('9284579784594529233' AS varchar) AS JSON)),
                ('uint128', CAST(CAST('327885090305647464340472349599389424070' AS varchar) AS JSON)),
                ('uint256', CAST(CAST('42430022509486004761842093696456834131842743534333780353852533344986326118836' AS varchar) AS JSON))
                ]),
            0
        ),
        -- random values with random offset
        (
            0xb5ee9c7201010101004900008df21bb9bce6ba70f6be56c280e77d20365d07d361e6f47dab17c496839101a82fff4c78cde6719773a2ab2a3c00ad3d393fdb43ef4638839cd20a40e689ba81d81a0e57090c6d20,
            map_from_entries(ARRAY[
                ('uint1', CAST(CAST(1 AS int) AS JSON)),
                ('uint8', CAST(CAST(173 AS int) AS JSON)),
                ('uint32', CAST(CAST(168009204 AS bigint) AS JSON)),
                ('uint64', CAST(CAST('9284579784594529233' AS varchar) AS JSON)),
                ('uint128', CAST(CAST('327885090305647464340472349599389424070' AS varchar) AS JSON)),
                ('uint256', CAST(CAST('42430022509486004761842093696456834131842743534333780353852533344986326118836' AS varchar) AS JSON))
                ]),
            73
        )
    )
    AS temp (boc, expected, offset)
), test_results AS (    
    SELECT {{ ton_from_boc('boc', [
    ton_begin_parse(),
    ton_skip_bits('offset'),
    ton_load_uint(8, 'uint8'),
    ton_load_uint(1, 'uint1'),
    ton_load_uint(32, 'uint32'),
    ton_load_uint(64, 'uint64'),
    ton_load_uint(128, 'uint128'), 
    ton_load_uint(256, 'uint256')
    ]) }} as result, expected 
    FROM test_data
)
SELECT json_format(CAST(result AS json)) AS result, json_format(CAST(expected AS json)) AS expected FROM test_results
WHERE result != expected