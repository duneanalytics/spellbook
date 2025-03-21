WITH test_data AS (
    SELECT boc, CAST(expected AS ROW(address1 varchar, address2 varchar)) AS expected, offset
    FROM (
        VALUES
        -- addr_std twice
        (
            0xb5ee9c720101010100450000858002e0e4a72954ac26df7a4fa19c63b1531c1f15ce549a46f36bbbd07c42345abf50005c1c94e52a9584dbef49f4338c762a6383e2b9ca9348de6d777a0f88468b57ea,
            ROW(
                UPPER('0:170725394aa56136fbd27d0ce31d8a98e0f8ae72a4d2379b5dde83e211a2d5fa'),
                UPPER('0:170725394aa56136fbd27d0ce31d8a98e0f8ae72a4d2379b5dde83e211a2d5fa')
            ),
            0
        ),
        -- addr_std with random offset
        (
            0xb5ee9c7201010101004e000097f21bb9bce6ba70f6be400170725394aa56136fbd27d0ce31d8a98e0f8ae72a4d2379b5dde83e211a2d5fa8002e0e4a72954ac26df7a4fa19c63b1531c1f15ce549a46f36bbbd07c42345abf5,
            ROW(
                UPPER('0:170725394aa56136fbd27d0ce31d8a98e0f8ae72a4d2379b5dde83e211a2d5fa'),
                UPPER('0:170725394aa56136fbd27d0ce31d8a98e0f8ae72a4d2379b5dde83e211a2d5fa')
            ),
            73
        ),
        -- addr_none twice
        (
            0xb5ee9c7201010101000300000108,
            ROW(
                'addr_none',
                'addr_none'
            ),
            0
        ),
        -- addr_none + addr_std
        (
            0xb5ee9c720101010100240000432000b83929ca552b09b7de93e86718ec54c707c573952691bcdaeef41f108d16afd4,
            ROW(
                'addr_none',
                UPPER('0:170725394aa56136fbd27d0ce31d8a98e0f8ae72a4d2379b5dde83e211a2d5fa')
            ),
            0
        ),
        -- masterchain + workchain
        (
            0xb5ee9c720101010100450000859fe66666666666666666666666666666666666666666666666666666666666666670005c1c94e52a9584dbef49f4338c762a6383e2b9ca9348de6d777a0f88468b57ea,
            ROW(
                UPPER('-1:3333333333333333333333333333333333333333333333333333333333333333'),
                UPPER('0:170725394aa56136fbd27d0ce31d8a98e0f8ae72a4d2379b5dde83e211a2d5fa')
            ),
            0
        )
        -- TODO addr_std + anycast
        -- TODO addr_var
        -- TODO addr_extern
    )
    AS temp (boc, expected, offset)
), test_results AS (    
    SELECT {{ ton_from_boc('boc', [
    ton_begin_parse(),
    ton_skip_bits('offset'),
    ton_load_address(as='address1'),
    ton_load_address(as='address2')
    ]) }} as result, expected 
    FROM test_data
)
SELECT json_format(CAST(result AS json)) AS result, json_format(CAST(expected AS json)) AS expected FROM test_results
WHERE result != expected