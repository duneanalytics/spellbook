WITH test_data AS (
    SELECT boc, CAST(expected AS ROW(opcode bigint, owner_address varchar, sender_address varchar, recipient_address varchar, _current_time bigint, asset_id UINT256)) as expected
    FROM (
        VALUES
        (
            0xb5ee9c724101030100c20002d302801461866e4c8d6f4cb3a4f38a5963bbaf642181d6497781061eda610e1cc382a69001d8c4d89eadd151ddb14b151ee561dce8fb93212ecaf6886b1e99f4e95be1ac6e00518619b93235bd32ce93ce29658eeebd9086075925de04187b698438730e0a9a33e80031c00102000000a0ca9006bd3fb03d355daeeff93b24be90afaa6e3ca0073ff5720f8a852c9332780000000000a7d8c0000000000267ff0b000001ba6e4e3c68000000de501fd306000000ee6ad31fa2000000f64966b700a02a46ea,
            ROW(
                2,
                '0:A30C3372646B7A659D279C52CB1DDD7B210C0EB24BBC0830F6D30870E61C1534',
                '0:76313627AB7454776C52C547B958773A3EE4C84BB2BDA21AC7A67D3A56F86B1B',
                '0:A30C3372646B7A659D279C52CB1DDD7B210C0EB24BBC0830F6D30870E61C1534',
                1741684835,
                '91621667903763073563570557639433445791506232618002614896981036659302854767224'
            )
        )
    )
    AS temp (boc, expected)
), test_results AS (    
    SELECT {{ ton_from_boc('boc', [
    ton_begin_parse(),
    ton_load_uint(8, 'opcode'),
    ton_load_address('owner_address'),
    ton_load_address('sender_address'),
    ton_load_address('recipient_address'),
    ton_load_uint(32, '_current_time'),
    ton_skip_refs(1),
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_uint(256, 'asset_id')
    ]) }} as result, expected 
    FROM test_data
)
SELECT result, expected FROM test_results
WHERE result != expected