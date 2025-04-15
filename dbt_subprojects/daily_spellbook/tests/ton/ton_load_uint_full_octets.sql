{#
 Regression test for a bug in ton_load_uint with wrong read block size when cursor_bit_offset is divisible by 8
#}
WITH test_data AS (
    SELECT boc, CAST(expected AS ROW(asset_id UINT256, amount_supplied bigint)) as expected
    FROM (
        VALUES
        (0xb5ee9c720101030100a000028f018018a55e87f2fc38b256be182294f726090191330299522563eb69a8b87cf076ef9002f1b45f414628689be481c091fc68a261d39a885d3da8112cc0209335ba9c313d9f6f8802010200a0ca9006bd3fb03d355daeeff93b24be90afaa6e3ca0073ff5720f8a852c93327800000000004c728a0000000000000000000005faf5b5dec5000003095110dff0000000c71694d3e3000000d3a358f55f0000,
        ROW(CAST('91621667903763073563570557639433445791506232618002614896981036659302854767224' AS UINT256), 5010058)
        )
    ) AS temp (boc, expected)
), test_results AS (    
    SELECT {{ ton_from_boc('boc', [
    ton_begin_parse(),
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_uint(256, 'asset_id'),
    ton_load_uint(64, 'amount_supplied')
    ]) }} as result, expected 
    FROM test_data
)
SELECT result, expected FROM test_results
WHERE result != expected