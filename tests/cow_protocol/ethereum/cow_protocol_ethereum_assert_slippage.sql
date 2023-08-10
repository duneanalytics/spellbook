WITH test_1 AS
(
    SELECT * FROM {{ ref('cow_protocol_ethereum_trade_slippage') }}
    WHERE order_uid = 0x514a80473dc24034a1983ec831603f2f100ad7defd1578b077c637f73f3b92ecffab14b181409170378471b13ff2bff5be012c646434b605
    AND block_number = 16975171
    AND (
        abs(amount_percentage) > 3.3
       OR
        abs(amount_atoms) > 10471539
       OR
        abs(amount_usd) > 10.5
    )
)

SELECT * FROM (
    SELECT * FROM test_1
)