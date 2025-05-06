WITH test_data AS (
    SELECT name, timestamp, token_address, expected_price_ton, expected_price_usd, decimals
    FROM (
        VALUES
        ('STON jetton', TIMESTAMP '2025-02-17', '0:3690254DC15B2297610CDA60744A45F2B710AA4234B89ADB630E99D79B01BD4F',  0.6919270, 2.62841, 1e9),
        ('USDT jetton', TIMESTAMP '2025-02-17', '0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE',  0.2635, 1, 1e6),

        ('TON-USDT LP', TIMESTAMP '2025-02-17', '0:8649CAD97B5C5BC96A960EF748EA6CCFF8601C01616FE995EE6893AE4AA7A6C6',  33.865402, 127.66943, 1e9),

        ('TON-SLP price', TIMESTAMP '2025-02-17', '0:8D636010DD90D8C0902AC7F9F397D8BD5E177F131EE2CCA24CE894F15D19CEEA',  1.17855, 4.4699248, 1e9),
        ('USDT-SLP price', TIMESTAMP '2025-02-17', '0:AEA78C710AE94270DC263A870CF47B4360F53CC5ED38E3DB502E9E9AFB904B11',  0.3239736, 1.229129, 1e9),
        ('NOT-SLP price', TIMESTAMP '2025-02-17', '0:2AB634CFCBDBE3B97503691E0780C3D07C9069210A2B24B991BA4F9941B453F9',  0.00094732, 0.00360335, 1e9)
    )
    AS temp (name, timestamp, token_address, expected_price_ton, expected_price_usd, decimals)
), test_results AS (    
    SELECT T.name, case when P.token_address IS NULL THEN 'Missing' ELSE 'Present' END as status,
    (ABS(P.price_ton * T.decimals - T.expected_price_ton) / T.expected_price_ton) as delta_ton,
    (ABS(P.price_usd * T.decimals - T.expected_price_usd) / T.expected_price_usd) as delta_usd
    FROM 
    {{ ref ('ton_jetton_price_daily') }} P
    RIGHT JOIN test_data T ON P.timestamp = T.timestamp AND P.token_address = T.token_address
)
SELECT name || ': delta_ton = ' || cast(coalesce(delta_ton, 0) as varchar) || ', delta_usd = ' || cast(coalesce(delta_usd, 0) as varchar) as name FROM test_results
WHERE status = 'Missing' OR delta_ton > 0.01 OR delta_usd > 0.01