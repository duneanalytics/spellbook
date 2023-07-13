{{ config(
    alias = 'markets',
    post_hook='{{ expose_spells(\'["goerli"]\',
                                "project",
                                "ironbank_v2",
                                \'["michael-ironbank-v2"]\') }}'
) }}

SELECT
    market, 
    market_symbol,
    market_decimals,
    debt_token,
    debt_token_symbol,
    debt_token_decimals,
    ib_token,
    ib_token_symbol,
    ib_token_decimals
FROM
    (
        VALUES
            ('0xB4FBF271143F4FBf7B91A5ded31805e42b2208d6', 'WETH', 18, '0x66F373c3C8596Dd664902f0B237A626791351C32', 'debtWETH', 18, '0xE10AaAb9293074a506884d11FBEAf3026467db0a', 'ibWETH', 18),
            ('0x803dD5F52E0F3c82ba23034e09C641C6a08b42cf', 'wstETH', 18,'0x277946C1c2EA219974b21F9D86661D017bbAEb46', 'debtWSTETH', 18, '0x2a7E61e0fa3903B3273FC19944E631DC46aBAF6d', 'ibWSTETH', 18),
            ('0xd401D426f9CF3f711858b57B120d0e0692Ff0bd7', 'DAI', 18, '0x79CBfDCe75D0453A14fA0ff946Bd860FceD0DD87', 'debtDAI', 18, '0xBe6d67c09Eb495128E90E63be0F3274aDAA8ba6B', 'ibDAI', 18),
            ('0x20C91ea649987E46f1173bf796e43c151bcfD7E5', 'USDC', 6, '0xC8bB2a7838Ca250AcC6fF48D87b3874B1C96c800', 'debtUSDC', 6, '0x6CA7d129ed21BDCD629ACc960bfA899fe95289D3', 'ibUSDC', 18),
            ('0x72F59F9a754b2e66A17783F15914940c0DdCf97d', 'WBTC', 8, '0x4132A30eCb23c9d629D1638649b8118aC7d64e86', 'debtWBTC', 8, '0x3Fc8BAa64e52FF01308953d73C5C45b7814F4496', 'ibWBTC', 18),
            ('0xc12D4819Ae10dEb13A307c3DCe7C1dE69Fe99b5c', 'pWETH', 18, NULL, NULL, NULL, '0x66163069731aF075c60F2c63EB71b022195c4f55', 'ibPWETH', 18),
            ('0xE43745d7638EF60818C7402cecc5537c014B5B3A', 'pUSDC', 6, NULL, NULL, NULL, '0x8e5E761A4d4C029E215c7187174479D8A3dCB175', 'ibPUSDC', 18),
    ) AS temp_table (
        market, 
        market_symbol,
        market_decimals,
        debt_token,
        debt_token_symbol,
        debt_token_decimals,
        ib_token,
        ib_token_symbol,
        ib_token_decimals
    )
