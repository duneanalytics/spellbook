{{ config(
    alias = 'itokens',
    post_hook='{{ expose_spells(\'["optimism"]\',
                            "project",
                            "ironbank",
                            \'["michael-ironbank"]\') }}'
) }}

SELECT
    symbol, 
    contract_address, 
    decimals, 
    underlying_token_address, 
    underlying_decimals, 
    underlying_symbol
FROM
    (
        VALUES
        ('iWETH', '0x17533a1bDe957979E3977EbbFBC31E6deeb25C7d', 8, '0x4200000000000000000000000000000000000006', 18, 'WETH'),
        ('iUSDC', '0x1d073cf59Ae0C169cbc58B6fdD518822ae89173a', 8, '0x7F5c764cBc14f9669B88837ca1490cCa17c31607', 6, 'USDC'),
        ('iUSDT', '0x874C01c2d1767EFA01Fa54b2Ac16be96fAd5a742', 8, '0x94b008aA00579c1307B0EF2c499aD98a8ce58e58', 6, 'USDT'),
        ('iDAI', '0x049E04bEE77cFfB055f733A138a2F204D3750283', 8, '0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1', 18, 'DAI'),
        ('iWBTC', '0xcdb9b4db65C913aB000b40204248C8A53185D14D', 8, '0x68f180fcCe6836688e9084f035309E29Bf0A2095', 8, 'WBTC'),
        ('iOP', '0x4645e0952678E9566FB529D9313f5730E4e1C412', 8, '0x4200000000000000000000000000000000000042', 18, 'OP'),
        ('iSNX', '0xE724FfA5D30782499086682C8362CB3673bF69ae', 8, '0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4', 18, 'SNX'),
        ('iSUSD', '0x04F0fd3CD03B17a3E5921c0170ca6dD3952841cA', 8, '0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9', 18, 'sUSD')
    ) AS temp_table (
        symbol, 
        contract_address, 
        decimals, 
        underlying_token_address, 
        underlying_decimals, 
        underlying_symbol
    )
