{{ config(
    schema = 'timeswap_polygon'
    ,alias = 'pools'
    ,unique_key = ['pool_pair', 'maturity', 'strike']
    ,post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "timeswap",
                                \'["raveena15, varunhawk19"]\') }}'
    )
}}

SELECT
    token0_symbol,
    token1_symbol,
    token0_address,
    token1_address,
    token0_decimals,
    token1_decimals,
    strike,
    maturity,
    pool_pair,
    chain,
    borrow_contract_address,
    lend_contract_address
FROM
    (
        VALUES
            (
                'USDC',
                'WETH',
                0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174,
                0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619,
                6,
                18,
                CAST('237490072735998707284173473430675352952919527482' AS UINT256),
                CAST('1677513600' AS UINT256),
                'USDC-WETH',
                'Polygon',
                0x7f7B71d60027aEE523FFfac9cC17ebF7915f1d02,
                0xb0EB735B0246bB11Fb57DB28DF5370D72C153314
            ),
            (
                'stMATIC', 
                'MIMATIC', 
                0x3A58a54C066FdC0f2D55FC9C89F0415C92eBf3C4,
                0xa3Fa99A148fA48D14Ed51d610c367C61876997F1,
                18,
                18,
                CAST('340282366920938463463374607431768211456' AS UINT256),
                CAST('1679918400' AS UINT256),
                'stMATIC-MIMATIC', 
                'Polygon',
                0x477C3AB5c9811e87dEE1fBd43D5357427B749b21,
                0x1a3a964BBa5e918f9630C26D1f772021B0e03CAc
            ),
            (
                'USDC', 
                'USDT', 
                0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174,
                0xc2132D05D31c914a87C6611C10748AEb04B58e8F,
                6,
                6,
                CAST('279031540875169540039967178094049933393' AS UINT256),
                CAST('1678795200' AS UINT256), 
                'USDC-USDT',
                'Polygon',
                0xe14f43397584d138315C113eF6A82010ab84B701,
                0xAb30A405C28fb1c1466f5BFa03BdBa63e1b36584
            ),
            (
                'USDC', 
                'stMATIC', 
                0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174,
                0x3A58a54C066FdC0f2D55FC9C89F0415C92eBf3C4,
                6,
                18,
                CAST('170141183460469231731687303715884105728000000000000' AS UINT256),
                CAST('1682337600' AS UINT256), 
                'USDC-stMATIC', 
                'Polygon',
                0x990A70F9a33d748C90Dd05c302dAFfd3FFFAA2a7,
                0x36b34aB7538061089700Ba976BA7e88d7ad500e9
            ),
            (
                'USDC',
                'QUICK', 
                0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174,
                0xB5C064F955D8e7F38fE0460C556a72987494eE17,
                6,
                18,
                CAST('2617556668622603565102881595628986241969230769230769' AS UINT256),
                CAST('1684929600' AS UINT256), 
                'USDC-QUICK',
                'Polygon',
                0xb9385AfC6Ddf565C0256116Aa3415EfdFca1E872,
                0x880D3fc39683Ecbfd7636cc48D5FCc34508ca7c3
            ),
            (
                'USDC',
                'dQUICK',
                0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174,
                0x958d208Cdf087843e9AD98d23823d32E17d723A1,
                6,
                18,
                CAST('3402823669209384634633746074317682114560000000000000' AS UINT256),
                CAST('1687521600' AS UINT256),
                'USDC-dQUICK',
                'Polygon',
                0xb9385AfC6Ddf565C0256116Aa3415EfdFca1E872,
                0x880D3fc39683Ecbfd7636cc48D5FCc34508ca7c3
            ),
            (
                'USDC',
                'stMATIC',
                0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174,
                0x3A58a54C066FdC0f2D55FC9C89F0415C92eBf3C4,
                6,
                18,
                CAST('261755666862260356510288159562898624196923076923076' AS UINT256),
                CAST('1688126400' AS UINT256),
                'USDC-stMATIC',
                'Polygon',
                0xb9385AfC6Ddf565C0256116Aa3415EfdFca1E872,
                0x880D3fc39683Ecbfd7636cc48D5FCc34508ca7c3
            ),
            (
                'USDC',
                'QUICK',
                0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174,
                0xB5C064F955D8e7F38fE0460C556a72987494eE17,
                6,
                18,
                CAST('4253529586511730793292182592897102643200000000000000' AS UINT256),
                CAST('1690113600' AS UINT256),
                'USDC-QUICK',
                'Polygon',
                0x9f024e0c98b598421c1f3170c94254c266870340,
                0xa68636df3019e56857ad5a1759d130b17746820f
            )

    ) AS temp_table (
        token0_symbol,
        token1_symbol,
        token0_address,
        token1_address,
        token0_decimals,
        token1_decimals,
        strike,
        maturity,
        pool_pair,
        chain,
        borrow_contract_address,
        lend_contract_address
    )
