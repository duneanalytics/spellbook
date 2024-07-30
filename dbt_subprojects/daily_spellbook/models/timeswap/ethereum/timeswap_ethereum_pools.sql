{{ config(
    schema = 'timeswap_ethereum'
    ,alias = 'pools'
    ,unique_key = ['pool_pair', 'maturity', 'strike']
    ,post_hook='{{ expose_spells(\'["ethereum"]\',
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
                'BLUR',
                'USDC',
                0x5283D291DBCF85356A21bA090E6db59121208b44,
                0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
                18,
                6,
                CAST('102084710076281539039012382' AS UINT256),
                CAST('1679659200' AS UINT256),
                'BLUR-USDC',
                'Ethereum',
                0x28D0591275863d5d8ED33Ea30a8Ab58C351155A9,
                0x5073657C5459a6BcB66a769Ad9687D2576630f53
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