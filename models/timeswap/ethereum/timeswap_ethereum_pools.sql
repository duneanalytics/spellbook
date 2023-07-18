{{ config(
    alias = alias('pools'),
    tags = ['dunesql'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "timeswap",
                                \'["raveena15, varunhawk19"]\') }}',
    unique_key = ['pool_pair', 'maturity', 'strike']
    )
}}

SELECT
    token0_symbol,
    token1_symbol,
     token0_address  as token0_address,
     token1_address  as token1_address,
    token0_decimals,
    token1_decimals,
    strike,
    maturity,
    pool_pair,
    chain,
     borrow_contract_address  as borrow_contract_address,
     lend_contract_address  as lend_contract_address
FROM
    (
        VALUES
            (
                'BLUR',
                'USDC',
                0x5283d291dbcf85356a21ba090e6db59121208b44,
                0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,
                18,
                6,
                '102084710076281539039012382',
                '1679659200',
                'BLUR-USDC',
                'Ethereum',
                0x28d0591275863d5d8ed33ea30a8ab58c351155a9,
                0x5073657c5459a6bcb66a769ad9687d2576630f53
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