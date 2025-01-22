{{ config(
        schema='prices_mantle',
        alias='tokens',
        materialized='table',
        file_format='delta',
        tags=['static']
        )
}}

SELECT
    token_id,
    blockchain,
    symbol,
    contract_address,
    decimals
FROM
(
    VALUES
    ('mnt-mantle', 'mantle', 'MNT', 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000, 18),
    ('mnt-mantle', 'mantle', 'WMNT', 0x78c1b0c915c4faa5fffa6cabf0219da63d7f4cb8, 18),
    ('joe-trader-joe', 'mantle', 'JOE', 0x371c7ec6d8039ff7933a2aa28eb827ffe1f52f07, 18),
    ('usdc-usd-coin', 'mantle', 'USDC', 0x09bc4e0d864854c6afb6eb9a9cdf58ac190d0df9, 6),
    ('weth-weth', 'mantle', 'WETH', 0xdeaddeaddeaddeaddeaddeaddeaddeaddead1111, 18),
    ('meth-meth', 'mantle', 'mETH', 0xcda86a272531e8640cd7f1a92c01839911b90bb0, 18),
    ('usdt-tether', 'mantle', 'USDT', 0x201eba5cc46d216ce6dc03f6a759e8e766e956ae, 6),
    ('axlusdc-axelar-wrapped-usdc', 'mantle', 'axlUSDC', 0xeb466342c4d449bc9f53a865d5cb90586f405215, 6),
    ('stg-stargatetoken', 'mantle', 'STG', 0x8731d54e9d02c286767d56ac03e8037c07e01e98, 18),
    ('pendle-pendle', 'mantle', 'PENDLE', 0xd27b18915e7acc8fd6ac75db6766a80f8d2f5729, 18),
    ('ena-ethena', 'mantle', 'ENA', 0x58538e6a46e07434d7e7375bc268d3cb839c0133, 18),
    ('usde-ethena-usde', 'mantle', 'USDe', 0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34, 18),
    ('puff-puff-token', 'mantle', 'PUFF', 0x26a6b0dcdcfb981362afa56d581e4a7dba3be140, 18),
    ('moe-merchant-moe', 'mantle', 'MOE', 0x4515A45337F461A11Ff0FE8aBF3c606AE5dC00c9, 18),
    ('usdy-ondo-us-dollar-yield', 'mantle', 'USDY', 0x5bE26527e817998A7206475496fDE1E68957c5A6, 18),
    ('svl-slash-vision-labs', 'mantle', 'SVL', 0xabbeed1d173541e0546b38b1c0394975be200000, 18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
