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
    ('stg-stargatetoken', 'mantle', 'STG', '0x8731d54e9d02c286767d56ac03e8037c07e01e98', 18),
    ('pendle-pendle', 'mantle', 'PENDLE', '0xd27b18915e7acc8fd6ac75db6766a80f8d2f5729', 18),
    ('usdc-usd-coin', 'mantle', 'USDC', '0x09bc4e0d864854c6afb6eb9a9cdf58ac190d0df9', 6), -- authentic one
--     ('wbtc-wrapped-bitcoin', 'mantle', 'WBTC', '0xcabae6f6ea1ecab08ad02fe02ce9a44f09aebfa2', 8), not sure about authenticity
    ('ena-ena', 'mantle', 'ENA', '0x58538e6a46e07434d7e7375bc268d3cb839c0133', 18),
    ('weth-weth', 'mantle', 'WETH', '0xdeaddeaddeaddeaddeaddeaddeaddeaddead1111', 18), -- authentic one
    ('lend-lend', 'mantle', 'LEND', '0x25356aeca4210ef7553140edb9b8026089e49396', 18),
--     ('pepe-pepe', 'mantle', 'PEPE', '0x2cb76c8949c7b7fae6dc0614c3b1bfd435f9a0bc', 18),
    ('meth-meth', 'mantle', 'mETH', '0xcda86a272531e8640cd7f1a92c01839911b90bb0', 18), -- authentic one
--     ('lusd-lusd', 'mantle', 'LUSD', '0xf93a85d53e4af0d62bdf3a83ccfc1ecf3eaf9f32', 18), not sure about authenticity
--     ('wsteth-wrapped-steth', 'mantle', 'wstETH', '0x636d4073738c071326aa70c9e5db7c334beb87be', 18), not sure about authenticity
    ('usdt-tether', 'mantle', 'USDT', '0x201eba5cc46d216ce6dc03f6a759e8e766e956ae', 6) -- authentic one
--     ('usde-ethena-usde', 'mantle', 'USDe', '0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34', 18) not sure about authenticity
) as temp (token_id, blockchain, symbol, contract_address, decimals)
