{% set blockchain = 'cronos' %}

{{ config(
    schema = 'prices_' + blockchain,
    alias = 'tokens',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static']
    )
}}

SELECT
    token_id
    , '{{ blockchain }}' as blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES
    ('wcro-wrapped-cro', 'WCRO', 0x5C7F8A570d578ED84E63fdFA7b1eE72dEae1AE23, 18)
    , ('vvs-vvs-finance', 'VVS', 0x2D03bECE6747ADc00E1a131BBA1469C15fD11e03, 18)
    , ('usdc-usd-coin', 'USDC', 0xc21223249CA28397B4B6541dfFaEcC539BfF0c59, 6)
    , ('usdt-tether', 'USDT', 0x66e428c3f67a68878562e79A0234c1F83c208770, 6)
    , ('dai-dai', 'DAI', 0xF2001B145b43032AAF5Ee2884e456CCd805F677D, 18)
    , ('weth-weth', 'WETH', 0xe44Fd7fCb2b1581822D0c862B68222998a0c299a, 18)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 0x062E66477Faf219F25D27dCED647BF57C3107d52, 8)
) as temp (token_id, symbol, contract_address, decimals)
