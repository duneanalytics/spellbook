{% set blockchain = 'tac' %}

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
    ('tac-tac-protocol', 'TAC', 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, 18)
    , ('toncoin-the-open-network', 'TON', 0xb76d91340F5CE3577f0a056D29f6e3Eb4E88B140, 9)
    , ('weth-weth', 'WETH', 0x61D66bC21fED820938021B06e9b2291f3FB91945, 18)
    , ('wsteth-wrapped-liquid-staked-ether-20', 'wstETH', 0xAf368c91793CB22739386DFCbBb2F1A9e4bCBeBf, 18)
    , ('cbbtc-coinbase-wrapped-btc', 'cbBTC', 0x7048c9e4aBD0cf0219E95a17A8C6908dfC4f0Ee4, 8)
    , ('usdt-tether', 'USD₮', 0xAF988C3f7CB2AceAbB15f96b19388a259b6C438f, 6)
    , ('wif-dogwifcoin', 'WIF', 0x27e4Ade13d78Aad45bea31D448f5504031e4871E, 18)
    , ('lbtc-lombard-staked-btc', 'LBTC', 0xecAc9C5F704e954931349Da37F60E39f515c11c1, 8)
    , ('usd0-usd0-liquid-bond', 'USD0++', 0x1791BAff6a5e2F2A1340e8B7C1EA2B0c1E2DD1ea, 18)
    , ('usr-resolv-usr', 'USR', 0xb1b385542B6E80F77B94393Ba8342c3Af699f15c, 18)
    , ('rlp-resolv-rlp', 'RLP', 0x35533f54740F1F1aA4179E57bA37039dfa16868B, 18)
    , ('m-btc-merlins-seal-btc', 'M-BTC', 0xe82dbD543FD729418613d68Cd1E8FC67b0f46E31, 18)
    , ('unibtc-universal-btc', 'uniBTC', 0xf9775085d726e782e83585033b58606f7731ab18, 8)
    , ('wrseth-wrapped-rseth', 'wrsETH', 0x5448bbf60ee2edbcd32f032f3294982f4ad1119e, 18)
    , ('usdc-usd-coin', 'USBD', 0x6bede1c6009a78c222d9bdb7974bb67847fdb68c, 18)
    , ('toncoin-the-open-network', 'tacTON' , 0x450c6baa2c0bc5328a461771bc32e01ba41f31ae, 9)
    , ('unibtc-universal-btc', 'satUniBTC', 0x4cbe838e2bd3b46247f80519b6ac79363298aa09, 8)
    , ('usdt-tether', 'Re7USDT', 0x4183Bd253Dc1918A04Bd8a8dD546BaAD58898109, 18)
    , ('toncoin-the-open-network', 'RE7TON', 0x84BBc0be5a6f831a4E2C28a2F3b892C70AcAa5b3, 18)
    , ('eth-ethereum', '9SETH', 0x26f38a9eAf377744296A907a9226066315B9147f, 18)
    , ('weth-weth', 'Re7WETH', 0xC5e1bD2473811bB782326006A3c03477F7834D35, 18)
    , ('usdt-tether', 'MC_USDT', 0x9A5411e72fe645d4A6a177568BEE94c9AE6aa102, 18)
    , ('usdt-tether', 'edgeUSDT', 0x9A057627f023f5C37Ebc6E7959720848968d7a43, 18)
    , ('cbbtc-coinbase-wrapped-btc', 'Re7cbBTC', 0xf49f14Cff1bA2eE7E23222A76e0C2b3D0BDE06dC, 18)
    , ('lbtc-lombard-staked-btc', 'Re7LBTC', 0xe9BD3590A68939344953b4f912d83b7c8C2A1f77, 18)
    , ('pufeth-pufeth', 'pufETH', 0x37D6382B6889cCeF8d6871A8b60E667115eDDBcF, 18)
    , ('tac-tac-protocol', 'WTAC', 0xb63b9f0eb4a6e6f191529d71d4d88cc8900df2c9, 18)
) as temp (token_id, symbol, contract_address, decimals)