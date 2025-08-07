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
) as temp (token_id, symbol, contract_address, decimals)