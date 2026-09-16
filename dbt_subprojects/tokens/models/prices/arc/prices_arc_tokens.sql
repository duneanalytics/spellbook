{% set blockchain = 'arc' %}

{{ config(
    schema = 'prices_' + blockchain,
    alias = 'tokens',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static']
    )
}}

-- The native gas token (USDC at 0x0000...0000, 18 decimals) is intentionally absent here:
-- it is registered in prices_native_tokens.sql, which is where prices.tokens picks native
-- assets up. Listing it again would duplicate rows in the prices.day/hour/minute pipelines.
--
-- Two groups below:
--
-- 1. Canonical Circle assets: USDC's 6-decimal ERC-20 interface at 0x3600...0000 (the same
--    balance as the native token, exposed through a standard ERC-20 view) and EURC.
--
-- 2. Wrapped-asset substitutions (Animus / Wirex family). These are priced off the
--    underlying asset's CoinPaprika feed rather than a feed of their own, which is an
--    established pattern in this registry -- usdc-usd-coin alone already serves 75 contracts
--    across 20 symbols, including aTokens and bridged variants. The substitution ASSUMES a
--    1:1 peg to the underlying and is PENDING ISSUER CONFIRMATION of that peg; if a wrapper
--    turns out to float against its underlying, its row here should be dropped rather than
--    repriced.
--
-- Deliberately not registered, because no defensible feed exists for them:
--   AWORP      0x26d1ffbbb8b310b090ee0536748b4adfc88ae644
--   CRCL       0x2ba0f44bdfc17fba30eda9cdbecb908ca45b043b
--   Architects 0x8bcb94279fc2c984ec34e0c1f2192df8c69ea4f0

SELECT
    token_id
    , '{{ blockchain }}' as blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES
    -- canonical Circle assets
      ('usdc-usd-coin', 'USDC', 0x3600000000000000000000000000000000000000, 6)
    , ('euroc-euro-coin', 'EURC', 0x89b50855aa3be2f677cd6303cec089b5f319d72a, 6)
    -- assets listed on Aave v4's Arc deployment. Addresses read from the
    -- protocol's AddAsset events on the Core hub, decimals from the same event.
    -- cirBTC has no feed of its own and is priced off wbtc, carrying the same
    -- 1:1 peg assumption as the substitutions below.
    , ('euroc-euro-coin', 'EURC', 0xbef5f6d51cb62b58e6a8f77868681825c6fe21c1, 6)
    , ('weth-weth', 'WETH', 0x128cc466b61f542da60c70e3aa11c10e19b84edb, 18)
    , ('wbtc-wrapped-bitcoin', 'cirBTC', 0x171a4217b86a807a64eb94757db6849fb4bdbaa0, 8)
    -- wrapper -> underlying substitutions (1:1 peg assumed, pending issuer confirmation)
    , ('usdc-usd-coin', 'AUSD', 0xf5b08979251f398180385b54381ee3d6fa1bbe09, 18)
    , ('euroc-euro-coin', 'AEUR', 0x8cd7e5a2240a1a7efaa9b164caa1dc80e9ed23a3, 18)
    , ('wbtc-wrapped-bitcoin', 'ABTC', 0x7ce5e3fb080545c8912cf93297d93441911e9e4d, 18)
    , ('eth-ethereum', 'AETH', 0x932fcc663ebf33d1e0e8ae27469b3b26f90a02ac, 18)
    , ('trx-tron', 'ATRX', 0xfdd489aa05b452f2b042f34d12569dbe76fa1e61, 18)
    , ('gbp-pound-sterling-token', 'AGBP', 0xa073783b43dfbfa2a78e0ae015a82968d816f41a, 18)
    , ('wxt-wirex-token', 'AWXT', 0x04adf55844be2f4c8d23e3f5f2386b08400b0cd1, 18)
) as temp (token_id, symbol, contract_address, decimals)
