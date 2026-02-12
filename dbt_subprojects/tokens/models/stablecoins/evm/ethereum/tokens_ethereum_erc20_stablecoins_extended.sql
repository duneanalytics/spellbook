{% set chain = 'ethereum' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_extended',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- extended list: new stablecoin addresses added after the core list was frozen
-- add new stablecoins here (not in tokens_ethereum_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x337e7456b420bd3481e7fa61fa9850343d610d34, 'MXN'), -- wMXN
     (0xd76f5faf6888e24d9f04bf92a0c8b921fe4390e0, 'BRL'), -- wBRL
     (0x07041776f5007ACa2A54844F50503a18A72A8b68, 'USD'), -- USAT
     (0xd687759f35bb747a29246a4b9495c8f52c49e00c, 'AUD')  -- AUDX

     /* yield-bearing / rebasing tokens
     (0x96f6ef951840721adbf46ac996b59e0235cb985c, 'USD'), -- USDY
     (0xd46ba6d942050d489dbd938a2c909a5d5039a161, 'USD'), -- AMPL
     (0x7712c34205737192402172409a8f7ccef8aa2aec, 'USD'), -- BUIDL
     (0x2a8e1e676ec238d8a992307b495b45b3feaa5e86, 'USD'), -- OUSD
     (0xa774ffb4af6b0a91331c084e1aebae6ad535e6f3, 'USD'), -- FLEXUSD (list: flexUSD)
     (0x15700b564ca08d9439c58ca5053166e8317aa138, 'USD'), -- deUSD
     (0x79c58f70905f734641735bc61e45c19dd9ad60bc, 'USD'), -- usdc-dai-usdt (list: USDC-DAI-USDT)
     (0x59d9356e565ab3a36dd77763fc0d87feaf85508c, 'USD'), -- USDM
     (0xbbaec992fc2d637151daf40451f160bf85f3c8c1, 'USD'), -- USDM
     (0x7751e2f4b8ae93ef6b79d86419d42fe3295a4559, 'USD'), -- wUSDL
     (0xbdc7c08592ee4aa51d06c27ee23d5087d65adbcd, 'USD'), -- USDL
     (0x57f5e098cad7a3d1eed53991d4d66c45c9af7812, 'USD'), -- wUSDM
     (0x7c1156e515aa1a2e851674120074968c905aaf37, 'USD'), -- lvlUSD
     */

     /* rebasing / interest accruing tokens
     (0x9EEAD9ce15383CaEED975427340b3A369410CFBF), -- aUSDT (aave)
     (0x9d39a5de30e57443bff2a8307a4256c8797a3497), -- sUSDe (staked USDe)
     (0x98c23e9d8f34fefb1b7bd6a91b7ff122f4e16f5c), -- aEthUSDC (aave)
     (0x23878914efe38d27c4d67ab83ed1b93a74d4086a), -- aEthUSDT (aave)
     (0xad55aebc9b8c03fc43cd9f62260391c13c23e7c0), -- cUSDO (lending)
     (0x3d7d6fdf07ee548b939a80edbc9b2256d0cdc003), -- srUSDe (staked restaked USDe)
     (0xa3931d71877c0e7a3148cb7eb4463524fec27fbd), -- sUSDS (savings USDS)
     (0x48f9e38f3070ad8945dfeae3fa70987722e3d89c), -- iUSD (iron bank)
     (0xdcd0f5ab30856f28385f641580bbd85f88349124)  -- alUSD (alchemix)
     */

) as temp_table (contract_address, currency)
