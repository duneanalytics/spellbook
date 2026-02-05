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

select '{{chain}}' as blockchain, contract_address
from (values

     (0x337e7456b420bd3481e7fa61fa9850343d610d34), -- wMXN
     (0xd76f5faf6888e24d9f04bf92a0c8b921fe4390e0), -- wBRL
     (0x07041776f5007ACa2A54844F50503a18A72A8b68)  -- USAT

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

) as temp_table (contract_address)
