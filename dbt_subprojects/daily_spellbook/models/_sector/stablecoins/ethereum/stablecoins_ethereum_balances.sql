{{
  config(
    schema = 'stablecoins_ethereum',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'address', 'token_address', 'blockchain'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

-- trigger CI

with
stablecoin_tokens_total as (
  select distinct
    symbol,
    contract_address as token_address
  from 
    {{ source('tokens_ethereum', 'erc20_stablecoins')}}
)

, filter_tokens as (
  select * from (VALUES

    ('ethereum', 0x956f47f50a910163d8bf957cf5846d573e7f87ca, 'Algorithmic stablecoin', 'FEI', 18 ),
    ('ethereum', 0xbc6da0fe9ad5f3b0d58160288917aa56653660e9, 'Crypto-backed stablecoin', 'alUSD', 18 ),
    ('ethereum', 0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f, 'Crypto-backed stablecoin', 'GHO', 18 ),
    ('ethereum', 0x085780639cc2cacd35e474e71f4d000e2405d8f6, 'Crypto-backed stablecoin', 'fxUSD', 18 ),
    ('ethereum', 0x9d39a5de30e57443bff2a8307a4256c8797a3497, 'Crypto-backed stablecoin', 'sUSDe', 18 ),
    ('ethereum', 0xfd03723a9a3abe0562451496a9a394d2c4bad4ab, 'Crypto-backed stablecoin', 'DYAD', 18 ),
    ('ethereum', 0xa47c8bf37f92abed4a126bda807a7b7498661acd, 'Algorithmic stablecoin', 'UST', 18 ),
    ('ethereum', 0x73a15fed60bf67631dc6cd7bc5b6e8da8190acf5, 'Fiat-backed stablecoin', 'USD0', 18 ),
    ('ethereum', 0x056fd409e1d7a124bd7017459dfea2f387b6d5cd, 'Fiat-backed stablecoin', 'GUSD', 2 ),
    ('ethereum', 0x57ab1ec28d129707052df4df418d58a2d46d5f51, 'Crypto-backed stablecoin', 'sUSD', 18 ),
    ('ethereum', 0x865377367054516e17014ccded1e7d814edc9ce4, 'Crypto-backed stablecoin', 'DOLA', 18 ),
    ('ethereum', 0x6c3ea9036406852006290770bedfcaba0e23a0e8, 'Fiat-backed stablecoin', 'PYUSD', 6 ),
    ('ethereum', 0x35d8949372d46b7a3d5a56006ae77b215fc69bc0, 'Crypto-backed stablecoin', 'USD0++', 18 ),
    ('ethereum', 0x4c9edd5852cd905f086c759e8383e09bff1e68b3, 'Crypto-backed stablecoin', 'USDe', 18 ),
    ('ethereum', 0x8e870d67f660d95d5be530380d0ec0bd388289e1, 'Fiat-backed stablecoin', 'USDP', 18 ),
    ('ethereum', 0x674c6ad92fd080e4004b2312b45f796a192d27a0, 'Algorithmic stablecoin', 'USDN', 18 ),
    ('ethereum', 0xa693b19d2931d498c5b318df961919bb4aee87a5, 'Crypto-backed stablecoin', 'UST', 6 ),
    ('ethereum', 0x1456688345527be1f37e9e627da0837d6f08c925, 'Crypto-backed stablecoin', 'USDP', 18 ),
    ('ethereum', 0x6b175474e89094c44da98b954eedeac495271d0f, 'Hybrid stablecoin', 'DAI', 18 ),
    ('ethereum', 0x0000206329b97db379d5e1bf586bbdb969c63274, 'Crypto-backed stablecoin', 'USDA', 18 ),
    ('ethereum', 0x59d9356e565ab3a36dd77763fc0d87feaf85508c, 'Crypto-backed stablecoin', 'USDM', 18 ),
    ('ethereum', 0xbbaec992fc2d637151daf40451f160bf85f3c8c1, 'Crypto-backed stablecoin', 'USDM', 6 ),
    ('ethereum', 0x4fabb145d64652a948d72533023f6e7a623c7c53, 'Fiat-backed stablecoin', 'BUSD', 18 ),
    ('ethereum', 0x2a8e1e676ec238d8a992307b495b45b3feaa5e86, 'Crypto-backed stablecoin', 'OUSD', 18 ),
    ('ethereum', 0xc5f0f7b66764f6ec8c8dff7ba683102295e16409, 'Fiat-backed stablecoin', 'FDUSD', 18 ),
    ('ethereum', 0xf939e0a03fb07f59a73314e73794be0e57ac1b4e, 'Crypto-backed stablecoin', 'crvUSD', 18 ),
    ('ethereum', 0xdf574c24545e5ffecb9a659c229253d4111d87e1, 'Fiat-backed stablecoin', 'HUSD', 8 ),
    ('ethereum', 0x7712c34205737192402172409a8f7ccef8aa2aec, 'Fiat-backed stablecoin', 'BUIDL', 6 ),
    ('ethereum', 0xe2f2a5c287993345a840db3b0845fbc70f5935a5, 'Crypto-backed stablecoin', 'mUSD', 18 ),
    ('ethereum', 0x4591dbff62656e7859afe5e45f6f47d3669fbb28, 'Crypto-backed stablecoin', 'mkUSD', 18 ),
    ('ethereum', 0x5f98805a4e8be255a32880fdec7f6728c6568ba0, 'Crypto-backed stablecoin', 'LUSD', 18 ),
    ('ethereum', 0xdc035d45d973e3ec169d2276ddab16f1e407384f, 'Crypto-backed stablecoin', 'USDS', 18 ),
    ('ethereum', 0x15700b564ca08d9439c58ca5053166e8317aa138, 'Crypto-backed stablecoin', 'deUSD', 18 ),
    ('ethereum', 0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3, 'Crypto-backed stablecoin', 'MIM', 18 ),
    ('ethereum', 0x0000000000085d4780b73119b644ae5ecd22b376, 'Fiat-backed stablecoin', 'TUSD', 18 ),
    ('ethereum', 0x853d955acef822db058eb8505911ed77f175b99e, 'Hybrid stablecoin', 'FRAX', 18 ),
    ('ethereum', 0xd46ba6d942050d489dbd938a2c909a5d5039a161, 'Crypto-backed stablecoin', 'AMPL', 9 ),
    ('ethereum', 0x03ab458634910aad20ef5f1c8ee96f1d6ac54919, 'Crypto-backed stablecoin', 'RAI', 18 ),
    ('ethereum', 0x96f6ef951840721adbf46ac996b59e0235cb985c, 'Fiat-backed stablecoin', 'USDY', 18 ),
    ('ethereum', 0xdac17f958d2ee523a2206206994597c13d831ec7, 'Fiat-backed stablecoin', 'USDT', 6 ),
    ('ethereum', 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, 'Fiat-backed stablecoin', 'USDC', 6 )
    ) as t(blockchain, contract_address, backing, symbol, decimal)

)

, stablecoin_tokens as (
  select distinct st.symbol, st.token_address from stablecoin_tokens_total st 
  inner join filter_tokens ft on st.token_address = ft.contract_address
)

,balances as (
    {{
      balances_incremental_subset_daily(
            blockchain = 'ethereum',
            token_list = 'stablecoin_tokens',
            start_date = '2023-01-01'
      )
    }}
)

select
    t.symbol
    ,b.*
from balances b
left join stablecoin_tokens t
    on b.token_address = t.token_address
 