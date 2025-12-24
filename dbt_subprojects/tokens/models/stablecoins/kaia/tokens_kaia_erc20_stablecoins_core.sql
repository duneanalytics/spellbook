{% set chain = 'kaia' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_core',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- core list: frozen stablecoin addresses used for initial incremental balances
-- new stablecoins should be added to tokens_kaia_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0x18bc5bcc660cf2b9ce3cd51a404afe1a0cbd3c22), -- IDRX

     (0x5c13e303a62fc5dedf5b52d66873f2e59fedadc2), -- USDT
     (0xd077a400968890eacc75cdc901f0356c943e4fdb), -- USD₮
     (0xcee8faf64bb97a73bb51e115aa89c17ffa8dd167)  -- oUSDT

) as temp_table (contract_address)
