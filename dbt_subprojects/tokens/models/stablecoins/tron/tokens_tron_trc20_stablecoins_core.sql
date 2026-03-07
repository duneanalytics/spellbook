{% set chain = 'tron' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'trc20_stablecoins_core',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- core list: frozen stablecoin addresses used for initial incremental transfers
-- new stablecoins should be added to tokens_tron_trc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address, currency
from (values

     ('TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t', 'USD'), -- USDT
     ('TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8', 'USD'), -- USDCOLD
     ('TPYmHEhy5n8TCEfYGqW2rPxsghSfzghPDn', 'USD'), -- USDDOLD
     ('TUpMhErZL2fhh4sVNULAbNKLokS4GjC1F4', 'USD'), -- TUSD
     ('TPFqcBAaaUMCSVRCqPaQ9QnzKhmuoLR6Rc', 'USD'), -- USD1
     ('TXDk8mbtRbXeYuMNS83CfKPaYYT8XWv9Hz', 'USD'), -- USDD
     ('TLeVfrdym8RoJreJ23dAGyfJDygRtiWKBZ', 'RUB')  -- A7A5

) as temp_table (contract_address, currency)
