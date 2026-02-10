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

select '{{chain}}' as blockchain, contract_address
from (values

     ('TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t'), -- USDT
     ('TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8'), -- USDCOLD
     ('TPYmHEhy5n8TCEfYGqW2rPxsghSfzghPDn'), -- USDDOLD
     ('TXDk8mbtRbXeYuMNS83CfKPaYYT8XWv9Hz'), -- USDD
     ('TUpMhErZL2fhh4sVNULAbNKLokS4GjC1F4'), -- TUSD
     ('TPFqcBAaaUMCSVRCqPaQ9QnzKhmuoLR6Rc')  -- USD1

) as temp_table (contract_address)
