{% set chain = 'solana' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'spl_stablecoins_extended',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['token_mint_address']
  )
}}

-- extended list: new stablecoin addresses added after the core list was frozen
-- add new stablecoins here (not in tokens_solana_spl_stablecoins_core)

select '{{chain}}' as blockchain, token_mint_address, currency
from (values
    ('FtgGSFADXBtroxq8VCausXRr2of47QBf5AS1NtZCu4GD', 'BRL'),  -- BRZ
    ('Copm5KwCLXDTWYgXJYmo6ixmMZrxd1wabkujkcuaK47C', 'COP'),  -- COPM
    ('HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr', 'EUR'),  -- EURC
    ('DghpMkatCiUsofbTmid3M3kAbDTPqDwKiYHnudXeGG52', 'EUR'),  -- EURCV
    ('6zYgzrT7X2wi9a9NeMtUvUWLLmf2a8vBsbYkocYdB9wa', 'MXN'),  -- MXNE
    ('idrxTdNftk6tYedPv2M7tCFHBVCpk5rkiNRd8yUArhr', 'IDR'),   -- IDRX
    ('Crn4x1Y2HUKko7ox2EZMT6N2t2ZyH7eKtwkBGVnhEq1g', 'JPY'),  -- GYEN
    ('C4Kkr9NZU3VbyedcgutU6LKmi6MKz81sx6gRmk5pX519', 'EUR'),  -- VEUR
    ('5H4voZhzySsVvwVYDAKku8MZGuYBC7cXaBKDPW4YHWW1', 'GBP'),  -- VGBP
    ('dngKhBQM3BGvsDHKhrLnjvRKfY5Q7gEnYGToj9Lk8rk', 'ZAR'),   -- ZARP
    ('AhhdRu5YZdjVkKR3wbnUDaymVQL2ucjMQ63sZ3LFHsch', 'CHF'),  -- VCHF
    ('A94X2fRy3wydNShU4dRaDyap2UuoeWJGWyATtyp61WZf', 'TRY')   -- TRYB
) as temp_table (token_mint_address, currency)
