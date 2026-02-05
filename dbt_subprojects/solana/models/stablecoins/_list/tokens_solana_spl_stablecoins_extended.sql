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

select '{{chain}}' as blockchain, token_mint_address
from (values
    ('FtgGSFADXBtroxq8VCausXRr2of47QBf5AS1NtZCu4GD'),  -- BRZ
    ('Copm5KwCLXDTWYgXJYmo6ixmMZrxd1wabkujkcuaK47C'),  -- COPM
    ('HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr'),  -- EURC
    ('DghpMkatCiUsofbTmid3M3kAbDTPqDwKiYHnudXeGG52'),  -- EURCV
    ('6zYgzrT7X2wi9a9NeMtUvUWLLmf2a8vBsbYkocYdB9wa'),  -- MXNE
    ('idrxTdNftk6tYedPv2M7tCFHBVCpk5rkiNRd8yUArhr'),   -- IDRX
    ('Crn4x1Y2HUKko7ox2EZMT6N2t2ZyH7eKtwkBGVnhEq1g'),  -- GYEN
    ('C4Kkr9NZU3VbyedcgutU6LKmi6MKz81sx6gRmk5pX519'),  -- VEUR
    ('5H4voZhzySsVvwVYDAKku8MZGuYBC7cXaBKDPW4YHWW1'),  -- VGBP
    ('dngKhBQM3BGvsDHKhrLnjvRKfY5Q7gEnYGToj9Lk8rk'),   -- ZARP
    ('AhhdRu5YZdjVkKR3wbnUDaymVQL2ucjMQ63sZ3LFHsch'),  -- VCHF
    ('A94X2fRy3wydNShU4dRaDyap2UuoeWJGWyATtyp61WZf')   -- TRYB
) as temp_table (token_mint_address)
