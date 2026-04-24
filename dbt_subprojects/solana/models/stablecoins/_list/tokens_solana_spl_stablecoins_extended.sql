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
    ('A94X2fRy3wydNShU4dRaDyap2UuoeWJGWyATtyp61WZf', 'TRY'),  -- TRYB
    ('A9mUU4qviSctJVPJdBJWkb28deg915LYJKrzQ19ji3FM', 'USD'),  -- USDC (Wormhole from Ethereum)
    ('Ejqkht2dyN1BaaEtK92zBKY6S8HbVH8APB5sDK9Rmokt', 'USD'),  -- rUSD
    ('9Gst2E7KovZ9jwecyGqnnhpG1mhHKdyLpJQnZonkCFhA', 'USD'),  -- USDX
    ('JuprjznTrTSp2UFa3ZBUFgwdAmtZCq4MQCwysN55USD', 'USD'),   -- jupUSD
    ('HVbpJAQGNpkgBaYBZQBR1t7yFdvaYVp2vCQQfKKEN4tM', 'USD'),  -- USDP
    ('52GzcLDMfBveMRnWXKX7U3Pa5Lf7QLkWWvsJRDjWDBSk', 'NGN'),  -- NGNC
    ('7FpVvhn3wgd959qvJymnRSHT4XE48P4mfqm5KoFkVKFD', 'KZT'),  -- KZTE
    ('GGUSDyBUPFg5RrgWwqEqhXoha85iYGs6cL57SyK4G2Y7', 'USD'),  -- GGUSD
    ('2VhjJ9WxaGC3EZFwJG9BDUs9KxKCAjQY4vgd1qxgYWVg', 'EUR'),  -- EUROe
    ('CASHx9KJUStyftLFWGvEVf59SGeG9sh5FfcnZMVPCASH', 'USD'),  -- CASH
    ('AUSD1jCcCyPLybk1YnvPWsHQSrZ46dxwoMniN4N2UEB9', 'USD'),  -- AUSD
    ('AUDDttiEpCydTm7joUMbYddm72jAWXZnCpPZtDoxqBSw', 'AUD')   -- AUDD

    /* yield-bearing / rebasing tokens
    ('AvZZF1YaZDziPY2RCK4oJrRVrbN3mTD9NL24hPeaZeUj', 'USD'),  -- syrupUSD
    */
) as temp_table (token_mint_address, currency)
