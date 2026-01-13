{% set chain = 'solana' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'spl_stablecoins_core',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['token_mint_address']
  )
}}

-- core list: frozen stablecoin addresses used for initial incremental transfers
-- new stablecoins should be added to tokens_solana_spl_stablecoins_extended
-- source: stablecoin candidate detection query (authenticity_score >= 75)

select '{{chain}}' as blockchain, token_mint_address
from (values
    ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'),  -- USDC
    ('Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'),  -- USDT
    ('USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB'),   -- USD1
    ('2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo'),  -- PyUSD
    ('2u1tszSeqZ3qBWF3uNGPFc8TzMk2tdiwknnRMWGWjGWH'),  -- USDG
    ('USDSwr9ApdHk5bvJKMjzff41FfuX8bSxdKcR81vTwcA'),   -- USDS
    ('6FrrzDk5mQARGc1TDYoyVnSyRdds1t4PbtohCD6p3tgG'),  -- USX
    ('DUSDt4AeLZHWYmcXnVGYdgAzjtzU5mXUVnTMdnSzAttM'),  -- DUSD
    ('5YMkXAYccHSGnHn9nob9xEvv6Pvka9DZWH7nTbotTu9E'),  -- hyUSD
    ('9zNQRsGLjNKwCUU5Gq5LR8beUCPzQMVMqKAi3SSZh54u'),  -- FDUSD
    ('DEkqHyPN7GMRJ5cArtQFAWefqbZb33Hyf6s5iCwjEonT'),  -- USDe
    ('star9agSpjiFe3M49B3RniVU4CMBBEK3Qnaqn3RGiFM'),   -- USD*
    ('4FVaHEubcqws8hKwJSiW8f8CmKGUyMsBxTKUytcGdRvd'),  -- HSUSD
    ('9ckR7pPPvyPadACDTzLwK2ZAEeUJ3qGSnzPs8bVaHrSy'),  -- USDu
    ('Ex5DaKYMCN6QWFA4n67TmMwsH8MJV68RX6YXTmVM532C'),  -- USDv
    ('USDH1SM1ojwWUga67PGrgFWUHibbjqMvuMaDkRJTgkX'),   -- USDH
    ('7kbnvuGBxxj8AG9qp8Scn56muWGaRaFqxg1FsRp3PaFT'),  -- UXD
    ('Eh6XEPhSwoLv5wFApukmnaVSHQ6sAnoD9BmgmwQoN2sN'),  -- sUSDe
    ('A1KLoBrKBde8Ty9qtNQUtq3C2ortoC3u7twggz7sEto6'),  -- USDY
    ('FrBfWJ4qE5sCzKm3k3JaAtqZcXUh4LvJygDeketsrsH4'),  -- ZUSD
    ('Ea5SjE2Y6yvCeW5dYTn7PYMuW5ikXkvbGdcmSnXeaLjS')   -- PAI
) as temp_table (token_mint_address)
