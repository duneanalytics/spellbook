{{
  config(
    schema = 'tokens',
    alias = 'spl_stablecoins_metadata',
    materialized = 'table',
    tags = ['static'],
    post_hook = '{{ expose_spells(blockchains = \'["solana"]\',
                                  spell_type = "sector",
                                  spell_name = "tokens",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

select blockchain, token_mint_address, backing, symbol, decimals, name
from (values

-- solana
('solana', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'Fiat-backed stablecoin', 'USDC', 6, 'Circle'),
('solana', 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', 'Fiat-backed stablecoin', 'USDT', 6, 'Tether'),
('solana', 'USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB', 'Fiat-backed stablecoin', 'USD1', 6, 'World Liberty Financial'),
('solana', '2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo', 'Fiat-backed stablecoin', 'PyUSD', 6, 'PayPal'),
('solana', '2u1tszSeqZ3qBWF3uNGPFc8TzMk2tdiwknnRMWGWjGWH', 'Fiat-backed stablecoin', 'USDG', 6, 'Global Dollar'),
('solana', 'USDSwr9ApdHk5bvJKMjzff41FfuX8bSxdKcR81vTwcA', 'Crypto-backed stablecoin', 'USDS', 6, 'Sky'),
('solana', '6FrrzDk5mQARGc1TDYoyVnSyRdds1t4PbtohCD6p3tgG', 'Crypto-backed stablecoin', 'USX', 6, 'dForce'),
('solana', 'DUSDt4AeLZHWYmcXnVGYdgAzjtzU5mXUVnTMdnSzAttM', 'Crypto-backed stablecoin', 'DUSD', 6, ''),
('solana', '5YMkXAYccHSGnHn9nob9xEvv6Pvka9DZWH7nTbotTu9E', 'Crypto-backed stablecoin', 'hyUSD', 6, 'High Yield USD'),
('solana', '9zNQRsGLjNKwCUU5Gq5LR8beUCPzQMVMqKAi3SSZh54u', 'Fiat-backed stablecoin', 'FDUSD', 6, 'First Digital Labs'),
('solana', 'DEkqHyPN7GMRJ5cArtQFAWefqbZb33Hyf6s5iCwjEonT', 'Crypto-backed stablecoin', 'USDe', 9, 'Ethena'),
('solana', 'star9agSpjiFe3M49B3RniVU4CMBBEK3Qnaqn3RGiFM', 'Crypto-backed stablecoin', 'USD*', 6, 'Perena'),
('solana', '4FVaHEubcqws8hKwJSiW8f8CmKGUyMsBxTKUytcGdRvd', 'Crypto-backed stablecoin', 'HSUSD', 9, ''),
('solana', '9ckR7pPPvyPadACDTzLwK2ZAEeUJ3qGSnzPs8bVaHrSy', 'Crypto-backed stablecoin', 'USDu', 6, ''),
('solana', 'Ex5DaKYMCN6QWFA4n67TmMwsH8MJV68RX6YXTmVM532C', 'Crypto-backed stablecoin', 'USDv', 9, ''),
('solana', 'USDH1SM1ojwWUga67PGrgFWUHibbjqMvuMaDkRJTgkX', 'Crypto-backed stablecoin', 'USDH', 6, 'Hubble'),
('solana', '7kbnvuGBxxj8AG9qp8Scn56muWGaRaFqxg1FsRp3PaFT', 'Crypto-backed stablecoin', 'UXD', 6, 'UXD Protocol'),
('solana', 'Eh6XEPhSwoLv5wFApukmnaVSHQ6sAnoD9BmgmwQoN2sN', 'Crypto-backed stablecoin', 'sUSDe', 9, 'Ethena'),
('solana', 'A1KLoBrKBde8Ty9qtNQUtq3C2ortoC3u7twggz7sEto6', 'RWA-backed stablecoin', 'USDY', 6, 'Ondo'),
('solana', 'FrBfWJ4qE5sCzKm3k3JaAtqZcXUh4LvJygDeketsrsH4', 'Fiat-backed stablecoin', 'ZUSD', 6, 'GMO-Z'),
('solana', 'Ea5SjE2Y6yvCeW5dYTn7PYMuW5ikXkvbGdcmSnXeaLjS', 'Crypto-backed stablecoin', 'PAI', 6, 'Parrot')

) as temp_table (blockchain, token_mint_address, backing, symbol, decimals, name)

