{{ config(
      schema = 'tokens_solana'
      , alias = 'stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["arbitrum"]\',
                                  "sector",
                                  "tokens_solana",
                                  \'["synthquest"]\') }}'
      , unique_key = ['address']
  )
}}

select blockchain, symbol, address, decimals, backing, name
from (Values
      ('solana', 'USDC','EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 6, 'Fiat Stablecoin', 'Circle')
    , ('solana', 'USDT','Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', 6,'Fiat Stablecoin', 'Tether')
    , ('solana', 'PYUSD','2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo', 6,'Fiat Stablecoin', 'Paxos')
    , ('solana', 'USDH','USDH1SM1ojwWUga67PGrgFWUHibbjqMvuMaDkRJTgkX', 6,'Crypto Stablecoin', 'Hubble')
    , ('solana', 'UXD','7kbnvuGBxxj8AG9qp8Scn56muWGaRaFqxg1FsRp3PaFT', 6,'Dollar-Pegged', 'UXD Protocol')
    , ('solana', 'sUSDe','Eh6XEPhSwoLv5wFApukmnaVSHQ6sAnoD9BmgmwQoN2sN', 9,'Dollar-Pegged', 'Ethena')
    , ('solana', 'USDe','DEkqHyPN7GMRJ5cArtQFAWefqbZb33Hyf6s5iCwjEonT', 9,'Dollar-Pegged', 'Ethena')
    , ('solana', 'USDY','A1KLoBrKBde8Ty9qtNQUtq3C2ortoC3u7twggz7sEto6', 6,'Dollar-Pegged', 'Ondo')
    , ('solana', 'ZUSD','FrBfWJ4qE5sCzKm3k3JaAtqZcXUh4LvJygDeketsrsH4', 6,'Dollar-Pegged','GMO-Z')
    , ('solana', 'PAI', 'Ea5SjE2Y6yvCeW5dYTn7PYMuW5ikXkvbGdcmSnXeaLjS', 6,'Crypto Stablecoin', 'Parrot')
) as t(blockchain, symbol, address, decimals, backing, name)
