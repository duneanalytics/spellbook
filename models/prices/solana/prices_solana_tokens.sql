{{ config(
        schema='prices_solana',
        alias ='tokens',
        materialized='table',
        file_format = 'delta',
        tags=['static']
        )
}}
SELECT 
    TRIM(token_id) as token_id
    , LOWER(TRIM(blockchain)) as blockchain
    , TRIM(symbol) as symbol
    , TRIM(contract_address) as contract_address --address cannot be lower() on solana
    , decimals
FROM
(
    VALUES

    ("aury-aurory", "solana", "AURY", "AURYydfxJib1ZkTir1Jn1J9ECYUtjb6rKQVmtYaixWPP", 9),
    ("btc-bitcoin", "solana", "BTC", "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E", 6),
    ("cope-cope", "solana", "COPE", "8HGyAAB1yoM1ttS7pXjHMa3dukTFGQggnFFH3hJZgzQh", 6),
    ("dfl-defi-land", "solana", "DFL", "DFL1zNkaGPWm1BqAVqRjCZvHmwTFrEaJtbzJWgseoNJh", 9),
    ("dust-dust-protocol", "solana", "DUST", "DUSTawucrTsGU8hcqRdHDCbuYhCPADMLM2VcCb8VnFnQ", 9),
    ("eth-ethereum", "solana", "ETH", "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs", 8),
    ("eth-ethereum", "solana", "soETH", "2FPyTwcZLUg1MDrwsyoP4D6s1tM7hAkHYRjkNb5w6Pxk", 6),
    ("gst-gst", "solana", "GST", "AFbX8oGjGpmVFywbVouvhQSRmiW2aR1mohfahi4Y2AdB", 9),
    ("luna-terra", "solana", "LUNA", "F6v4wfAdJB8D8p77bMXZgYt8TDKsYxLYxH5AFhUkYx9W", 6),
    ("mngo-mango-markets", "solana", "MNGO", "MangoCzJ36AjZyKwVj3VnYU4GTonjfVEnJmvvWaxLac", 6),
    ("msol-marinade-staked-sol", "solana", "MSOL", "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So", 9),
    ("ray-raydium", "solana", "RAY", "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R", 6),
    ("samo-samoyedcoin", "solana", "SAMO", "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU", 9),
    ("shdw-genesysgo-shadow", "solana", "SHDW", "SHDWyBxihqiCj6YekG2GUr7wqKLeLAMK1gHZck9pL6y", 9),
    ("srm-serum", "solana", "SRM", "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt", 6),
    ("steth-lido-staked-ether", "solana", "wstETH", "ZScHuTtqZukUrtZS43teTKGs2VqkKL8k4QCouR2n6Uo", 8),
    ("stsol-lido-staked-sol", "solana", "stSOL", "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj", 9),
    ("sol-solana", "solana", "SOL", "So11111111111111111111111111111111111111112", 9),
    ("ust-terrausd", "solana", "USTC", "9vMJfxuKxXBoEa7rM12mYLMwTacLMLDJqHozw96WQL8i", 6),
    ("usdc-usd-coin", "solana", "USDC", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", 6),
    ("usdt-tether", "solana", "USDT", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", 6)
    
) as temp (token_id, blockchain, symbol, contract_address, decimals)
