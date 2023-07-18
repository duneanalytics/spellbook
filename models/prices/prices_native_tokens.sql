{{ config(
        schema='prices_native',
        alias = alias('tokens'),
        materialized='table',
        file_format = 'delta',
        tags=['static']
        )
}}
SELECT 
    TRIM(token_id) as token_id
    , LOWER(TRIM(blockchain)) as blockchain
    , TRIM(symbol) as symbol
    , LOWER(TRIM(contract_address)) as contract_address
    , CAST(decimals as int)
FROM
(
    VALUES

    ("ada-cardano", null, "ADA", null, null),
    ("ae-aeternity", null, "AE", null, null),
    ("algo-algorand", null, "ALGO", null, null),
    ("atom-cosmos", null, "ATOM", null, null),
    ("avax-avalanche", null, "AVAX", null, null),
    ("bch-bitcoin-cash", null, "BCH", null, null),
    ("bnb-binance-coin", null, "BNB", null, null),
    ("bsv-bitcoin-sv", null, "BSV", null, null),
    ("btc-bitcoin", null, "BTC", null, null),
    ("celo-celo", null, "CELO", null, null),
    ("dash-dash", null, "DASH", null, null),
    ("dcr-decred", null, "DCR", null, null),
    ("doge-dogecoin", null, "DOGE", null, null),
    ("eos-eos", null, "EOS", null, null),
    ("etc-ethereum-classic", null, "ETC", null, null),
    ("eth-ethereum", null, "ETH", null, null),
    ("ftm-fantom", null, "FTM", null, null),
    ("hbar-hedera-hashgraph", null, "HBAR", null, null),
    ("icx-icon", null, "ICX", null, null),
    ("ltc-litecoin", null, "LTC", null, null),
    ("matic-polygon", null, "MATIC", null, null),
    ("miota-iota", null, "MIOTA", null, null),
    ("mona-monacoin", null, "MONA", null, null),
    ("neo-neo", null, "NEO", null, null),
    ("ont-ontology", null, "ONT", null, null),
    ("sol-solana", null, "SOL", null, null),
    ("stx-blockstack", null, "STX", null, null),
    ("thr-thorecoin", null, "THR", null, null),
    ("tomo-tomochain", null, "TOMO", null, null),
    ("trx-tron", null, "TRX", null, null),
    ("xdai-xdai", null, "XDAI", null, null),
    ("xem-nem", null, "XEM", null, null),
    ("xlm-stellar", null, "XLM", null, null),
    ("xmr-monero", null, "XMR", null, null),
    ("xrp-xrp", null, "XRP", null, null),
    ("xtz-tezos", null, "XTZ", null, null),
    ("zec-zcash", null, "ZEC", null, null)
    
) as temp (token_id, blockchain, symbol, contract_address, decimals)
