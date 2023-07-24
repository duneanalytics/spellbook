{{ config(
        schema='prices_native',
        alias = alias('tokens'),
        materialized='table',
        file_format = 'delta',
        tags=['static', 'dunesql']
        )
}}
SELECT 
    TRIM(token_id) as token_id
    , LOWER(TRIM(blockchain)) as blockchain
    , TRIM(symbol) as symbol
    , contract_address
    , CAST(decimals as int)
FROM
(
    VALUES

    ('ada-cardano', null, 'ADA', CAST(NULL AS VARBINARY), null),
    ('ae-aeternity', null, 'AE', CAST(NULL AS VARBINARY), null),
    ('algo-algorand', null, 'ALGO', CAST(NULL AS VARBINARY), null),
    ('atom-cosmos', null, 'ATOM', CAST(NULL AS VARBINARY), null),
    ('avax-avalanche', null, 'AVAX', CAST(NULL AS VARBINARY), null),
    ('bch-bitcoin-cash', null, 'BCH', CAST(NULL AS VARBINARY), null),
    ('bnb-binance-coin', null, 'BNB', CAST(NULL AS VARBINARY), null),
    ('bsv-bitcoin-sv', null, 'BSV', CAST(NULL AS VARBINARY), null),
    ('btc-bitcoin', null, 'BTC', CAST(NULL AS VARBINARY), null),
    ('celo-celo', null, 'CELO', CAST(NULL AS VARBINARY), null),
    ('dash-dash', null, 'DASH', CAST(NULL AS VARBINARY), null),
    ('dcr-decred', null, 'DCR', CAST(NULL AS VARBINARY), null),
    ('doge-dogecoin', null, 'DOGE', CAST(NULL AS VARBINARY), null),
    ('eos-eos', null, 'EOS', CAST(NULL AS VARBINARY), null),
    ('etc-ethereum-classic', null, 'ETC', CAST(NULL AS VARBINARY), null),
    ('eth-ethereum', null, 'ETH', CAST(NULL AS VARBINARY), null),
    ('ftm-fantom', null, 'FTM', CAST(NULL AS VARBINARY), null),
    ('hbar-hedera-hashgraph', null, 'HBAR', CAST(NULL AS VARBINARY), null),
    ('icx-icon', null, 'ICX', CAST(NULL AS VARBINARY), null),
    ('ltc-litecoin', null, 'LTC', CAST(NULL AS VARBINARY), null),
    ('matic-polygon', null, 'MATIC', CAST(NULL AS VARBINARY), null),
    ('miota-iota', null, 'MIOTA', CAST(NULL AS VARBINARY), null),
    ('mona-monacoin', null, 'MONA', CAST(NULL AS VARBINARY), null),
    ('neo-neo', null, 'NEO', CAST(NULL AS VARBINARY), null),
    ('ont-ontology', null, 'ONT', CAST(NULL AS VARBINARY), null),
    ('sol-solana', null, 'SOL', CAST(NULL AS VARBINARY), null),
    ('stx-blockstack', null, 'STX', CAST(NULL AS VARBINARY), null),
    ('thr-thorecoin', null, 'THR', CAST(NULL AS VARBINARY), null),
    ('tomo-tomochain', null, 'TOMO', CAST(NULL AS VARBINARY), null),
    ('trx-tron', null, 'TRX', CAST(NULL AS VARBINARY), null),
    ('xdai-xdai', null, 'XDAI', CAST(NULL AS VARBINARY), null),
    ('xem-nem', null, 'XEM', CAST(NULL AS VARBINARY), null),
    ('xlm-stellar', null, 'XLM', CAST(NULL AS VARBINARY), null),
    ('xmr-monero', null, 'XMR', CAST(NULL AS VARBINARY), null),
    ('xrp-xrp', null, 'XRP', CAST(NULL AS VARBINARY), null),
    ('xtz-tezos', null, 'XTZ', CAST(NULL AS VARBINARY), null),
    ('zec-zcash', null, 'ZEC', CAST(NULL AS VARBINARY), null)
    
) as temp (token_id, blockchain, symbol, contract_address, decimals)
