{{ config(
        schema='prices_native',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags = ['static']
        )
}}
SELECT 
    token_id
    , CAST(blockchain as VARCHAR) as blockchain
    , symbol
    , CAST(contract_address as VARBINARY) as contract_address
    , CAST(decimals as int) as decimals
FROM
(
    VALUES

    ('ada-cardano', null, 'ADA', null, null),
    ('apt-aptos', null, 'APT', null, null),
    ('ae-aeternity', null, 'AE', null, null),
    ('algo-algorand', null, 'ALGO', null, null),
    ('atom-cosmos', null, 'ATOM', null, null),
    ('avax-avalanche', null, 'AVAX', null, null),
    ('bch-bitcoin-cash', null, 'BCH', null, null),
    ('bnb-binance-coin', null, 'BNB', null, null),
    ('bsv-bitcoin-sv', null, 'BSV', null, null),
    ('btc-bitcoin', null, 'BTC', null, null),
    ('celo-celo', null, 'CELO', null, null),
    ('dash-dash', null, 'DASH', null, null),
    ('dcr-decred', null, 'DCR', null, null),
    ('doge-dogecoin', null, 'DOGE', null, null),
    ('eos-eos', null, 'EOS', null, null),
    ('etc-ethereum-classic', null, 'ETC', null, null),
    ('eth-ethereum', null, 'ETH', null, null),
    ('ftm-fantom', null, 'FTM', null, null),
    ('hbar-hedera-hashgraph', null, 'HBAR', null, null),
    ('icp-internet-computer', null, 'ICP', null, null),
    ('icx-icon', null, 'ICX', null, null),
    ('kava-kava', null, 'KAVA', null, null),
    ('kuji-kujira', null, 'KUJI', null, null),
    ('ltc-litecoin', null, 'LTC', null, null),
    ('matic-polygon', null, 'MATIC', null, null),
    ('miota-iota', null, 'MIOTA', null, null),
    ('mnt-mantle', null, 'MNT', null, null),
    ('mona-monacoin', null, 'MONA', null, null),
    ('neo-neo', null, 'NEO', null, null),
    ('ont-ontology', null, 'ONT', null, null),
    ('osmo-osmosis', null, 'OSMO', null, null),
    ('sei-sei', null, 'SEI', null, null),
    ('sol-solana', null, 'SOL', null, null),
    ('stx-blockstack', null, 'STX', null, null),
    ('sui-sui', null, 'SUI', null, null),
    ('tao-bittensor', null, 'TAO', null, null),
    ('thr-thorecoin', null, 'THR', null, null),
    ('tia-celestia', null, 'TIA', null, null),
    ('tomo-tomochain', null, 'TOMO', null, null),
    ('trx-tron', null, 'TRX', null, null),
    ('xdai-xdai', null, 'XDAI', null, null),
    ('xem-nem', null, 'XEM', null, null),
    ('xlm-stellar', null, 'XLM', null, null),
    ('xmr-monero', null, 'XMR', null, null),
    ('xrp-xrp', null, 'XRP', null, null),
    ('xtz-tezos', null, 'XTZ', null, null),
    ('zec-zcash', null, 'ZEC', null, null)
    
) as temp (token_id, blockchain, symbol, contract_address, decimals)
