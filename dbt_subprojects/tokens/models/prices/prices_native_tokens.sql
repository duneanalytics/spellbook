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
    --maintain legacy native token approach for historical queries
    ('ada-cardano', null, 'ADA', null, null),
    ('apt-aptos', null, 'APT', null, null),
    ('ae-aeternity', null, 'AE', null, null),
    ('algo-algorand', null, 'ALGO', null, null),
    ('arb-arbitrum', null, 'ARB', null, null),
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
    ('matic-polygon', null, 'MATIC', null, null), --maintain legacy native token for legacy queries
    ('matic-polygon', null, 'POL', null, null), --no full history for 'pol-polygon-ecosystem-token' API ID on coinpaprika
    ('miota-iota', null, 'MIOTA', null, null),
    ('mnt-mantle', null, 'MNT', null, null),
    ('mona-monacoin', null, 'MONA', null, null),
    ('neo-neo', null, 'NEO', null, null),
    ('ont-ontology', null, 'ONT', null, null),
    ('osmo-osmosis', null, 'OSMO', null, null),
    ('sei-sei', null, 'SEI', null, null), --note: SEI already added to native tokens, commenting for history in PR to initiate SEI
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
    ('zec-zcash', null, 'ZEC', null, null),
    ('astr-astar', null, 'ASTR', null, null),
    ('dym-dymension-iou', null, 'DYM', null, null),
    ('manta-manta-network', null, 'MANTA', null, null),
    ('rose-oasis-network', null, 'ROSE', null, null),
    ('saga-saga', null, 'SAGA', null, null),
    ('kas-kaspa', null, 'KAS', null, null),
    ('egld-elrond', null, 'EGLD', null, null),
    ('ntrn1-neutron', null, 'NTRN', null, null),
    ('flr-flare-network', null, 'FLR', null, null),
    ('ar-arweave', null, 'AR', null, null),
    ('glmr-moonbeam', null, 'GLMR', null, null),
    ('klay-klaytn', null, 'KLAY', null, null),
    ('fuse-fuse-network', null, 'FUSE', null, null),
    ('zel-zelcash', null, 'FLUX', null, null),
    ('myria-myria', null, 'MYRIA', null, null),
    ('mode-mode', null, 'MODE', null,null),
    ('ton-toncoin', null, 'TON', null,null),
    ('strk-starknet', null, 'STRK', null,null),
    ('kda-kadena', null, 'KDA', null, null),

    --add new approach to native tokens to populate all fields, standardizing joins to prices regardless of token type
    ('arb-arbitrum', 'arbitrum', 'ARB', 0x0000000000000000000000000000000000000000, 18),
    ('avax-avalanche', 'avalanche_c', 'AVAX', 0x0000000000000000000000000000000000000000, 18),
    ('eth-ethereum', 'base', 'ETH', 0x0000000000000000000000000000000000000000, 18),
    ('eth-ethereum', 'blast', 'ETH', 0x0000000000000000000000000000000000000000, 18),
    ('bnb-binance-coin', 'bnb', 'BNB', 0x0000000000000000000000000000000000000000, 18),
    ('celo-celo', 'celo', 'CELO', 0x0000000000000000000000000000000000000000, 18),
    ('eth-ethereum', 'ethereum', 'ETH', 0x0000000000000000000000000000000000000000, 18),
    ('ftm-fantom', 'fantom', 'FTM', 0x0000000000000000000000000000000000000000, 18),
    ('xdai-xdai', 'gnosis', 'xDAI', 0x0000000000000000000000000000000000000000, 18),
    ('eth-ethereum', 'linea', 'ETH', 0x0000000000000000000000000000000000000000, 18),
    ('mnt-mantle', 'mantle', 'MNT', 0x0000000000000000000000000000000000000000, 18),
    ('eth-ethereum', 'nova', 'ETH', 0x0000000000000000000000000000000000000000, 18),
    ('eth-ethereum', 'optimism', 'ETH', 0x0000000000000000000000000000000000000000, 18),
    ('matic-polygon', 'polygon', 'POL', 0x0000000000000000000000000000000000000000, 18), --no full history for 'pol-polygon-ecosystem-token' API ID on coinpaprika
    ('eth-ethereum', 'scroll', 'ETH', 0x0000000000000000000000000000000000000000, 18),
    ('sei-sei', 'sei', 'SEI', 0x0000000000000000000000000000000000000000, 18),
    ('sol-solana', 'solana', 'SOL', 0x0000000000000000000000000000000000000000, 9),
    ('eth-ethereum', 'zkevm', 'ETH', 0x0000000000000000000000000000000000000000, 18),
    ('zk-zksync', 'zksync', 'ZK', 0x0000000000000000000000000000000000000000, 18),
    ('eth-ethereum', 'zora', 'ETH', 0x0000000000000000000000000000000000000000, 18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)