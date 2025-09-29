{{ config(
        schema='prices_native',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags = ['static']
        )
}}

with prices_native_tokens as (
    select
        blockchain
        , token_id
    from
    (
        values
        ('abstract', 'eth-ethereum')
        , ('apechain', 'ape-apecoin')
        , ('aptos', 'apt-aptos')
        , ('arbitrum', 'eth-ethereum')
        , ('avalanche_c', 'avax-avalanche')
        , ('b3', 'eth-ethereum')
        , ('base', 'eth-ethereum')
        , ('beacon', 'eth-ethereum')
        , ('berachain', 'bera-berachain')
        , ('bitcoin', 'btc-bitcoin')
        , ('blast', 'eth-ethereum')
        , ('bnb', 'bnb-binance-coin')
        , ('bob', 'eth-ethereum')
        , ('boba', 'eth-ethereum')
        , ('celo', 'celo-celo')
        , ('corn', 'corn-corn2')
        , ('degen', 'degen-degen-base')
        , ('ethereum', 'eth-ethereum')
        , ('fantom', 'ftm-fantom')
        , ('flare', 'flr-flare-network')
        , ('flow', 'eth-ethereum')
        , ('fuel', 'eth-ethereum')
        , ('gnosis', 'dai-dai')
        , ('goerli', 'eth-ethereum')
        , ('hemi', 'eth-ethereum')
        , ('hyperevm', 'hype-hyperliquid')
        , ('ink', 'eth-ethereum')
        , ('kaia', 'kaia-kaia')
        , ('katana', 'eth-ethereum')
        , ('lens', 'gho-gho')
        , ('linea', 'eth-ethereum')
        , ('mantle', 'mnt-mantle')
        , ('mode', 'eth-ethereum')
        , ('noble', 'eth-ethereum')
        , ('nova', 'eth-ethereum')
        , ('opbnb', 'bnb-binance-coin')
        , ('optimism', 'eth-ethereum')
        , ('optimism_legacy_ovm1', 'eth-ethereum')
        , ('peaq', 'peaq-peaq-network')
        , ('plume', 'plume-plume')
        , ('polygon', 'matic-polygon')
        , ('ronin', 'ron-ronin-token')
        , ('scroll', 'eth-ethereum')
        , ('sei', 'sei-sei')
        , ('sepolia', 'eth-ethereum')
        , ('shape', 'eth-ethereum')
        , ('solana', 'sol-solana')
        , ('somnia', 'somi-somnia')
        , ('sonic', 's-sonic')
        , ('sophon', 'soph-sophon')
        , ('starknet', 'strk-starknet')
        , ('stellar', 'xlm-stellar')
        , ('superseed', 'eth-ethereum')
        , ('taiko', 'eth-ethereum')
        , ('ton', 'ton-toncoin')
        , ('tron', 'trx-tron')
        , ('unichain', 'eth-ethereum')
        , ('viction', 'tomo-tomochain')
        , ('worldchain', 'eth-ethereum')
        , ('zkevm', 'eth-ethereum')
        , ('zksync', 'eth-ethereum')
        , ('zora', 'eth-ethereum')
    ) as temp (blockchain, token_id)
)
select
    p.token_id
    , p.blockchain
    , d.token_address as contract_address
    , d.token_symbol as symbol
    , d.token_decimals as decimals
from prices_native_tokens as p
inner join {{source('dune','blockchains')}} as d
    on p.blockchain = d.name