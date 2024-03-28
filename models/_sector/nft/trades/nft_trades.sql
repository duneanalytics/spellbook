{{ config(
    schema = 'nft',
    alias = 'trades',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum","solana","bnb","optimism","arbitrum","polygon","zksync"]\',
                    "sector",
                    "nft",
                    \'["soispoke","0xRob", "hildobby"]\') }}')
}}


{{ port_to_old_schema(ref('nft_trades_beta')) }}
