{{ config(
    schema = 'nft',
    alias = 'trades',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum","solana","bnb","optimism","arbitrum","polygon","zksync", "blast", "ronin", "nova", "abstract", "apechain"]\',
                    "sector",
                    "nft",
                    \'["soispoke","0xRob", "hildobby", "0xr3x"]\') }}')
}}


{{ port_to_old_schema(ref('nft_trades_beta')) }}