{{ config(
    schema = 'nft',
    alias = 'trades',
    materialized = 'view',
    filtering_columns = ['blockchain', 'project', 'block_month'],
    post_hook='{{ expose_spells(\'["abstract", "apechain", "arbitrum", "avalanche_c", "base", "blast", "bnb", "celo", "ethereum", "fantom", "linea", "nova", "optimism", "polygon", "ronin", "scroll", "zksync", "zora"]\',
                    "sector",
                    "nft",
                    \'["soispoke","0xRob", "hildobby", "0xr3x"]\') }}')
}}


{{ port_to_old_schema(ref('nft_trades_beta')) }}
