{{ config(
    schema = 'nft',
    alias ='events',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum","solana","bnb","optimism","arbitrum","polygon"]\',
                    "sector",
                    "nft",
                    \'["soispoke","0xRob", "hildobby"]\') }}')
}}

SELECT * FROM {{ref('nft_events_old')}}
