{{ config(
        schema = 'nft',
        alias = alias('trades'),
        tags = ['dunesql'],
        materialized = 'view',
        post_hook='{{ expose_spells(\'["ethereum","solana","bnb", "optimism","arbitrum","polygon"]\',
                                    "sector",
                                    "nft",
                                    \'["soispoke", "0xRob", "hildobby"]\') }}')
}}

SELECT *
FROM {{ ref('nft_events') }}
WHERE evt_type = 'Trade'
