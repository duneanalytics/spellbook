{{ config(
    schema = 'nft',
    alias ='events',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum","solana","bnb","optimism","arbitrum","polygon"]\',
                    "sector",
                    "nft",
                    \'["soispoke","0xRob", "hildobby"]\') }}')
}}

SELECT * FROM {{ ref('nft_ethereum_trades_beta_ported')}}
UNION ALL
SELECT * FROM {{ref('nft_events_old')}}
WHERE (project, version) not in (SELECT distinct project, version FROM {{ref('nft_ethereum_trades_beta_ported')}})


