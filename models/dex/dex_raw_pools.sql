{{ config(
        schema='dex',
        alias = 'raw_pools',
        materialized = 'view',
        file_format = 'delta',
        unique_key = ['blockchain', 'pool'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo", "base", "zksync", "zora"]\',
                        "sector",
                        "dex",
                        \'["grkhr"]\') }}'
)
}}



-- get rid of degen-pools changed tokens after creation
select * from {{ ref('dex_raw_pool_creations') }}
where (blockchain, pool) not in (
    select blockchain, pool from {{ ref('dex_raw_pool_initializations') }}
    group by blockchain, pool 
    having count(*) > 1
)