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
{{ dex_raw_pools() }}