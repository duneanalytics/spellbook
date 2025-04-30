{{ config(
        alias = 'nft_curated',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["scroll"]\',
                                "sector",
                                "tokens",
                                \'["msilb7"]\') }}'
        )
}}

-- This will be empty for now, can be populated with hand-curated NFT data later
SELECT
    CAST(NULL AS varbinary) AS contract_address
    , CAST(NULL AS VARCHAR) AS name
    , CAST(NULL AS VARCHAR) AS symbol
WHERE FALSE 
