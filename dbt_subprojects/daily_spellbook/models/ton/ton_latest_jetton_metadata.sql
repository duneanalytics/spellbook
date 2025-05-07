{{ config(
    schema = 'ton'
    , alias = 'latest_jetton_metadata'
    , materialized = 'view'
    )
}}

WITH ranks AS (
    SELECT 
        address
        , update_time_onchain
        , update_time_metadata
        , mintable
        , admin_address
        , jetton_content_onchain
        , jetton_wallet_code_hash
        , code_hash
        , metadata_status
        , symbol
        , name
        , description
        , image
        , image_data
        , decimals
        , sources
        , tonapi_image_url
        , ROW_NUMBER() OVER (
            PARTITION BY address 
            ORDER BY update_time_metadata DESC, update_time_onchain DESC
        ) AS rank 
    FROM {{ source('ton', 'jetton_metadata') }}
)

SELECT 
    address
    , update_time_onchain
    , update_time_metadata
    , mintable
    , admin_address
    , jetton_content_onchain
    , jetton_wallet_code_hash
    , code_hash
    , metadata_status
    , symbol
    , name
    , description
    , image
    , image_data
    , decimals
    , sources
    , tonapi_image_url
FROM ranks
WHERE rank = 1 