{{ config(
        schema = 'tokens'
        , alias = 'transfers_last_updated'
        , materialized = 'table'
        , file_format = 'delta'
        )
}}

SELECT
    blockchain
    , block_month
    , max(_updated_at) AS last_update_date
FROM {{ ref('tokens_transfers') }}
GROUP BY
    blockchain
    , block_month
