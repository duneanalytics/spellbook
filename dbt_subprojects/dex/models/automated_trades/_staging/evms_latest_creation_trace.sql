{{ config(
        schema = 'evms',
        alias = 'latest_creation_trace',
        materialized = 'table',
        file_format = 'delta',
        partition_by = ['blockchain', 'block_month'],
        unique_key = ['blockchain', 'block_month', 'address', 'from']
        )
}}

SELECT
    blockchain
    , address
    , "from"
    , cast(date_trunc('month', block_time) as date) as block_month
    , MAX(block_number) as latest_block
FROM
    {{ source('evms', 'creation_traces') }}
GROUP BY
    blockchain
    , address
    , "from"
    , cast(date_trunc('month', block_time) as date)