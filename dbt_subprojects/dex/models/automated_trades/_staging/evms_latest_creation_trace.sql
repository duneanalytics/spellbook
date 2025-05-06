{{ config(
    schema = 'evms'
    , alias = 'latest_creation_trace'
    , partition_by = ['blockchain', 'block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'block_month', 'address', '"from"']
)
}}

SELECT
    blockchain
    , cast(date_trunc('month', block_time) as date) as block_month
    , address
    , "from"
    , MAX(block_number) as latest_block
FROM
    {{ source('evms', 'creation_traces') }}
{% if is_incremental() %}
WHERE
    {{ incremental_predicate('block_time') }}
{% endif %}
GROUP BY
    blockchain
    , cast(date_trunc('month', block_time) as date)
    , address
    , "from"