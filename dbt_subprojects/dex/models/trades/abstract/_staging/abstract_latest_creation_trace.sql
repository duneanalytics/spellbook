{% set blockchain = 'abstract' %}

{{ config(
    schema = blockchain
    , alias = 'latest_creation_trace'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_month', 'address', '"from"']
)
}}

with abstract_creation_traces as (
    select
        '{{blockchain}}' as blockchain
        , block_time
        , block_number
        , tx_hash
        , varbinary_substring(topic3, 13, 32) as address
        , varbinary_substring(topic1, 13, 32) as "from"
        , data as code
        , date_trunc('month', block_date) as block_month
    from
        {{ source(blockchain, 'logs') }}
    where
        contract_address = 0x0000000000000000000000000000000000008006
        and topic0 = 0x290afdae231a3fc0bbae8b1af63698b0a1d79b21ad17df0342dfb952fe74f8e5
        {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
        {% endif %}
)
, latest_trace as (
    select
        blockchain
        , block_month
        , address
        , "from"
        , max(block_number) as latest_block
    from
        abstract_creation_traces
    group by
        blockchain
        , block_month
        , address
        , "from"
)
select
    *
from
    latest_trace