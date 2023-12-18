{% macro 
    oneinch_call_transfers_macro(
        blockchain
        , blockchain_meta
    ) 
%}



with

meta as (
    select 
        wrapped_native_token_address
        , first_deploy_at
    from {{ blockchain_meta }}
    where blockchain = '{{ blockchain }}'
    limit 1
)

, calls as (
    select * from {{ ref('oneinch_calls') }}
    where
        blockchain = '{{ blockchain }}'
        and tx_success
        and call_success
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% endif %}
)

, transfers as (
    
    select
        '{{ blockchain }}' as blockchain
        , block_number
        , block_time
        , tx_hash
        , trace_address as transfer_trace_address
        , if(value > uint256 '0', 0xae, "to") as contract_address
        , case {{ selector }}
            when {{ transfer_selector }} then bytearray_to_uint256(substr(input, 37, 32))
            when {{ transfer_from_selector }} then bytearray_to_uint256(substr(input, 69, 32))
            else value
        end as amount
        , case
            when {{ selector }} = {{ transfer_selector }} or value > uint256 '0' then "from"
            when {{ selector }} = {{ transfer_from_selector }} then substr(input, 17, 20)
        end as transfer_from
        , case
            when {{ selector }} = {{ transfer_selector }} then substr(input, 17, 20)
            when {{ selector }} = {{ transfer_from_selector }} then substr(input, 49, 20)
            when value > uint256 '0' then "to"
        end as transfer_to
    from {{ source(blockchain, 'traces') }}
    where (
            {{ selector }} = {{ transfer_selector }} and length(input) = 68
            or {{ selector }} = {{ transfer_from_selector }} and length(input) = 100
            or value > uint256 '0'
        )
        and call_type = 'call'
        and tx_success
        and success
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% else %}
            and block_time >= (select first_deploy_at from meta)
        {% endif %}
)

, merging as (
    select 
        calls.blockchain
        , calls.block_number
        , calls.block_time
        , calls.tx_hash
        , call_trace_address
        , transfer_trace_address
        , contract_address
        , amount
        , transfer_from
        , transfer_to
    from calls
    join transfers on 
        calls.block_number = transfers.block_number
        and calls.tx_hash = transfers.tx_hash
        and slice(transfer_trace_address, 1, cardinality(call_trace_address)) = call_trace_address
)

-- output --

select 
    blockchain
    , block_number
    , block_time
    , tx_hash
    , call_trace_address
    , transfer_trace_address
    , if(contract_address = 0xae, wrapped_native_token_address, contract_address) as contract_address
    , amount
    , if(contract_address = 0xae, true, false) as transfer_native
    , transfer_from
    , transfer_to
    , if(
        coalesce(transfer_from, transfer_to) is not null
        , count(*) over(partition by blockchain, tx_hash, call_trace_address, array_join(array_sort(array[transfer_from, transfer_to]), ''))
    ) as transfers_between_players
    , date_trunc('minute', block_time) as minute
    , date(date_trunc('month', block_time)) as block_month
from merging, meta

{% endmacro %}