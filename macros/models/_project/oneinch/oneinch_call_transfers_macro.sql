{% macro 
    oneinch_call_transfers_macro(
        blockchain
    ) 
%}



{% set transfer_selector = '0xa9059cbb' %}
{% set transfer_from_selector = '0x23b872dd' %}
{% set selector = 'substr(input, 1, 4)' %}



with

meta as (
    select * from {{ ref('oneinch_meta_blockchains') }}
    where blockchain = '{{ blockchain }}'
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

, merging as (
    select *
    from calls
    join (

        select 
            tx_hash as transfer_tx_hash
            , block_number as transfer_block_number
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
        where
            {% if is_incremental() %}
                {{ incremental_predicate('block_time') }}
            {% else %}
                block_time >= (select first_deploy_at from meta)
            {% endif %}
            and (
                {{ selector }} = {{ transfer_selector }} and length(input) = 68
                or {{ selector }} = {{ transfer_from_selector }} and length(input) = 100
                or value > uint256 '0'
            )
            and call_type = 'call'
            and tx_success
            and success
            -- and (block_number, tx_hash) in (select block_number, tx_hash from calls)
    ) transfers on 
        transfer_block_number = block_number
        and transfer_tx_hash = tx_hash
        and slice(transfer_trace_address, 1, cardinality(call_trace_address)) = call_trace_address
)

-- output --

select 
    blockchain
    , block_number
    , block_time
    , tx_hash
    , tx_from
    , tx_to
    -- , tx_success
    -- , tx_nonce
    -- , gas_price
    -- , priority_fee
    -- , contract_name
    -- , protocol
    -- , protocol_version
    , method
    , call_selector
    , call_trace_address
    , call_from
    , call_to
    -- , call_success
    -- , call_gas_used
    -- , call_output
    -- , call_error
    -- , remains
    -- , maker
    -- , receiver
    -- , src_token_address
    -- , src_amount
    -- , dst_token_address
    -- , dst_amount
    -- , fusion
    -- , order_hash
    , transfer_trace_address
    , if(contract_address = 0xae, wrapped_native_token_address, contract_address) as contract_address
    , amount
    -- , if(contract_address = 0xae, true, false) as transfer_native
    , if(contract_address = 0xae, 'native', 'erc20') as token_standard -- bep20 or poher?
    , transfer_from
    , transfer_to
    , if(
        coalesce(transfer_from, transfer_to) is not null
        , count(*) over(partition by blockchain, tx_hash, call_trace_address, array_join(array_sort(array[transfer_from, transfer_to]), ''))
    ) as transfers_between_players
    , date_trunc('minute', block_time) as minute
    , date(date_trunc('month', block_time)) as block_month
from merging
join meta using(blockchain)

{% endmacro %}