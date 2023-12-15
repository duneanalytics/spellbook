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
    select 
        wrapped_native_token_address
        , first_deploy_at
    from {{ ref('oneinch_meta_blockchains') }}
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
    select * from calls
    join {{ oneinch_parsed_transfers_from_calls_macro(blockchain) }} transfers on 
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