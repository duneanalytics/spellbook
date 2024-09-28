{% macro 
    oneinch_call_transfers_macro(
        blockchain
    ) 
%}



with

meta as (
    select 
        wrapped_native_token_address
        , first_deploy_at
    from ({{ oneinch_blockchain_macro(blockchain) }})
)

, calls as (
    select * from ({{ oneinch_calls_macro(blockchain) }})
    where
        tx_success
        and call_success
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% endif %}
)

, transfers as (
    select * from ({{ oneinch_parsed_transfers_from_calls_macro(blockchain) }})
    where
        {% if is_incremental() %}
            {{ incremental_predicate('block_time') }}
        {% else %}
            block_time >= (select first_deploy_at from meta)
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