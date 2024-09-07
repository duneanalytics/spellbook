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
    select
        *
        , coalesce(withdrawal.blockchain, blockchain) as result_blockchain
        , coalesce(withdrawal.block_number, block_number) as result_block_number
        , coalesce(withdrawal.block_time, block_time) as result_block_time
        , coalesce(withdrawal.tx_hash, tx_hash) as result_tx_hash
        , coalesce(withdrawal.trace_address, call_trace_address) as result_trace_address
    from ({{ oneinch_calls_macro(blockchain) }})
    left join unnest(withdrawals) as w(withdrawal) on true
    where
        tx_success
        and call_success
        and coalesce(withdrawal.blockchain, blockchain) = '{{ blockchain }}'
        {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
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
        , calls.call_trace_address
        , calls.dst_blockchain
        , calls.hashlock
        , calls.withdrawal
        , transfers.blockchain as transfer_blockchain
        , transfers.block_number as transfer_block_number
        , transfers.block_time as transfer_block_time
        , transfers.tx_hash as transfer_tx_hash
        , transfers.transfer_trace_address
        , contract_address
        , amount
        , transfer_from
        , transfer_to
    from calls
    join transfers on 
        transfers.block_number = calls.result_block_number
        and transfers.tx_hash = calls.result_tx_hash
        and slice(transfer_trace_address, 1, cardinality(result_trace_address)) = result_trace_address
)

-- output --

select 
    blockchain
    , block_number
    , block_time
    , tx_hash
    , call_trace_address
    , dst_blockchain
    , hashlock
    , withdrawal
    , transfer_blockchain
    , transfer_block_number
    , transfer_block_time
    , transfer_tx_hash
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
    , date_trunc('minute', transfer_block_time) as minute
    , date(date_trunc('month', transfer_block_time)) as block_month
from merging, meta

{% endmacro %}