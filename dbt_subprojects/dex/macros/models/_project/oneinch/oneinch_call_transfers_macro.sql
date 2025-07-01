{% macro 
    oneinch_call_transfers_macro(
        blockchain
    ) 
%}

-- base columns to not to duplicate in the union
{% set
    calls_base_columns = [
        'blockchain',
        'block_number',
        'block_time',
        'tx_hash',
        'call_trace_address',
        'dst_blockchain',
        'hashlock',
    ]
%}

with

meta as (
    select 
        wrapped_native_token_address
        , first_deploy_at
    from ({{ oneinch_blockchain_macro(blockchain) }})
)

-- calls with escrow results on all blockchains --
, results as (
    select
        blockchain as result_blockchain
        , block_number as result_block_number
        , block_time as result_block_time
        , tx_hash as result_tx_hash
        , trace_address as result_trace_address
        , hashlock
        , escrow as result_escrow
        , method as result_method
        , amount as result_amount
    from {{ ref('oneinch_escrow_results') }}
    where
        call_success
        and tx_success
        {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %} -- with an incremental predicate, as the results always come after the creations
)

, calls as (
    select
        {{ calls_base_columns | join(', ') }}
        , blockchain as result_blockchain
        , block_number as result_block_number
        , block_time as result_block_time
        , tx_hash as result_tx_hash
        , call_trace_address as result_trace_address
        , cast(null as varbinary) as result_escrow
        , null as result_method
        , null as result_amount
    from ({{ oneinch_calls_macro(blockchain) }})
    where
        tx_success
        and call_success
        {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
    
    union all -- add calls with escrow results

    select
        {{ calls_base_columns | join(', ') }}
        , result_blockchain
        , result_block_number
        , result_block_time
        , result_tx_hash
        , result_trace_address
        , result_escrow
        , result_method
        , result_amount
    from (select * from ({{ oneinch_calls_macro(blockchain) }}) where hashlock is not null)
    join results using(hashlock) -- escrow results only
    where
        tx_success
        and call_success
        and result_escrow in (src_escrow, dst_escrow)
        {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
)

, transfers as (
    select * from ({{ oneinch_parsed_transfers_from_calls_macro(blockchain) }})
    where
        {% if is_incremental() %}
            {{ incremental_predicate('block_time') }}
        {% else %}
            block_time >= greatest((select first_deploy_at from meta), timestamp {{ oneinch_easy_date() }})
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
        , calls.result_escrow
        , calls.result_trace_address
        , calls.result_method
        , calls.result_amount
        , transfers.blockchain as transfer_blockchain
        , transfers.block_number as transfer_block_number
        , transfers.block_time as transfer_block_time
        , transfers.tx_hash as transfer_tx_hash
        , transfers.transfer_trace_address
        , if(contract_address = 0xae, wrapped_native_token_address, contract_address) as contract_address
        , amount
        , if(contract_address = 0xae, true, false) as transfer_native
        , transfer_from
        , transfer_to
        , date_trunc('minute', transfers.block_time) as minute
    from calls
    join meta on true
    join transfers on true
        and transfers.block_number = calls.result_block_number
        and transfers.tx_hash = calls.result_tx_hash
        and slice(transfer_trace_address, 1, cardinality(result_trace_address)) = result_trace_address
)

, tokens as (
    select
        blockchain as transfer_blockchain
        , contract_address
        , symbol as token_symbol
        , decimals as token_decimals
    from {{ source('tokens', 'erc20') }}
    where blockchain = '{{blockchain}}'
)

, prices as (
    select
        blockchain as transfer_blockchain
        , contract_address
        , minute
        , price
        , decimals
        , symbol
    from {{ source('prices', 'usd') }}
    {% if is_incremental() %}
        where {{ incremental_predicate('minute') }}
            and blockchain = '{{blockchain}}'
    {% endif %}
)

, trusted_tokens as (
    select
        blockchain as transfer_blockchain
        , contract_address
        , true as trusted
    from {{ source('prices', 'trusted_tokens') }}
    where blockchain = '{{blockchain}}'
    group by 1, 2, 3
)

{% set symbol = 'coalesce(symbol, token_symbol)' %}
{% set decimals = 'coalesce(token_decimals, decimals)' %}

-- output --

select 
    blockchain
    , block_number
    , block_time
    , tx_hash
    , call_trace_address
    , dst_blockchain
    , hashlock
    , result_escrow
    , result_trace_address
    , result_method
    , result_amount
    , transfer_blockchain
    , transfer_block_number
    , transfer_block_time
    , transfer_tx_hash
    , transfer_trace_address
    , contract_address
    , amount
    , transfer_native
    , transfer_from
    , transfer_to
    , if(
        coalesce(transfer_from, transfer_to) is not null
        , count(*) over(partition by blockchain, tx_hash, call_trace_address, array_join(array_sort(array[transfer_from, transfer_to]), ''))
    ) as transfers_between_players
    , minute
    , {{ symbol }} as symbol
    , {{ decimals }} as decimals
    , amount * price / pow(10, {{ decimals }}) as amount_usd
    , coalesce(trusted, false) as trusted
    , date(date_trunc('month', transfer_block_time)) as block_month
from merging
left join prices using(transfer_blockchain, contract_address, minute)
left join tokens using(transfer_blockchain, contract_address)
left join trusted_tokens using(transfer_blockchain, contract_address)

{% endmacro %}