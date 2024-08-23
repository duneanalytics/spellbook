{% macro 
    oneinch_project_calls_macro(
        blockchain
        , date_from = '2024-08-20'
    )
%}

{% set native_addresses = '(0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)' %}



with

static as (
    select
          array['swap', 'settle', 'change', 'exact', 'batch', 'trade', 'sell', 'buy', 'fill', 'route', 'zap', 'symbiosis', 'aggregate', 'multicall', 'execute', 'wrap', 'transform'] as suitables
        , array['add', 'remove', 'mint', 'increase', 'decrease', 'cancel', 'destroy', 'claim', 'rescue', 'withdraw', 'simulate', 'join', 'exit', 'interaction', '721', '1155', 'nft', 'create'] as exceptions
)

, meta as (
    select 
        wrapped_native_token_address
        , native_token_symbol as native_symbol
    from {{ source('oneinch', 'blockchains') }}
    where blockchain = '{{blockchain}}'
)

, contracts as (
    select
        blockchain
        , address as call_to
        , any_value(project) as project
        , any_value(tag) as tag
        , any_value(flags) as flags
    from {{ ref('oneinch_' + blockchain + '_mapped_contracts') }}
    where
        project not in ('MEVBot', 'Unknown')
    group by 1, 2
)

, signatures as (
    select *
    from (
        select
            id as selector
            , min(signature) as signature
            , min(split_part(signature, '(', 1)) as method
        from {{ source('abi', 'signatures') }}
        where length(id) = 4
        group by 1
    )
    join static on true
    where
        not reduce(exceptions, false, (r, x) -> if(position(x in lower(replace(method, '_'))) > 0, true, r), r -> r) -- without "exception" methods
        and reduce(suitables, false, (r, x) -> if(position(x in lower(replace(method, '_'))) > 0, true, r), r -> r) -- "suitable" methods only
)

, calls as (
    select *
    from (
        select
            block_number
            , block_time
            , tx_hash
            , "from" as call_from
            , "to" as call_to
            , trace_address as call_trace_address
            , cardinality(trace_address) = 0 as direct
            , substr(input, 1, 4) as selector
            , success as call_success
            , tx_success
        from {{ source(blockchain, 'traces') }}
        where
            {% if is_incremental() %}
                {{ incremental_predicate('block_time') }}
            {% else %}
                block_time >= timestamp '{{date_from}}'
            {% endif %}
    )
    join contracts using(call_to)
    join signatures using(selector)
    join meta on true
    join (
        select
            block_number
            , block_time
            , hash as tx_hash
            , "from" as tx_from
            , "to" as tx_to
            , gas_used as tx_gas_used
        from {{ source(blockchain, 'transactions') }}
        where
            {% if is_incremental() %}
                {{ incremental_predicate('block_time') }}
            {% else %}
                block_time >= timestamp '{{date_from}}'
            {% endif %}
            
    ) using(block_number, block_time, tx_hash)
)

-- output --

select
    blockchain
    , block_number
    , block_time
    , tx_hash
    , tx_from
    , tx_to
    , tx_success
    , tx_gas_used
    , call_trace_address
    , project
    , tag
    , map_concat(flags, map_from_entries(array[('direct', direct)])) as flags
    , selector as call_selector
    , method
    , signature
    , call_from
    , call_to
    , call_success
    , date(date_trunc('month', block_time)) as block_month
from calls

{% endmacro %}