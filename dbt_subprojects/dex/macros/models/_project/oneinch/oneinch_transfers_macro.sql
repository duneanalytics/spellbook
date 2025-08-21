{% macro 
    oneinch_transfers_macro(
        blockchain
    ) 
%}

{% set
    columns = [
        'blockchain',
        'block_number',
        'block_time',
        'tx_hash',
        'date(block_time) as block_date',
    ]
%}

with

meta as (
    select 
        wrapped_native_token_address
        , first_deploy_at
    from ({{ oneinch_blockchain_macro(blockchain) }})
)

, bonded as (
    select {{ columns | join(', ') }}
    from ({{ oneinch_calls_macro(blockchain) }})
    where
        {% if is_incremental() %}
            {{ incremental_predicate('block_time') }}
        {% else %}
            block_time > timestamp {{ oneinch_easy_date() }}
        {% endif %}
        and tx_success
        and call_success

    union

    select {{ columns | join(', ') }}
    from {{ ref('oneinch_' + blockchain + '_escrow_results') }}
    where
        {% if is_incremental() %}
            {{ incremental_predicate('block_time') }}
        {% else %}
            block_time > timestamp {{ oneinch_easy_date() }}
        {% endif %}
        and call_success
        and tx_success
    
    union

    select {{ columns | join(', ') }}
    from {{ ref('oneinch_escrow_dst_creations') }}
    where
        {% if is_incremental() %}
            {{ incremental_predicate('block_time') }}
        {% else %}
            block_time > timestamp {{ oneinch_easy_date() }}
        {% endif %}
        and blockchain = '{{blockchain}}'
        and call_success
)

, transfers as (
    select *
    from ({{ oneinch_parsed_transfers_from_calls_macro(blockchain) }})
    where
        {% if is_incremental() %}
            {{ incremental_predicate('block_time') }}
        {% else %}
            block_time >= greatest((select first_deploy_at from meta), timestamp {{ oneinch_easy_date() }})
        {% endif %}
)

, merging as (
    select 
        blockchain
        , block_number
        , block_time
        , tx_hash
        , block_date
        , transfer_trace_address
        , if(contract_address = 0xae, wrapped_native_token_address, contract_address) as contract_address
        , amount
        , if(contract_address = 0xae, true, false) as transfer_native
        , transfer_from
        , transfer_to
        , date_trunc('minute', block_time) as minute
    from bonded
    join meta on true
    join transfers using(blockchain, block_number, block_time, tx_hash, block_date)
)

, tokens as (
    select
        contract_address
        , symbol as token_symbol
        , decimals as token_decimals
    from {{ source('tokens', 'erc20') }}
    where blockchain = '{{blockchain}}'
)

, prices as (
    select
        contract_address
        , minute
        , price
        , decimals
        , symbol
    from {{ source('prices', 'usd') }}
    where
        {% if is_incremental() %}
            {{ incremental_predicate('minute') }}
        {% else %}
            minute > timestamp {{ oneinch_easy_date() }}
        {% endif %}
        and blockchain = '{{blockchain}}'
)

, trusted_tokens as (
    select
        contract_address
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
    , block_date
    , date(date_trunc('month', transfer_block_time)) as block_month
from merging
left join prices using(contract_address, minute)
left join tokens using(contract_address)
left join trusted_tokens using(contract_address)

{% endmacro %}