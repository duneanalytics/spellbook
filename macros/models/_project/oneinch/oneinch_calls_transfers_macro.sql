{% macro 
    oneinch_calls_transfers_macro(
        blockchain,
        project_start_date_str
    ) 
%}

{% set project_start_date = "timestamp '" + project_start_date_str + "'" %} 
{% set lookback_days = -7 %}
{% set transfer_selector = '0xa9059cbb' %}
{% set transfer_from_selector = '0x23b872dd' %}
{% set selector = 'substr(input, 1, 4)' %}
{% set columns = [
    'blockchain',
    'block_number',
    'block_time',
    'tx_hash',
    'tx_from',
    'tx_to',
    'tx_success',
    'tx_nonce',
    'gas_price',
    'priority_fee',
    'contract_name',
    'protocol',
    'protocol_version',
    'method',
    'call_selector',
    'call_trace_address',
    'call_from',
    'call_to',
    'call_success',
    'call_gas_used',
    'call_output',
    'call_error',
    'remains',
    'wrapped_address',
    'explorer_link'
] %}
{% set columns = columns | join(', ') %}
{% set native_addresses = '(0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)' %}
{% set true_native_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}



with

info as (
    select
        wrapped_native_token_address as wrapped_address
        , native_token_symbol as native_symbol
        , explorer_link
    from {{ ref('evms_info') }}
    where blockchain = '{{ blockchain }}'
)

, settlements as (
    select
        blockchain
        , contract_address as call_from
        , true as fusion
    from {{ ref('oneinch_fusion_settlements') }}
    where blockchain = '{{ blockchain }}'
)

, calls as (
    select
        {{ columns }}
        , null as maker
        , dst_receiver as receiver
        , if(src_token_address in {{native_addresses}}, wrapped_address, src_token_address) as src_token_address
        , if(src_token_address in {{native_addresses}}, native_symbol) as src_native
        , src_amount
        , if(dst_token_address in {{native_addresses}}, wrapped_address, dst_token_address) as dst_token_address
        , if(dst_token_address in {{native_addresses}}, native_symbol) as dst_native
        , dst_amount
        , false as fusion
        , null as order_hash
    from {{ ref('oneinch_' + blockchain + '_ar') }}
    join info on true
    where
        tx_success
        and call_success
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% endif %}

    union all

    select
        {{ columns }}
        , maker
        , receiver
        , if(maker_asset in {{native_addresses}}, wrapped_address, maker_asset) as src_token_address
        , if(maker_asset in {{native_addresses}}, native_symbol) as src_native
        , making_amount as src_amount
        , if(taker_asset in {{native_addresses}}, wrapped_address, taker_asset) as dst_token_address
        , if(taker_asset in {{native_addresses}}, native_symbol) as dst_native
        , taking_amount as dst_amount
        , coalesce(fusion, false) as fusion
        , order_hash
    from {{ ref('oneinch_' + blockchain + '_lop') }}
    join info on true
    left join settlements using(blockchain, call_from)
    where
        tx_success
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
                block_time >= date_add('day', {{ lookback_days }}, now())
            {% else %}
                -- block_time >= {{ project_start_date }}
                block_time >= timestamp '2023-12-01'
            {% endif %}
            and (
                {{ selector }} = {{ transfer_selector }} and length(input) = 68
                or {{ selector }} = {{ transfer_from_selector }} and length(input) = 100
                or value > uint256 '0'
            )
            and call_type = 'call'
            and tx_success
            and success
            and (block_number, tx_hash) in (select block_number, tx_hash from calls)
        
    ) transfers on transfer_tx_hash = tx_hash
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
    , tx_success
    , tx_nonce
    , gas_price
    , priority_fee
    , contract_name
    , protocol
    , protocol_version
    , method
    , call_selector
    , call_trace_address
    , call_from
    , call_to
    , call_success
    , call_gas_used
    , call_output
    , call_error
    , remains
    , maker
    , receiver
    , src_token_address
    , src_native
    , src_amount
    , dst_token_address
    , dst_native
    , dst_amount
    , fusion
    , order_hash
    , transfer_trace_address
    , if(contract_address = 0xae, wrapped_address, contract_address) as contract_address
    , amount
    , if(contract_address = 0xae, true, false) as transfer_native
    , transfer_from
    , transfer_to
    , -1 as transfers_between_players
    , -1 as rn_tta_asc
    , -1 as rn_tta_desc
    -- , if(
    --     coalesce(transfer_from, transfer_to) is not null
    --     , count(*) over(partition by blockchain, tx_hash, call_trace_address, array_join(array_sort(array[transfer_from, transfer_to]), ''))
    -- ) as transfers_between_players
    -- , row_number() over(partition by transfer_tx_hash order by transfer_trace_address asc) as rn_tta_asc
    -- , row_number() over(partition by transfer_tx_hash order by transfer_trace_address desc) as rn_tta_desc
    , date_trunc('minute', block_time) as minute
    , date(date_trunc('month', block_time)) as block_month
    , explorer_link
from merging

{% endmacro %}