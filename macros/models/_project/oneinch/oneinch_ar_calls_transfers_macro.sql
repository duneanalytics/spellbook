{% macro 
    oneinch_ar_calls_transfers_macro(
        blockchain,
        project_start_date_str,
        wrapper_token_address
    ) 
%}



{% set project_start_date = "timestamp '" + project_start_date_str + "'" %} 
{% set lookback_days = -7 %}
{% set transfer_selector = '0xa9059cbb' %}
{% set transfer_from_selector = '0x23b872dd' %}
{% set selector = 'substr(input, 1, 4)' %}



with

calls as (
    select 
        tx_hash
        , gr_traces.start
        , gr_traces.caller
        , transactions.tx_from
        , transactions.tx_success
        , gr_traces.call_success
        , gr_traces.call_selector
        , gr_traces.call_input
        , gr_traces.call_output
        , transactions.block_time
    from (
        select 
            "from" as tx_from
            , "hash" as tx_hash
            , success as tx_success
            , block_time 
        from {{ source(blockchain, 'transactions') }}
        where
        {% if is_incremental() %}
            block_time >= cast(date_add('day', {{ lookback_days }}, now()) as timestamp)
        {% else %}
            block_time >= {{ project_start_date }}
        {% endif %}
    ) as transactions 
    join (
        select 
            tx_hash
            , min(trace_address) as start
            , min_by("from", trace_address) as caller
            , min_by(success, trace_address) as call_success
            , min_by({{ selector }}, trace_address) as call_selector
            , min_by(input, trace_address) as call_input
            , min_by(output, trace_address) as call_output
        from {{ source(blockchain, 'traces') }}
        where 
            {% if is_incremental() %}
                block_time >= cast(date_add('day', {{ lookback_days }}, now()) as timestamp)
            {% else %}
                block_time >= {{ project_start_date }}
            {% endif %}
            and "to" in (
                select distinct contract_address from {{ ref('oneinch_protocols') }}
                where protocol = 'AR' and blockchain = '{{ blockchain }}' and main
            )
            and call_type = 'call'
        group by tx_hash
    ) gr_traces using(tx_hash)
)


, merged as (
    select 
        '{{ blockchain }}' as blockchain
        , calls.block_time
        -- tx
        , calls.tx_hash
        , calls.tx_from
        , calls.tx_success
        -- call
        , calls.call_success
        , calls.start as call_trace_address
        , calls.caller
        , calls.call_selector
        -- transfer
        , transfers.trace_address as transfer_trace_address
        , transfers.contract_address
        , transfers.amount
        , transfers.native_token
        , transfers.transfer_from
        , transfers.transfer_to
        , if(transfers.trace_address is not null, row_number() over(partition by calls.tx_hash order by transfers.trace_address asc)) as rn_ta_asc 
        , if(transfers.trace_address is not null, row_number() over(partition by calls.tx_hash order by transfers.trace_address desc)) as rn_ta_desc
        -- ext
        , calls.call_output
        , calls.call_input
        , date_trunc('minute', calls.block_time) as minute
        , date(date_trunc('month', calls.block_time)) as block_month
    from calls
    left join (
        select 
            tx_hash
            , if(value > uint256 '0', {{ wrapper_token_address }}, "to") as contract_address
            , if(value > uint256 '0', true, false) as native_token
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
            , trace_address
            , input
            , success
            , call_type
        from {{ source(blockchain, 'traces') }}
        where 
            {% if is_incremental() %}
                block_time >= date_add('day', {{ lookback_days }}, now())
            {% else %}
                block_time >= {{ project_start_date }}
            {% endif %}
            and ({{ selector }} in ({{ transfer_selector }}, {{ transfer_from_selector }}) or value > uint256 '0')
            and tx_success
            and success
    ) transfers on transfers.tx_hash = calls.tx_hash
        and slice(transfers.trace_address, 1, cardinality(calls.start)) = calls.start 
)


select 
    *
    , cast(tx_hash as varchar)||'--'||
        array_join(call_trace_address, '_')||'--'||
        array_join(coalesce(transfer_trace_address, array[-1]), '_')
    as unique_call_transfer_id
from merged



{% endmacro %}


