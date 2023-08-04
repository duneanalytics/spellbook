{{ config( 
    schema = 'oneinch_bnb',
    alias = alias('calls_transfers'),
    tags = ['dunesql'],
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'start', '_transfer_trace_address_not_null', 'block_month']
    )
}}

{% set project_start_date = "timestamp '2020-11-13'" %} 
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
        , transactions.block_time
    from (
        select 
            "from" as tx_from
            , "hash" as tx_hash
            , success as tx_success
            , block_time from {{ source('bnb', 'transactions') }}
        where
        {% if is_incremental() %}
            block_time >= cast(date_add('day', {{lookback_days}}, now()) as timestamp)
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
            , min_by({{selector}}, trace_address) as call_selector
        from {{ source('bnb', 'traces') }}
        where 
            {% if is_incremental() %}
                block_time >= cast(date_add('day', {{lookback_days}}, now()) as timestamp)
            {% else %}
                block_time >= {{ project_start_date }}
            {% endif %}
            and "to" in (
                select distinct contract_address from {{ ref('oneinch_contract_addresses') }}
                where project = '1inch'
            )
            and call_type = 'call'
        group by tx_hash
    ) gr_traces using(tx_hash)
)

, merged as (
    select 
        calls.tx_hash
        , calls.block_time
        , calls.tx_from
        , calls.start
        , transfers.trace_address as transfer_trace_address
        , transfers.token_address
        , transfers.amount
        , transfers.transfer_from
        , transfers.transfer_to
        , calls.caller
        , calls.call_selector
        , calls.call_success
        , calls.tx_success
        , if(transfers.trace_address is not null, row_number() over(partition by calls.tx_hash order by transfers.trace_address asc)) as rn_ta_asc 
        , if(transfers.trace_address is not null, row_number() over(partition by calls.tx_hash order by transfers.trace_address desc)) as rn_ta_desc
        , date(date_trunc('month', calls.block_time)) as block_month
        , coalesce(transfers.trace_address, array[-1]) as _transfer_trace_address_not_null
    from calls
    left join (
        select 
            block_time
            , tx_hash
            , case
                when value > uint256 '0' then 0xae
                else "to"
            end as token_address
            , case {{selector}}
                when {{transfer_selector}} then bytearray_to_uint256(substr(input, 37, 32)) -- transfer
                when {{transfer_from_selector}} then bytearray_to_uint256(substr(input, 69, 32)) -- transferFrom
                else value
            end as amount
            , case
                when {{selector}} = {{transfer_selector}} or value > uint256 '0' then "from" -- transfer
                when {{selector}} = {{transfer_from_selector}} then substr(input, 17, 20) -- transferFrom
            end as transfer_from
            , case
                when {{selector}} = {{transfer_selector}} then substr(input, 17, 20) -- transfer
                when {{selector}} = {{transfer_from_selector}} then substr(input, 49, 20) -- transferFrom
                when value > uint256 '0' then "to"
            end as transfer_to
            , trace_address
            , input
            , success
            , call_type
        from {{ source('bnb', 'traces') }}
        where 
            {% if is_incremental() %}
                block_time >= date_add('day', {{lookback_days}}, now())
            {% else %}
                block_time >= {{ project_start_date }}
            {% endif %}
            and ({{selector}} in ({{transfer_selector}}, {{transfer_from_selector}}) or value > uint256 '0')
            and tx_success
            and success
    ) transfers on transfers.tx_hash = calls.tx_hash
        and slice(transfers.trace_address, 1, cardinality(calls.start)) = calls.start
        
)

select * from merged

