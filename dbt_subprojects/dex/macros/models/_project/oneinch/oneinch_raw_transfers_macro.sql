{% macro oneinch_raw_transfers_macro(blockchain) %}

{% set meta = oneinch_meta_cfg_macro() %}



with

calls as (
    select *
        , array_agg(call_trace_address) over(partition by block_number, tx_hash) as call_trace_addresses -- need all streams for this
    from (
        {% for stream, stream_data in meta['streams'].items() if blockchain in stream_data['exposed'] %}
            -- STREAM: {{ stream }} --
            {% set date_from = stream_data['start']['transfers'] %}
            select
                block_number
                , block_date
                , tx_hash
                , call_trace_address
                , call_to
                , protocol
                , contract_name
                , call_method
                , call_selector
            from {{ ref('oneinch_' + blockchain + '_' + stream + '_raw_calls') }}
            where true
                and call_success
                and block_time >= timestamp '{{ date_from }}' -- it is only needed for simple/easy dates
                {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
            
            {% if not loop.last %}union{% endif %}
            
        {% endfor %}
    )
)

, transfers as (
    select *
    from ({{ oneinch_ptfc_macro(blockchain) }}) -- filters and incremental logic within a macro
)

, merging as (
    select
        blockchain
        , block_number
        , block_time
        , tx_hash
        , call_trace_address
        , call_to
        , protocol
        , contract_name
        , call_method
        , call_selector
        , transfer_trace_address
        , contract_address as transfer_contract_address -- original
        , if(token_standard = 'native', {{ meta['blockchains']['wrapped_native_token_address'][blockchain] }}, contract_address) as contract_address
        , if(token_standard = 'native', {{ meta['blockchains']['native_token_symbol'][blockchain] }}) as native_symbol
        , amount
        , transfer_from
        , transfer_to
        , date_trunc('minute', block_time) as minute
        , block_date
        , slice(transfer_trace_address, 1, cardinality(call_trace_address)) = call_trace_address as nested -- nested transfers only
        , reduce(call_trace_addresses, call_trace_address, (r, x) -> if(slice(transfer_trace_address, 1, cardinality(x)) = x and x > r, x, r), r -> r) = call_trace_address as related -- transfers related to the call only, i.e. without transfers in nested calls
    from calls
    join transfers using(block_date, block_number, tx_hash)
)

, tokens as (
    select
        contract_address
        , symbol as token_symbol
        , decimals as token_decimals
    from {{ source('tokens', 'erc20') }}
    where blockchain = '{{ blockchain }}'
)

, prices as (
    select
        contract_address
        , minute
        , price
        , decimals
        , symbol
    from {{ source('prices', 'usd') }}
    where true
        and blockchain = '{{ blockchain }}'
        and minute >= least({% for stream_data in meta['streams'].values() %}date('{{ stream_data['start']['transfers'] }}'){% if not loop.last %}, {% endif %}{% endfor %})
        {% if is_incremental() %}and {{ incremental_predicate('minute') }}{% endif %}
)

, trusted_tokens as (
    select
        contract_address
        , true as trusted
    from {{ source('prices', 'trusted_tokens') }}
    where blockchain = '{{ blockchain }}'
    group by 1, 2
)

-- output --

select 
    blockchain
    , block_number
    , block_time
    , tx_hash
    , call_trace_address
    , call_selector
    , call_method
    , call_to
    , protocol
    , contract_name
    , transfer_trace_address
    , transfer_contract_address
    , transfer_native
    , transfer_from
    , transfer_to
    , coalesce(native_symbol, symbol, token_symbol) as transfer_symbol
    , transfer_amount
    , amount * price / pow(10, coalesce(token_decimals, decimals)) as transfer_amount_usd
    , coalesce(token_decimals, decimals) as transfer_decimals
    , coalesce(trusted, false) as trusted
    , price
    , block_date
    , date(date_trunc('month', block_time)) as block_month
from merging
left join prices using(contract_address, minute)
left join tokens using(contract_address)
left join trusted_tokens using(contract_address)

{% endmacro %}