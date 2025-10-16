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
                , block_month
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

{% if blockchain in meta['blockchains']['aave'] %}, atokens as (
    select
        contract_address -- atoken_address
        , max_by(underlyingAsset, evt_block_time) as underlying_address
        , max_by(aTokenSymbol, evt_block_time) as atoken_symbol
    from {{ source('aave_v3_' + blockchain, 'AToken_evt_Initialized') }}
    where underlyingAsset is not null
    group by 1 -- take the latest event only
){% endif %}

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
        , trace_address as transfer_trace_address
        , contract_address as transfer_contract_address -- original
        , if(token_standard = 'native', {{ meta['blockchains']['wrapped_native_token_address'][blockchain] }}, {% if blockchain in meta['blockchains']['aave'] %}coalesce(underlying_address, contract_address){% else %}contract_address{% endif %}) as contract_address
        , if(token_standard = 'native', {{ meta['blockchains']['native_token_symbol'][blockchain] }}{% if blockchain in meta['blockchains']['aave'] %}, atoken_symbol{% endif %}) as _symbol
        , amount_raw as amount
        , "from" as transfer_from
        , "to" as transfer_to
        , date_trunc('minute', block_time) as minute
        , block_date
        , slice(trace_address, 1, cardinality(call_trace_address)) = call_trace_address as nested -- nested transfers only
        , reduce(call_trace_addresses, call_trace_address, (r, x) -> if(slice(trace_address, 1, cardinality(x)) = x and x > r, x, r), r -> r) = call_trace_address as related -- transfers related to the call only, i.e. without transfers in nested calls
    from calls
    join {{ source('tokens', 'transfers_from_traces') }} using(block_month, block_date, block_number, tx_hash)
    {% if blockchain in meta['blockchains']['aave'] %}left join atokens using(contract_address){% endif %}
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
    , transfer_from
    , transfer_to
    , coalesce(_symbol, symbol, token_symbol) as transfer_symbol
    , amount as transfer_amount
    , amount * price / pow(10, coalesce(token_decimals, decimals)) as transfer_amount_usd
    , coalesce(token_decimals, decimals) as transfer_decimals
    , coalesce(trusted, false) as trusted
    , nested
    , related
    , price
    , block_date
    , date(date_trunc('month', block_time)) as block_month
    , sha1(to_utf8(concat_ws('|'
        , blockchain
        , cast(tx_hash as varchar)
        , array_join(transfer_trace_address, ',')
        , cast(contract_address as varchar)
    ))) as transfer_id
    , sha1(to_utf8(concat_ws('|'
        , blockchain
        , cast(tx_hash as varchar)
        , array_join(call_trace_address, ',')
        , array_join(transfer_trace_address, ',')
        , cast(contract_address as varchar)
    ))) as unique_key
from merging
left join prices using(contract_address, minute)
left join tokens using(contract_address)
left join trusted_tokens using(contract_address)

{% endmacro %}