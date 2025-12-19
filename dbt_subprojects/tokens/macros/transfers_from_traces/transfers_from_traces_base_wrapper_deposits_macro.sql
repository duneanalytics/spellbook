{%- macro
    transfers_from_traces_base_wrapper_deposits_macro(
        blockchain,
        transfers_from_traces_base_table
    )
-%}

-- the wrapper deposit includes two transfers: native and wrapped, so we should add second one manually reversing from/to
-- it's splitted to 2 operations and fetching from pre-materialized table to prevent doubling full-scan of traces

-- output --

select
    blockchain
    , block_month
    , block_date
    , block_time
    , block_number
    , tx_hash
    , trace_address
    , type
    , 'erc20' as token_standard
    , "to" as contract_address
    , amount_raw
    , "to"
    , "from"
    , sha1(to_utf8(concat_ws('|'
        , blockchain
        , cast(block_number as varchar)
        , cast(tx_hash as varchar)
        , array_join(trace_address, ',') -- ',' is necessary to avoid similarities after concatenation // array_join(array[1, 0], '') = array_join(array[10], '')
        , cast("to" as varchar)
    ))) as unique_key
from {{ transfers_from_traces_base_table }}
join ( -- to leave only real tokens (mostly for wrapped token, but works for rare cases too, like tradable weth fork). 
    select contract_address as "to"
    from {{ source('tokens', 'erc20') }}
    where blockchain = '{{ blockchain }}'
) using("to")
where true
    and type = 'deposit'
    and contract_address <> "to" -- due to inconsistency in dune.blockchains & tokens.erc20
    {% if is_incremental() -%} and {{ incremental_predicate('block_time') }} {%- endif %}



{%- endmacro -%}