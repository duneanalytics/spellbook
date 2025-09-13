{%- macro transfers_traces_wrapped_token_deposit_base(blockchain, transfers_traces_base_table) -%}


-- the wrapper deposit includes two transfers: native and wrapped, so we should add second one manually reversing from/to
-- it's splitted to 2 operations and fetching from pre-materialized table to prevent doubling full-scan of traces 


{%- set token_standard_20 = 'bep20' if blockchain == 'bnb' else 'erc20' %}

-- output
select
    blockchain
    , block_month
    , block_date
    , block_time
    , block_number
    , tx_hash
    , trace_address
    , type
    , '{{ token_standard_20 }}' as token_standard
    , "to" as contract_address
    , amount_raw
    , "to" as transfer_from
    , "from" as transfer_to
    , unique_key
from {{ transfers_traces_base_table }}
join ( -- to leave only real tokens (mostly for wrapped token, but works for rare cases too, like tradable weth fork). 
    select contract_address as "to"
    from {{ source('tokens', 'erc20') }}
    where blockchain = '{{ blockchain }}'
) using("to")
where
    type = 'deposit'
    {% if is_incremental() %} and {{ incremental_predicate('block_time') }}{% endif %}



{%- endmacro -%}