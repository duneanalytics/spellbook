{% macro 
    oneinch_parsed_transfers_from_calls_macro(
        blockchain
    ) 
%}



{% set transfer_selector = '0xa9059cbb' %}
{% set transferFrom_selector = '0x23b872dd' %}
{% set mint_selector = '0x40c10f19' %}
{% set burn_selector = '0x9dc29fac' %}
{% set deposit_selector = '0xd0e30db0' %}
{% set withdraw_selector = '0x2e1a7d4d' %}
{% set selector = 'substr(input, 1, 4)' %}
{% set null_address = '0x0000000000000000000000000000000000000000' %}



with

transfers as (
    select
        '{{ blockchain }}' as blockchain
        , block_number
        , block_time
        , tx_hash
        , trace_address as transfer_trace_address
        , case
            when {{ selector }} = {{ transfer_selector }} then 'transfer'
            when {{ selector }} = {{ transferFrom_selector }} then 'transferFrom'
            when {{ selector }} = {{ mint_selector }} then 'mint'
            when {{ selector }} = {{ burn_selector }} then 'burn'
            when {{ selector }} = {{ deposit_selector }} then 'deposit'
            when {{ selector }} = {{ withdraw_selector }} then 'withdraw'
            else 'native'
        end as type
        , if(value > uint256 '0', 0xae, "to") as contract_address
        , case
            when {{ selector }} in ({{ transfer_selector }}, {{ mint_selector }}, {{ burn_selector }}) then bytearray_to_uint256(substr(input, 37, 32)) -- transfer, mint, burn
            when {{ selector }} = {{ transferFrom_selector }} then bytearray_to_uint256(substr(input, 69, 32)) -- transferFrom
            when {{ selector }} = {{ withdraw_selector }} then bytearray_to_uint256(substr(input, 5, 32)) -- withdraw
            else value -- native, deposit
        end as amount
        , case
            when {{ selector }} in ({{ transferFrom_selector }}, {{ burn_selector }}) then substr(input, 17, 20) -- transferFrom, burn
            when {{ selector }} = {{ mint_selector }} then {{ null_address }} -- mint
            when {{ selector }} = {{ withdraw_selector }} then "from" -- withdraw
            else "from" -- transfer, native, deposit
        end as transfer_from
        , case
            when {{ selector }} in ({{ transfer_selector }}, {{ mint_selector }}) then substr(input, 17, 20) -- transfer, mint
            when {{ selector }} = {{ transferFrom_selector }} then substr(input, 49, 20) -- transferFrom
            when {{ selector }} = {{ burn_selector }} then {{ null_address }} -- burn
            when {{ selector }} = {{ withdraw_selector }} then "to" -- withdraw
            else "to" -- native, deposit
        end as transfer_to
    from {{ source(blockchain, 'traces') }}
    where
        (
            length(input) = 68 and {{ selector }} in ({{ transfer_selector }}, {{ mint_selector }}, {{ burn_selector }}) -- transfer, mint, burn
            or length(input) = 100 and {{ selector }} = {{ transferFrom_selector }} -- transferFrom
            or length(input) = 36 and {{ selector }} = {{ withdraw_selector }}  -- withdraw
            or value > uint256 '0' -- native, deposit
        )
        and call_type = 'call'
        and (tx_success or tx_success is null)
        and success
)
-- the wrapper deposit includes two transfers: native and wrapper

-- output

select *
from transfers

union all

-- adding wrapper transfers when deposit
select
    blockchain
    , block_number
    , block_time
    , tx_hash
    , transfer_trace_address
    , type
    , transfer_to as contract_address
    , amount
    , transfer_to as transfer_from
    , transfer_from as transfer_to
from transfers
join (select wrapped_native_token_address as transfer_to from ({{ oneinch_blockchain_macro(blockchain) }})) using(transfer_to)
where
    type = 'deposit'

{% endmacro %}