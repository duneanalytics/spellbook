{% macro 
    oneinch_parsed_transfers_from_calls_macro(
        blockchain
    ) 
%}



{% set transfer_selector = '0xa9059cbb' %}
{% set transferFrom_selector = '0x23b872dd' %}
{% set mint_selector = '0x40c10f19' %}
{% set burn_selector = '0x9dc29fac' %}
{% set selector = 'substr(input, 1, 4)' %}
{% set null_address = '0x0000000000000000000000000000000000000000' %}



select
    '{{ blockchain }}' as blockchain
    , block_number
    , block_time
    , tx_hash
    , trace_address as transfer_trace_address
    , if(value > uint256 '0', 0xae, "to") as contract_address
    , case
        when {{ selector }} in ({{ transfer_selector }}, {{ mint_selector }}, {{ burn_selector }}) then bytearray_to_uint256(substr(input, 37, 32)) -- transfer, mint, burn
        when {{ selector }} = {{ transferFrom_selector }} then bytearray_to_uint256(substr(input, 69, 32)) -- transferFrom
        else value -- native
    end as amount
    , case
        when {{ selector }} in ({{ transferFrom_selector }}, {{ burn_selector }}) then substr(input, 17, 20) -- transferFrom, burn
        when {{ selector }} = {{ mint_selector }} then {{ null_address }} -- mint
        else "from" -- transfer, native
    end as transfer_from
    , case
        when {{ selector }} in ({{ transfer_selector }}, {{ mint_selector }}) then substr(input, 17, 20) -- transfer, mint
        when {{ selector }} = {{ transferFrom_selector }} then substr(input, 49, 20) -- transferFrom
        when {{ selector }} = {{ burn_selector }} then {{ null_address }} -- burn
        else "to" -- native
    end as transfer_to
from {{ source(blockchain, 'traces') }}
where (
        {{ selector }} = {{ transfer_selector }} and length(input) = 68 -- transfer
        or {{ selector }} = {{ transferFrom_selector }} and length(input) = 100 -- transferFrom
        or {{ selector }} = {{ mint_selector }} and length(input) = 68 -- mint
        or {{ selector }} = {{ burn_selector }} and length(input) = 68 -- burn
        or value > uint256 '0'
    )
    and call_type = 'call'
    and tx_success
    and success

{% endmacro %}