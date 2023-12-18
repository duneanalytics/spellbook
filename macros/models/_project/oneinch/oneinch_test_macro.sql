{% macro 
    oneinch_test_macro(
        blockchain
    ) 
%}



{% set transfer_selector = '0xa9059cbb' %}
{% set transfer_from_selector = '0x23b872dd' %}
{% set selector = 'substr(input, 1, 4)' %}



select
    '{{ blockchain }}' as blockchain
    , block_number
    , block_time
    , tx_hash
    , trace_address as transfer_trace_address
    , if(value > uint256 '0', 0xae, "to") as contract_address
    , date(date_trunc('month', block_time)) as block_month
from {{ source(blockchain, 'traces') }}
where (
        {{ selector }} = {{ transfer_selector }} and length(input) = 68
        or {{ selector }} = {{ transfer_from_selector }} and length(input) = 100
        or value > uint256 '0'
    )
    and call_type = 'call'
    and tx_success
    and success
    and "to" = 0x1111111254EEB25477B68fb85Ed929f73A960582

{% endmacro %}