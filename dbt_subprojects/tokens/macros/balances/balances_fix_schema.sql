{%- macro balances_fix_schema(balances_raw, blockchain, native_token_address='0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') %}
select
    '{{blockchain}}' as blockchain,
    block_number,
    block_time,
    address ,
    coalesce(contract_address, {{native_token_address}}) as token_address,
    type as token_standard,
    token_id,
    amount as balance_raw
from {{ balances_raw }}
{% endmacro %}
