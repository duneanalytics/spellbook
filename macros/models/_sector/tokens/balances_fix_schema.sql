{%- macro balances_fix_schema(balances_raw, blockchain) %}
select
    '{{blockchain}}' as blockchain,
    block_number,
    block_time,
    address ,
    contract_address as token_address,
    type as token_standard,
    token_id,
    balances.amount as balance_raw
from {{ balances_raw }}
{% endmacro %}
