{%- macro balances_latest(balances) %}

select
    {{dbt_utils.generate_surrogate_key(['address', 'token_address', 'token_standard', "token_id"])}} as unique_key,
    blockchain,
    address,
    token_address,
    token_standard,
    token_symbol,
    token_id,
    collection_name,
    max(block_number) as block_number_latest,
    max(date_trunc('day', block_time)) as block_date_latest,
    max(block_time) as block_time_latest,
    max_by(balance_raw, block_number) as balance_raw_latest,
    max_by(balance, block_number) as balance_latest
from {{balances}}
where
    1 = 1
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
group by
    blockchain,
    address,
    token_address,
    token_standard,
    token_symbol,
    token_id,
    collection_name
{% endmacro %}