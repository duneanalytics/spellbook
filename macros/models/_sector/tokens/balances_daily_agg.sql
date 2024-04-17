{%- macro balances_daily_agg(balances_raw) %}
select
    blockchain,
    day,
    block_number,
    block_time,
    address,
    token_address,
    token_standard,
    token_id,
    balance_raw,
    lead(cast(day as timestamp)) over (partition by token_address,address,token_id order by day asc) as next_update_day,
    {{ dbt_utils.generate_surrogate_key(['day', 'address', 'token_address', 'token_standard', 'token_id']) }} as unique_key
from (
    select
        blockchain,
        cast(date_trunc('day', block_time) as timestamp) as day,
        block_number,
        block_time,
        address,
        token_address,
        token_standard,
        token_id,
        balance_raw,
        row_number() OVER (partition by date_trunc('day', block_time), token_standard, address, token_address, token_id order by block_number desc) as row_number
    from {{ balances_raw}} balances
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
) where row_number = 1


{% endmacro %}
