{%- macro balances_daily(balances_base) %}
select
    *,
    {{ dbt_utils.generate_surrogate_key(['day', 'type', 'address', 'contract_address', 'token_id']) }} as unique_key
from (
    select
        cast(date_trunc('day', block_time) as date) as day,
        block_number,
        block_time,
        "type",
        "address",
        contract_address,
        token_id,
        amount,
        row_number() OVER (partition by date_trunc('day', block_time), type, address, contract_address, token_id order by block_number desc) as row_number
    from {{ balances_base }} balances
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
) where row_number = 1


{% endmacro %}
