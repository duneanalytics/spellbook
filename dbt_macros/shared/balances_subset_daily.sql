{%- macro balances_subset_daily(
        blockchain,
        token_address,
        start_date
    )
%}

with
filtered_balances as (
    select
        CAST(address AS varchar) as pool_address,
        balance as token_balance,
        day as snapshot_day
    from {{ source('tokens_' ~ blockchain, 'balances_daily') }}
    where
        CAST(token_address AS varchar) = CAST({{ token_address }} AS varchar)
        {% if is_incremental() %}
        and {{ incremental_predicate('day') }}
        {% else %}
        and day >= DATE '{{ start_date }}'
        {% endif %}
)

select * from filtered_balances
{% endmacro %}