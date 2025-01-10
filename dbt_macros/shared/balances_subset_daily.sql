{%- macro balances_subset_daily(
        blockchain,
        token_address,
        start_date
    )
%}

with
filtered_balances as (
    select
        address as pool_address,
        balance as token_balance,
        day as snapshot_day
    from {{ source('tokens_' ~ blockchain, 'balances_daily') }}
    where
        token_address = {{ token_address }}
        {% if is_incremental() %}
        and {{ incremental_predicate('day') }}
        {% else %}
        and day >= DATE '{{ start_date }}'  -- Modified this line
        {% endif %}
)

select * from filtered_balances
{% endmacro %}