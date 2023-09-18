{% macro balances_erc721_noncompliant(transfers_erc721_rolling_day) %}

with

multiple_owners as (
    select 
      blockchain,
      token_address,
      token_id,
      count(wallet_address) as holder_count --should always be 1
    from {{ transfers_erc721_rolling_day }}
    where recency_index = 1
      and amount = 1
    group by 1,2,3
    having count(wallet_address) > 1
)

select distinct token_address from multiple_owners

{% endmacro %}
