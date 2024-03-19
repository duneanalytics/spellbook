{% macro dex_liquidity(
    blockchain = null
    , pools_model = null
    , balances_model = null 
    )
%}

SELECT 
    b.day,
    p.blockchain,
    p.project,
    p.version,
    p.pool,
    b.token_address,
    b.token_symbol,
    b.balance,
    b.balance_usd
FROM 
{{ pools_model }} p 
LEFT JOIN 
{{ balances_model }} b 
    ON p.pool = b.address 
    AND b.token_standard = 'erc20'
{% if is_incremental() %}
WHERE {{ incremental_predicate('b.day') }}
{% endif %}

{% endmacro %}