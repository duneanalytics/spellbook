{% macro dex_liquidity(
    blockchain = null
    , pools_model = null
    , balances_model = null 
    )
%}

WITH 

unnest_pool as (
    SELECT 
        p.blockchain,
        p.project,
        p.version,
        p.pool,
        t.token as token_address 
    FROM 
    {{ pools_model }} p 
    CROSS JOIN UNNEST(p.tokens) AS t(token)
)

SELECT 
    b.day,
    up.blockchain,
    up.project,
    up.version,
    up.pool,
    up.token_address,
    b.balance,
    b.balance_raw,
    b.balance_usd,
    b.type
FROM 
unnest_pool up 
LEFT JOIN 
{{ balances_model }} b 
    ON up.pool = b.wallet_address 
    AND up.token_address = b.token_address
{% if is_incremental() %}
WHERE {{ incremental_predicate('b.day') }}
{% endif %}

{% endmacro %}