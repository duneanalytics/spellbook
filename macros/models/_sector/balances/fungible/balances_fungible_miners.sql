{% macro balances_fungible_miners(blockchain) %}

SELECT 
    COUNT(*) as blocks_mined,
    CAST(date_trunc('month', "time") as date) as block_month
    miner 
FROM 
{{ source( blockchain , 'blocks') }}
{% if is_incremental() %}
    WHERE "time" >= date_trunc('day', now() - interval '3' Day)
{% endif %}
GROUP BY 2, 3 

{% endmacro %}