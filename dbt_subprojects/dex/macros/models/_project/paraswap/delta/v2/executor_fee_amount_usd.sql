{% macro gas_fee_usd() %}
 COALESCE( 
        d.price * w.executorFeeAmount / POWER(10, d.decimals), -- compute directly (dest token is gas fee compensation if it's augustus axecutor)
        s.price *  w.srcAmount / POWER(10, s.decimals) * CAST (w.executorFeeAmount AS DECIMAL) / (CAST (w.destAmount AS DECIMAL)), -- compute pro-rata based on src token if it's augustus executor
        n.price * tx.gas_used * tx.gas_price / POWER(10, n.decimals) / COALESCE(tx.order_count, 1), -- divide by number of orders in tx if multiple
        0
    )  AS gas_fee_usd
{% endmacro %}
