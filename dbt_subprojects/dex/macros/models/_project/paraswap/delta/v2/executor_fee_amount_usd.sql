{% macro gas_fee_usd() %}
 COALESCE( 
        d.price * w.executorFeeAmount / POWER(10, d.decimals), -- compute directly (dest token is gas fee compensation if it's augustus axecutor)
        s.price *  w.srcAmount / POWER(10, s.decimals) * CAST (w.executorFeeAmount AS DECIMAL) / (CAST (w.destAmount AS DECIMAL)), -- compute pro-rata based on src token if it's augustus executor
        -- TODO: also add 3rd party executor fee compensation -- based on spent native token, divided by amount of orders if multiple
        0
    )  AS gas_fee_usd
{% endmacro %}
