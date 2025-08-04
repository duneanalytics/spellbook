{% macro gas_fee_usd() %}
  case
    WHEN w.executorFeeAmount > 0
      then
        COALESCE( 
                d.price * w.executorFeeAmount / POWER(10, d.decimals), -- compute directly (dest token is gas fee compensation if it's augustus axecutor)
                s.price *  w.srcAmount / POWER(10, s.decimals) * CAST (w.executorFeeAmount AS DECIMAL) / (CAST (w.destAmount AS DECIMAL)), -- compute pro-rata based on src token if it's augustus executor        
        )
      else
        -- if can't parse executorFeeAmount - fall back to spent native token, divided by amount of orders if multiple
        wnt_usd.price * raw_tx_gas_used * raw_tx_gas_price  / POWER(10, wnt_usd.decimals)
    END AS gas_fee_usd
{% endmacro %}
