{% macro balances_fungible_noncompliant(transfers_rolling_day) %}
SELECT  
    DISTINCT token_address
FROM 
{{ transfers_rolling_day }}
WHERE round(amount/power(10, 18), 6) < -0.001
{% endmacro %}
