{%- macro gmx_add_tx_columns(model_cte, blockchain, columns = []) -%}

SELECT 
    model.*
    {% for column in columns %}
    , tx."{{column}}" as tx_{{column}}
    {% endfor %}
FROM {{model_cte}} AS model
INNER JOIN {{source(blockchain, 'transactions')}} AS tx
    ON model.tx_hash = tx.hash

{%- endmacro -%}
