{% macro add_tx_columns_dynamic(
    model_cte
    , columns = []
    )
%}

WITH blockchain_list AS (
    SELECT DISTINCT blockchain
    FROM {{ model_cte }}
)

, tx_data AS (
    {% raw %}
    {%- set blockchain_query %}
    SELECT DISTINCT blockchain FROM blockchain_list
    {%- endset %}
    
    {%- set results = run_query(blockchain_query) %}
    
    {%- if execute %}
    {%- set blockchains = results.columns[0].values() %}
    {%- else %}
    {%- set blockchains = [] %}
    {%- endif %}

    {% for blockchain in blockchains %}
    SELECT
        model.*
        {% for column in columns %}
        , tx."{{column}}" as tx_{{column}}
        {% endfor %}
    FROM {{model_cte}} model
    INNER JOIN {{source(blockchain, 'transactions')}} tx
        ON model.block_date = tx.block_date
        AND model.block_number = tx.block_number
        AND model.tx_hash = tx.hash
        AND model.blockchain = '{{blockchain}}'
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    {% endraw %}
)

SELECT * FROM tx_data

{% endmacro %}
