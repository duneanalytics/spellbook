{{ config(
        tags = ['dunesql'],
        alias = alias('transactions'),
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "emvs_testnets",
                                    \'["hildobby","msilb7"]\') }}'
        )
}}

{% set transactions_models = [
     ('goerli', source('goerli', 'transactions'))

] %}

SELECT *
FROM (
         {% for transactions_model in transactions_models %}
        SELECT
        '{{ transactions_model[0] }}' AS blockchain
        , access_list
        , block_hash
        , data
        , "from"
        , hash
        , to
        , block_number
        , block_time
        , gas_limit
        , CAST(gas_price AS double) AS gas_price
        , gas_used
        , index
        , max_fee_per_gas
        , max_priority_fee_per_gas
        , nonce
        , priority_fee_per_gas
        , success
        , "type"
        , CAST(value AS double) AS value
        , CAST(NULL as varbinary) AS l1_tx_origin
        , CAST(NULL AS double) AS l1_fee_scalar
        , CAST(NULL AS DECIMAL(38,0)) AS l1_block_number
        , CAST(NULL AS DECIMAL(38,0)) AS l1_fee
        , CAST(NULL AS DECIMAL(38,0)) AS l1_gas_price
        , CAST(NULL AS DECIMAL(38,0)) AS l1_gas_used
        , cast(NULL as timestamp) AS l1_timestamp
        , CAST(NULL AS DECIMAL(38,0)) AS effective_gas_price
        FROM {{ transactions_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}

        );