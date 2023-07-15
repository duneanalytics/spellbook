{{ config(
        tags = ['dunesql'],
        alias = alias('logs'),
        unique_key=['blockchain', 'tx_hash'],
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby","msilb7"]\') }}'
        )
}}

{% set logs_models = [
     ('goerli', source('goerli', 'logs'))

] %}

SELECT *
FROM (
        {% for logs_model in logs_models %}
        SELECT
        '{{ logs_model[0] }}' AS blockchain
        , block_time
        , block_number
        , block_hash
        , contract_address
        , topic0
        , topic1
        , topic2
        , topic3
        , data
        , tx_hash
        , index
        , tx_index
        FROM {{ logs_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );