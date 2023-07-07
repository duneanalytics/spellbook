{{ config(
        tags = ['dunesql'],
        alias = alias('creation_traces'),
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "evms_testnets",
                                    \'["hildobby", "msilb7"]\') }}'
        )
}}

{% set creation_traces_models = [
     ('goerli', source('goerli', 'creation_traces'), source('goerli', 'transactions'))
] %}

SELECT *
FROM (
        {% for creation_traces_model in creation_traces_models %}
        SELECT
        '{{ creation_traces_model[0] }}' AS blockchain
        , ct.block_time
        , ct.block_number
        , ct.tx_hash
        , ct.address
        , ct."from"
        , ct.code
        , txs."from" AS tx_from
        , txs.to AS tx_to
        FROM {{ creation_traces_model[1] }} ct
        INNER JOIN {{ creation_traces_model[2] }} txs
                ON ct.block_number = txs.block_number
                AND ct.tx_hash = txs.hash
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );