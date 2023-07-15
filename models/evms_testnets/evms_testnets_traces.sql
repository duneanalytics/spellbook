{{ config(
        tags = ['dunesql'],
        alias = alias('traces'),
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "emvs_testnets",
                                    \'["hildobby","msilb7"]\') }}'
        )
}}

{% set traces_models = [
     ('goerli', source('goerli', 'traces'))

] %}

SELECT *
FROM (
        {% for traces_model in traces_models %}
        SELECT
        '{{ traces_model[0] }}' AS blockchain
        , block_time
        , block_number
        , value
        , gas
        , gas_used
        , block_hash
        , success
        , tx_index
        , error
        , tx_success
        , tx_hash
        , "from"
        , to
        , trace_address
        , type
        , address
        , code
        , call_type
        , input
        , output
        , refund_address
        FROM {{ traces_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );