{{ config(
        tags = ['dunesql'],
        alias = alias('creation_traces'),
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby","msilb7"]\') }}'
        )
}}

{% set creation_traces_models = [
     ('goerli', source('goerli', 'creation_traces'))

] %}

SELECT *
FROM (
        {% for creation_traces_model in creation_traces_models %}
        SELECT
        '{{ creation_traces_model[0] }}' AS blockchain
        , block_time
        , block_number
        , tx_hash
        , address
        , "from"
        , code
        --, tx_from
        --, tx_to
        FROM {{ creation_traces_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );