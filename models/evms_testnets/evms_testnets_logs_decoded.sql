{{ config(
        tags = ['dunesql'],
        alias = alias('logs_decoded'),
        unique_key=['blockchain', 'tx_hash'],
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby","msilb7"]\') }}'
        )
}}

{% set decodedlogs_models = [
     ('goerli', source('goerli', 'logs_decoded'))

] %}

SELECT *
FROM (
        {% for decodedlogs_model in decodedlogs_models %}
        SELECT
        '{{ decodedlogs_model[0] }}' AS blockchain
        , block_time
        , block_number
        , index
        , contract_address
        , event_name
        , namespace
        , signature
        , tx_hash
        FROM {{ decodedlogs_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );