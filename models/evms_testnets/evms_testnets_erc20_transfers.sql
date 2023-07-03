{{ config(
        tags = ['dunesql'],
        alias = alias('erc20_transfers'),
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "evms_testnets",
                                    \'["hildobby", "msilb7"]\') }}'
        )
}}

{% set erc20_transfers_models = [
     ('goerli', source('erc20_goerli', 'evt_transfer'))
] %}

SELECT *
FROM (
        {% for erc20_transfers_model in erc20_transfers_models %}
        SELECT
        '{{ erc20_transfers_model[0] }}' AS blockchain
        , *
        FROM {{ erc20_transfers_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );