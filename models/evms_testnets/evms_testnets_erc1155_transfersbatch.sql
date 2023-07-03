{{ config(
        tags = ['dunesql'],
        alias = alias('erc1155_transfersbatch'),
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "evms_testnets",
                                    \'["hildobby", "msilb7"]\') }}'
        )
}}

{% set erc1155_batchtransfers_models = [
     ('goerli', source('erc1155_goerli', 'evt_transferbatch'))
] %}

SELECT *
FROM (
        {% for erc1155_batchtransfers_model in erc1155_batchtransfers_models %}
        SELECT
        '{{ erc1155_batchtransfers_model[0] }}' AS blockchain
        , *
        FROM {{ erc1155_batchtransfers_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );