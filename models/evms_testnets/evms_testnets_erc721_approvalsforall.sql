{{ config(
        tags = ['dunesql'],
        alias = alias('erc721_approvalsforall'),
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "evms_testnets",
                                    \'["hildobby", "msilb7"]\') }}'
        )
}}

{% set erc721_approvalforalls_models = [
     ('goerli', source('erc721_goerli', 'evt_ApprovalForAll'))
] %}

SELECT *
FROM (
        {% for erc721_approvalforalls_model in erc721_approvalforalls_models %}
        SELECT
        '{{ erc721_approvalforalls_model[0] }}' AS blockchain
        , *
        FROM {{ erc721_approvalforalls_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );