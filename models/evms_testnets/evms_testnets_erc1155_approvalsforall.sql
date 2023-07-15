{{ config(
        tags = ['dunesql'],
        alias = alias('erc1155_approvalsforall'),
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "emvs_testnets",
                                    \'["hildobby","msilb7"]\') }}'
        )
}}

{% set erc1155_approvalforalls_models = [
     ('goerli', source('erc1155_goerli', 'evt_ApprovalForAll'))

] %}

SELECT *
FROM (
        {% for erc1155_approvalforalls_model in erc1155_approvalforalls_models %}
        SELECT
        '{{ erc1155_approvalforalls_model[0] }}' AS blockchain
        , contract_address
        , evt_tx_hash
        , evt_index
        , evt_block_time
        , evt_block_number
        , approved
        , account
        , operator
        FROM {{ erc1155_approvalforalls_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );