{{ config(
        alias ='erc721_approvalsforall',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set erc721_approvalforalls_models = [
     ('ethereum', source('erc721_ethereum', 'evt_ApprovalForAll'))
     , ('polygon', source('erc721_polygon', 'evt_ApprovalForAll'))
     , ('bnb', source('erc721_bnb', 'evt_ApprovalForAll'))
     , ('avalanche_c', source('erc721_avalanche_c', 'evt_ApprovalForAll'))
     , ('gnosis', source('erc721_gnosis', 'evt_ApprovalForAll'))
     , ('fantom', source('erc721_fantom', 'evt_ApprovalForAll'))
     , ('optimism', source('erc721_optimism', 'evt_ApprovalForAll'))
     , ('arbitrum', source('erc721_arbitrum', 'evt_ApprovalForAll'))
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