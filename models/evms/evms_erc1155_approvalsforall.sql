{{ config(
        
        alias = 'erc1155_approvalsforall',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo", "base", "goerli", "zksync", "zora", "scroll"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set erc1155_approvalforalls_models = [
     ('ethereum', source('erc1155_ethereum', 'evt_ApprovalForAll'))
     , ('polygon', source('erc1155_polygon', 'evt_ApprovalForAll'))
     , ('bnb', source('erc1155_bnb', 'evt_ApprovalForAll'))
     , ('avalanche_c', source('erc1155_avalanche_c', 'evt_ApprovalForAll'))
     , ('gnosis', source('erc1155_gnosis', 'evt_ApprovalForAll'))
     , ('fantom', source('erc1155_fantom', 'evt_ApprovalForAll'))
     , ('optimism', source('erc1155_optimism', 'evt_ApprovalForAll'))
     , ('arbitrum', source('erc1155_arbitrum', 'evt_ApprovalForAll'))
     , ('celo', source('erc1155_celo', 'evt_ApprovalForAll'))
     , ('base', source('erc1155_base', 'evt_ApprovalForAll'))
     , ('goerli', source('erc1155_goerli', 'evt_ApprovalForAll'))
     , ('zksync', source('erc1155_zksync', 'evt_ApprovalForAll'))
     , ('zora', source('erc1155_zora', 'evt_ApprovalForAll'))
     , ('scroll', source('erc1155_scroll', 'evt_ApprovalForAll'))
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