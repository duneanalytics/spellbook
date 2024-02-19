{{ config(
        
        alias = 'erc721_approvals',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo", "base", "goerli", "zksync", "zora", "scroll"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set erc721_approvals_models = [
     ('ethereum', source('erc721_ethereum', 'evt_Approval'))
     , ('polygon', source('erc721_polygon', 'evt_Approval'))
     , ('bnb', source('erc721_bnb', 'evt_Approval'))
     , ('avalanche_c', source('erc721_avalanche_c', 'evt_Approval'))
     , ('gnosis', source('erc721_gnosis', 'evt_Approval'))
     , ('fantom', source('erc721_fantom', 'evt_Approval'))
     , ('optimism', source('erc721_optimism', 'evt_Approval'))
     , ('arbitrum', source('erc721_arbitrum', 'evt_Approval'))
     , ('celo', source('erc721_celo', 'evt_Approval'))
     , ('base', source('erc721_base', 'evt_Approval'))
     , ('goerli', source('erc721_goerli', 'evt_Approval'))
     , ('zksync', source('erc721_zksync', 'evt_Approval'))
     , ('zora', source('erc721_zora', 'evt_Approval'))
     , ('scroll', source('erc721_scroll', 'evt_Approval'))
] %}

SELECT *
FROM (
        {% for erc721_approvals_model in erc721_approvals_models %}
        SELECT
        '{{ erc721_approvals_model[0] }}' AS blockchain
        , contract_address
        , evt_tx_hash
        , evt_index
        , evt_block_time
        , evt_block_number
        , approved
        , owner
        , tokenId
        FROM {{ erc721_approvals_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );