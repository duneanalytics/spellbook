{{ config(
        
        alias = 'erc20_approvals',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo", "base", "goerli", "zksync", "zora", "scroll"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

{% set erc20_approvals_models = [
     ('ethereum', source('erc20_ethereum', 'evt_approval'))
     , ('polygon', source('erc20_polygon', 'evt_approval'))
     , ('bnb', source('erc20_bnb', 'evt_approval'))
     , ('avalanche_c', source('erc20_avalanche_c', 'evt_approval'))
     , ('gnosis', source('erc20_gnosis', 'evt_approval'))
     , ('fantom', source('erc20_fantom', 'evt_approval'))
     , ('optimism', source('erc20_optimism', 'evt_Approval'))
     , ('arbitrum', source('erc20_arbitrum', 'evt_approval'))
     , ('celo', source('erc20_celo', 'evt_approval'))
     , ('base', source('erc20_base', 'evt_Approval'))
     , ('goerli', source('erc20_goerli', 'evt_approval'))
     , ('zksync', source('erc20_zksync', 'evt_approval'))
     , ('zora', source('erc20_zora', 'evt_approval'))
     , ('scroll', source('erc20_scroll', 'evt_approval'))
] %}

SELECT *
FROM (
        {% for erc20_approvals_model in erc20_approvals_models %}
        SELECT
        '{{ erc20_approvals_model[0] }}' AS blockchain
        , contract_address
        , evt_tx_hash
        , evt_index
        , evt_block_time
        , evt_block_number
        , owner
        , spender
        , value
        FROM {{ erc20_approvals_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );