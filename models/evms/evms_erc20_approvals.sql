{{ config(
        alias ='erc20_approvals',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
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
] %}

SELECT *
FROM (
        {% for erc20_approvals_model in erc20_approvals_models %}
        SELECT
        '{{ erc20_approvals_model[0] }}' AS blockchain
        , *
        FROM {{ erc20_approvals_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );